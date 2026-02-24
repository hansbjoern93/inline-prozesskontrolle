import os, json, math, signal, logging
from collections import deque
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException


BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = os.getenv("KAFKA_INPUT_TOPIC", "wire_data")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wire_profile_detector")
OFFSET   = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")  # für Datensatz sinnvoller als latest

FIELD_L  = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
FIELD_D  = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")

TOPIC_ALL = f"{TOPIC_IN}_profiles"
TOPIC_NIO = f"{TOPIC_IN}_profile_events"

NOK_MM = float(os.getenv("NOK_DEVIATION_MM", "30.0"))
NONMONO_RESET = os.getenv("RESET_ON_NONMONOTONIC_LENGTH", "true").lower() == "true"
NONMONO_DELTA = float(os.getenv("NONMONO_RESET_DELTA_MM", "1000.0"))

# detector params (klein & fest)
SMOOTH_N = 3
SIGMA_K  = 3.0
SIGMA_MINPTS = 20
SIGMA_MINSTD = 0.05

TREND_N = 10
FLAT_SLOPE = 0.003
HYST = 3
LOOKBACK = 5

MIN_SPACE = 200.0
MIN_RISE = 100.0
MIN_PLATEAU = 500.0
MIN_FALL = 100.0


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] wire_profile_detector – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("wire_profile_detector")


def slope_lin(xs, ys) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    sx = sum(xs); sy = sum(ys)
    sxx = sum(x*x for x in xs)
    sxy = sum(x*y for x, y in zip(xs, ys))
    den = n*sxx - sx*sx
    if abs(den) < 1e-12:
        return 0.0
    return (n*sxy - sx*sy) / den


def parse_point(raw: bytes):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    L = D = None
    for e in data.get("ProcessData", []):
        n = e.get("Name", "")
        if n == FIELD_L:
            try: L = float(e["Value"])
            except: pass
        elif n == FIELD_D:
            try: D = float(e["Value"])
            except: pass

    if L is None or D is None:
        return None
    return L, D


class Welford:
    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0

    def reset(self):
        self.n = 0; self.mean = 0.0; self.M2 = 0.0

    def update(self, x: float):
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.M2 += d * (x - self.mean)

    @property
    def std(self):
        if self.n < 2:
            return SIGMA_MINSTD
        return max(math.sqrt(self.M2 / (self.n - 1)), SIGMA_MINSTD)

    def high(self): return self.mean + SIGMA_K * self.std
    def low(self):  return self.mean - SIGMA_K * self.std


class RefMeans:
    def __init__(self):
        self.count = 0
        self.mean = {"L1": 0.0, "L2": 0.0, "L3": 0.0, "L4": 0.0}

    def snapshot(self):
        return dict(self.mean)

    def update(self, p):
        self.count += 1
        for k in ("L1", "L2", "L3", "L4"):
            self.mean[k] += (p[k] - self.mean[k]) / self.count

    def eval(self, p):
        if self.count == 0:
            return "i.O.", "first profile initializes reference"
        reasons = []
        for k in ("L1", "L2", "L3", "L4"):
            dev = abs(p[k] - self.mean[k])
            if dev > NOK_MM:
                reasons.append(f"{k} Abw. {dev:.1f}mm > {NOK_MM:.1f}mm (ist={p[k]:.1f} ref={self.mean[k]:.1f})")
        return ("n.i.O.", "; ".join(reasons)) if reasons else ("i.O.", "")


class Detector:
    WAIT, SPACE, RISE, PLATEAU, FALL = "WAIT","SPACE","RISE","PLATEAU","FALL"

    def __init__(self):
        self.state = self.WAIT
        self.pid = 0

        self.smooth = deque(maxlen=SMOOTH_N)
        self.trend  = deque(maxlen=TREND_N)
        self.lb     = deque(maxlen=LOOKBACK + HYST + 2)

        self.stats = Welford()
        self.ref   = RefMeans()

        self.seg_start = 0.0
        self.state_entry = 0.0
        self.segs = {}

        self.h_sigma = 0
        self.h_flat  = 0

    def _enter(self, s, L):
        self.state = s
        self.state_entry = L
        self.stats.reset()
        self.h_sigma = 0
        self.h_flat  = 0
        self.lb.clear()

    def _minlen(self):
        return {self.SPACE: MIN_SPACE, self.RISE: MIN_RISE, self.PLATEAU: MIN_PLATEAU, self.FALL: MIN_FALL}.get(self.state, 0.0)

    def _enough(self, L):
        return (L - self.state_entry) >= self._minlen()

    def _corr_len(self, L):
        buf = list(self.lb)
        off = LOOKBACK + HYST - 1
        return buf[-(off + 1)] if len(buf) > off else L

    def _close(self, name, endL):
        self.segs[name] = max(0.0, endL - self.seg_start)
        self.seg_start = endL

    def feed(self, L, D):
        self.smooth.append(D)
        d = sum(self.smooth) / len(self.smooth)

        self.trend.append((L, d))
        self.lb.append(L)

        if len(self.trend) >= TREND_N:
            xs = [x for x, _ in self.trend]
            ys = [y for _, y in self.trend]
            sl = slope_lin(xs, ys)
            flat = abs(sl) < FLAT_SLOPE
        else:
            flat = True

        # stats für adaptive thresholds
        self.stats.update(d)
        sigma_ready = self.stats.n >= SIGMA_MINPTS
        above = sigma_ready and d > self.stats.high()
        below = sigma_ready and d < self.stats.low()

        # hysterese
        self.h_flat = self.h_flat + 1 if flat else 0
        if self.state == self.SPACE:
            self.h_sigma = self.h_sigma + 1 if above else 0
        elif self.state == self.PLATEAU:
            self.h_sigma = self.h_sigma + 1 if below else 0
        else:
            self.h_sigma = 0

        # transitions
        if self.state == self.WAIT:
            self.seg_start = L
            self._enter(self.SPACE, L)
            return None

        if self.state == self.SPACE:
            if self.h_sigma >= HYST and self._enough(L):
                self._close("L2", L)
                self._enter(self.RISE, L)

        elif self.state == self.RISE:
            if self.h_flat >= HYST and self._enough(L):
                tp = self._corr_len(L)
                self._close("L3", tp)
                self._enter(self.PLATEAU, L)
                self.seg_start = tp

        elif self.state == self.PLATEAU:
            if self.h_sigma >= HYST and self._enough(L):
                self._close("L1", L)
                self._enter(self.FALL, L)

        elif self.state == self.FALL:
            if self.h_flat >= HYST and self._enough(L):
                tp = self._corr_len(L)
                self._close("L4", tp)

                if all(k in self.segs for k in ("L1","L2","L3","L4")):
                    return self._finalize()
                self.segs.clear()

                self._enter(self.SPACE, L)
                self.seg_start = tp

        return None

    def _finalize(self):
        self.pid += 1
        p = {
            "profile_id": self.pid,
            "L1": float(self.segs.pop("L1", 0.0)),
            "L2": float(self.segs.pop("L2", 0.0)),
            "L3": float(self.segs.pop("L3", 0.0)),
            "L4": float(self.segs.pop("L4", 0.0)),
        }

        status, reason = self.ref.eval(p)
        p["status"] = status
        if reason: p["reason"] = reason

        if self.ref.count > 0:
            m = self.ref.snapshot()
            diffs = {k: abs(p[k] - m[k]) for k in ("L1","L2","L3","L4")}

            def mark(v): return "✅" if v <= NOK_MM else "❌"

            log.info(
                "Expected means (n=%d) | mean(L1)=%.1f mean(L2)=%.1f mean(L3)=%.1f mean(L4)=%.1f",
                self.ref.count, m["L1"], m["L2"], m["L3"], m["L4"]
            )
            log.info(
                "Diffs (±%.0fmm) | |L1-mean|=%.1f %s  |L2-mean|=%.1f %s  |L3-mean|=%.1f %s  |L4-mean|=%.1f %s",
                NOK_MM,
                diffs["L1"], mark(diffs["L1"]),
                diffs["L2"], mark(diffs["L2"]),
                diffs["L3"], mark(diffs["L3"]),
                diffs["L4"], mark(diffs["L4"]),
            )

        tail = f" | {p.get('reason','')}" if p.get("reason") else ""
        log.info(
            "Profil #%-4d | L1=%6.1f  L2=%6.1f  L3=%6.1f  L4=%6.1f mm | %s%s",
            p["profile_id"], p["L1"], p["L2"], p["L3"], p["L4"], p["status"], tail
        )
        if p["status"] == "n.i.O.":
            log.warning("n.i.O. Profil #%d: %s", p["profile_id"], p.get("reason",""))

        self.ref.update(p)
        return p


def build_consumer():
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": OFFSET,
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC_IN])
    return c


def build_producer():
    return Producer({"bootstrap.servers": BROKER})


def send(p, topic, o):
    for k in ("L1","L2","L3","L4"):
        if k in o: o[k] = round(o[k], 1)
    p.produce(topic, key=str(o["profile_id"]).encode(), value=json.dumps(o, separators=(",",":")).encode())
    p.poll(0)


def main():
    log.info("Profilerkennung gestartet | topic=%s | broker=%s | group=%s", TOPIC_IN, BROKER, GROUP_ID)

    c = build_consumer()
    p = build_producer()
    det = Detector()

    running = True
    def stop(sig, _):  # noqa
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_len = None

    try:
        while running:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            pt = parse_point(msg.value())
            if pt is None:
                continue

            L, D = pt

            if NONMONO_RESET and last_len is not None and L <= last_len:
                if (last_len - L) >= NONMONO_DELTA:
                    det = Detector()  # simplest reset
                last_len = L
                continue
            last_len = L

            prof = det.feed(L, D)
            if prof is not None:
                send(p, TOPIC_ALL, prof)
                if prof["status"] == "n.i.O.":
                    send(p, TOPIC_NIO, prof)

    finally:
        c.close()
        p.flush(5)


if __name__ == "__main__":
    main()