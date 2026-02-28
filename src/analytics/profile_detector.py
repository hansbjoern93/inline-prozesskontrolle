import os, json, math, signal, logging
from collections import deque

import redis
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException


# --- ENV ---
BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = os.getenv("KAFKA_INPUT_TOPIC", "wire_data")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wire_profile_detector")
OFFSET   = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

FIELD_L  = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
FIELD_D  = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")

TOPIC_ALL = f"{TOPIC_IN}_profiles"
TOPIC_NIO = f"{TOPIC_IN}_profile_events"

NOK_MM = float(os.getenv("NOK_DEVIATION_MM", "30.0"))

RESET_NONMONO = os.getenv("RESET_ON_NONMONOTONIC_LENGTH", "true").lower() in ("1","true","yes","y","on")
RESET_DELTA   = float(os.getenv("NONMONO_RESET_DELTA_MM", "1000.0"))

# detection params (simple)
SMOOTH_N = 3
TREND_N  = 10
FLAT_SLOPE = 0.003
HYST = 3
LOOKBACK = 5

SIGMA_K = 3.0
SIGMA_MINPTS = 20
SIGMA_MINSTD = 0.05

MINLEN = {"SPACE": 200.0, "RISE": 100.0, "PLATEAU": 500.0, "FALL": 100.0}

# --- Redis Streams state (restart-sicher) ---
REDIS_ON   = os.getenv("ENABLE_REDIS_STATE", "false").lower() in ("1","true","yes","y","on")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

STATE_STREAM = os.getenv("REDIS_PROFILE_STATE_STREAM", "profiles:state")
STATE_MAXLEN = int(os.getenv("REDIS_MAXLEN_STATE", "2000"))


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] profile_detector – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("profile_detector")


def safe_float(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        if not s: return None
        try: return float(s)
        except Exception: return None
    try: return float(x)
    except Exception: return None


def parse_point(raw: bytes):
    try:
        data = json.loads(raw)
    except Exception:
        return None

    L = D = None
    for e in data.get("ProcessData", []):
        n = e.get("Name")
        if n == FIELD_L:
            L = safe_float(e.get("Value"))
        elif n == FIELD_D:
            D = safe_float(e.get("Value"))
    if L is None or D is None:
        return None
    return float(L), float(D)


def redis_connect():
    if not REDIS_ON:
        return None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


class Welford:
    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def reset(self):
        self.n = 0; self.mean = 0.0; self.m2 = 0.0

    def update(self, x: float):
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    def std(self):
        if self.n < 2:
            return SIGMA_MINSTD
        s = math.sqrt(self.m2 / (self.n - 1))
        return s if s >= SIGMA_MINSTD else SIGMA_MINSTD

    def high(self): return self.mean + SIGMA_K * self.std()
    def low(self):  return self.mean - SIGMA_K * self.std()


class Detector:
    def __init__(self):
        self.state = "WAIT"
        self.pid = 0

        self.smooth = deque(maxlen=SMOOTH_N)
        self.trend  = deque(maxlen=TREND_N)
        self.lhist  = deque(maxlen=LOOKBACK + HYST + 10)

        self.stats = Welford()

        self.seg_start = 0.0
        self.entryL = 0.0
        self.segs = {}

        self.h_flat = 0
        self.h_sig  = 0

        self.ref_count = 0
        self.ref_sum = {"L1": 0.0, "L2": 0.0, "L3": 0.0, "L4": 0.0}

        self.r = redis_connect()
        self._restore()

    def _mean(self):
        if self.ref_count <= 0:
            return {"L1": 0.0, "L2": 0.0, "L3": 0.0, "L4": 0.0}
        return {k: self.ref_sum[k] / self.ref_count for k in self.ref_sum}

    def _restore(self):
        if self.r is None:
            return
        try:
            last = self.r.xrevrange(STATE_STREAM, count=1)
        except Exception:
            last = []
        if not last:
            return
        f = last[0][1]
        self.pid = int(f.get("pid", "0") or 0)
        self.ref_count = int(f.get("count", "0") or 0)
        for k in ("L1","L2","L3","L4"):
            self.ref_sum[k] = float(f.get(f"sum_{k}", "0") or 0.0)

    def _persist(self):
        if self.r is None:
            return
        fields = {"pid": str(self.pid), "count": str(self.ref_count)}
        for k in ("L1","L2","L3","L4"):
            fields[f"sum_{k}"] = str(self.ref_sum[k])
        try:
            self.r.xadd(STATE_STREAM, fields, maxlen=STATE_MAXLEN, approximate=True)
        except Exception:
            pass

    def _enter(self, s: str, L: float):
        self.state = s
        self.entryL = L
        self.stats.reset()
        self.h_flat = 0
        self.h_sig = 0
        self.lhist.clear()

    def _enough(self, L: float):
        return (L - self.entryL) >= MINLEN.get(self.state, 0.0)

    def _slope(self):
        if len(self.trend) < 2:
            return 0.0
        x0, y0 = self.trend[0]
        x1, y1 = self.trend[-1]
        dx = x1 - x0
        if abs(dx) < 1e-9:
            return 0.0
        return (y1 - y0) / dx

    def _corr_len(self, L):
        idx = LOOKBACK + HYST
        return self.lhist[-(idx + 1)] if len(self.lhist) > idx else L

    def _close(self, name, endL):
        self.segs[name] = max(0.0, endL - self.seg_start)
        self.seg_start = endL

    def feed(self, L: float, D: float):
        self.smooth.append(D)
        d = sum(self.smooth) / len(self.smooth)

        self.trend.append((L, d))
        self.lhist.append(L)

        flat = abs(self._slope()) < FLAT_SLOPE

        self.stats.update(d)
        sigma_ready = self.stats.n >= SIGMA_MINPTS
        above = sigma_ready and d > self.stats.high()
        below = sigma_ready and d < self.stats.low()

        self.h_flat = self.h_flat + 1 if flat else 0
        if self.state == "SPACE":
            self.h_sig = self.h_sig + 1 if above else 0
        elif self.state == "PLATEAU":
            self.h_sig = self.h_sig + 1 if below else 0
        else:
            self.h_sig = 0

        if self.state == "WAIT":
            self.seg_start = L
            self._enter("SPACE", L)
            return None

        if self.state == "SPACE" and self.h_sig >= HYST and self._enough(L):
            self._close("L2", L)
            self._enter("RISE", L)
            return None

        if self.state == "RISE" and self.h_flat >= HYST and self._enough(L):
            tp = self._corr_len(L)
            self._close("L3", tp)
            self._enter("PLATEAU", L)
            self.seg_start = tp
            return None

        if self.state == "PLATEAU" and self.h_sig >= HYST and self._enough(L):
            self._close("L1", L)
            self._enter("FALL", L)
            return None

        if self.state == "FALL" and self.h_flat >= HYST and self._enough(L):
            tp = self._corr_len(L)
            self._close("L4", tp)

            if all(k in self.segs for k in ("L1","L2","L3","L4")):
                return self._finalize()

            self.segs.clear()
            self._enter("SPACE", L)
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

        if self.ref_count == 0:
            p["status"] = "i.O."
            p["reason"] = "first profile initializes reference"
        else:
            m = self._mean()
            bad = []
            for k in ("L1","L2","L3","L4"):
                dev = abs(p[k] - m[k])
                if dev > NOK_MM:
                    bad.append(f"{k} dev {dev:.1f}mm > {NOK_MM:.1f}mm")
            if bad:
                p["status"] = "n.i.O."
                p["reason"] = "; ".join(bad)
            else:
                p["status"] = "i.O."

        self.ref_count += 1
        for k in ("L1","L2","L3","L4"):
            self.ref_sum[k] += p[k]
        self._persist()

        log.info("Profil #%d | L1=%.1f L2=%.1f L3=%.1f L4=%.1f | %s",
                 p["profile_id"], p["L1"], p["L2"], p["L3"], p["L4"], p["status"])
        if p["status"] == "n.i.O.":
            log.warning("n.i.O. #%d: %s", p["profile_id"], p.get("reason",""))
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


def send(producer: Producer, topic: str, msg: dict):
    out = dict(msg)
    for k in ("L1","L2","L3","L4"):
        if k in out:
            out[k] = round(out[k], 1)
    producer.produce(topic, key=str(out["profile_id"]), value=json.dumps(out, separators=(",",":")))
    producer.poll(0)


def main():
    log.info("Start | in=%s | broker=%s | group=%s | redis=%s", TOPIC_IN, BROKER, GROUP_ID, "ON" if REDIS_ON else "OFF")

    consumer = build_consumer()
    producer = build_producer()
    det = Detector()

    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_len = None

    try:
        while running:
            msg = consumer.poll(1.0)
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

            if RESET_NONMONO and last_len is not None and L <= last_len:
                if (last_len - L) >= RESET_DELTA:
                    det = Detector()
                last_len = L
                continue
            last_len = L

            prof = det.feed(L, D)
            if prof:
                send(producer, TOPIC_ALL, prof)
                if prof["status"] == "n.i.O.":
                    send(producer, TOPIC_NIO, prof)

    finally:
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    main()