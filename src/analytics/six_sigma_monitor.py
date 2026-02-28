import os, json, time, statistics
from collections import deque
from datetime import datetime, timezone

import redis
from confluent_kafka import Consumer, Producer


METRICS = {
    "fHaertetemperatur (Haerten)": "hard",
    "fAnlasstemperatur (Anlassen)": "anneal",
}


def fnum(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    try:
        return float(x)
    except Exception:
        return None


def ts_iso(s: str) -> float:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class SixSigma:
    def __init__(self):
        boot = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
        self.t_in = os.getenv("INPUT_TOPIC", "1031103_1000")
        self.t_out = os.getenv("ALERT_TOPIC", "1031103_801")
        self.win_s = int(os.getenv("WINDOW_SECONDS", "300"))
        self.min_pts = int(os.getenv("MIN_POINTS", "30"))
        self.offset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

        self.c = Consumer(
            {
                "bootstrap.servers": boot,
                "group.id": os.getenv("KAFKA_GROUP_ID", "six-sigma-monitor"),
                "auto.offset.reset": self.offset,
                "enable.auto.commit": True,
            }
        )
        self.p = Producer({"bootstrap.servers": boot})

        self.w = {m: deque() for m in METRICS}      # metric -> deque[(ts,val)]
        self.e = {m: None for m in METRICS}         # metric -> event dict / None

        self.redis_on = os.getenv("ENABLE_REDIS_STATE", "false").lower() in ("1","true","yes","y","on")
        self.r = None
        self.pref = os.getenv("REDIS_SIXSIGMA_STREAM_PREFIX", "sixsigma:window")
        self.s_state = os.getenv("REDIS_SIXSIGMA_STATE_STREAM", "sixsigma:state")
        self.max_w = int(os.getenv("REDIS_MAXLEN_WINDOW", str(max(1000, self.win_s * 12))))
        self.max_s = int(os.getenv("REDIS_MAXLEN_STATE", "1000"))

        if self.redis_on:
            self.r = self.redis_connect()
            if self.r:
                self.restore()

    def redis_connect(self):
        try:
            r = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                decode_responses=True,
            )
            r.ping()
            return r
        except Exception:
            return None

    def s_window(self, metric: str) -> str:
        return f"{self.pref}:{METRICS[metric]}"

    def restore(self):
        for metric in METRICS:
            key = self.s_window(metric)
            try:
                items = self.r.xrevrange(key, count=self.max_w)
            except Exception:
                items = []
            pts = []
            for _id, f in reversed(items):
                t = fnum(f.get("ts"))
                v = fnum(f.get("v"))
                if t is not None and v is not None:
                    pts.append((t, v))
            if pts:
                cut = pts[-1][0] - self.win_s
                pts = [(t, v) for (t, v) in pts if t >= cut]
            self.w[metric] = deque(pts)

        try:
            last = self.r.xrevrange(self.s_state, count=1)
        except Exception:
            last = []
        if last:
            f = last[0][1]
            try:
                ev = json.loads(f.get("events_json", "{}"))
            except Exception:
                ev = {}
            for metric in METRICS:
                self.e[metric] = ev.get(metric)

    def xadd_point(self, metric: str, ts: float, v: float):
        if not self.r:
            return
        try:
            self.r.xadd(self.s_window(metric), {"ts": str(ts), "v": str(v)}, maxlen=self.max_w, approximate=True)
        except Exception:
            pass

    def xadd_events(self):
        if not self.r:
            return
        try:
            self.r.xadd(self.s_state, {"events_json": json.dumps(self.e, separators=(",", ":"))},
                        maxlen=self.max_s, approximate=True)
        except Exception:
            pass

    def update(self, metric: str, ts: float, v: float):
        q = self.w[metric]
        q.append((ts, v))
        while q and (ts - q[0][0]) > self.win_s:
            q.popleft()
        self.xadd_point(metric, ts, v)

    def send(self, payload: dict):
        self.p.produce(self.t_out, json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        self.p.poll(0)

    def check(self, metric: str, ts: float, v: float):
        q = self.w[metric]
        if len(q) < self.min_pts:
            return

        vals = [x for _, x in q]
        mean = statistics.mean(vals)
        sd = statistics.stdev(vals)
        if sd == 0:
            return

        alarm = (v > mean + 3 * sd) or (v < mean - 3 * sd)
        ev = self.e[metric]

        if alarm:
            if ev is None:
                self.send({"type": "alarm_start", "timestamp": ts, "metric": metric, "value": v})
                self.e[metric] = {"start": ts, "end": ts, "peak": v, "count": 1}
                self.xadd_events()
            else:
                ev["end"] = ts
                ev["count"] += 1
                if abs(v - mean) > abs(ev["peak"] - mean):
                    ev["peak"] = v
        else:
            if ev is not None and (ts - ev["end"]) >= 1.0:
                self.send(
                    {
                        "type": "alarm_summary",
                        "event_start": ev["start"],
                        "event_end": ev["end"],
                        "duration_sec": round(ev["end"] - ev["start"], 2),
                        "metric": metric,
                        "peak_value": ev["peak"],
                        "alarm_count": ev["count"],
                    }
                )
                self.e[metric] = None
                self.xadd_events()

    def run(self):
        self.c.subscribe([self.t_in])
        while True:
            msg = self.c.poll(1.0)
            if msg is None or msg.error():
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            t = data.get("Time")
            if not t:
                continue
            ts = ts_iso(t)

            hard = anneal = None
            for it in data.get("ProcessData", []):
                n = it.get("Name")
                if n == "fHaertetemperatur (Haerten)":
                    hard = fnum(it.get("Value"))
                elif n == "fAnlasstemperatur (Anlassen)":
                    anneal = fnum(it.get("Value"))

            if hard is not None:
                m = "fHaertetemperatur (Haerten)"
                self.update(m, ts, hard)
                self.check(m, ts, hard)

            if anneal is not None:
                m = "fAnlasstemperatur (Anlassen)"
                self.update(m, ts, anneal)
                self.check(m, ts, anneal)


if __name__ == "__main__":
    SixSigma().run()