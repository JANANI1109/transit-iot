"""
Public Transport IoT System — EDA Engine
Simulated Kafka-style event-driven architecture using Python queues.
In production: replace SimulatedBroker with kafka-python.
"""

import time, random, threading, queue, logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable

from dsl_interpreter import DSLEngine, SensorEvent, ActionResult

log = logging.getLogger("EDA")

ROUTES = ["route_1","route_2","route_3","route_5","route_6","route_7"]

ROUTE_PROFILES = {
    "route_1": {"base_load":55,"base_passengers":60,"base_delay":2,
                "lat":12.9716,"lon":77.5946,"name":"Majestic–Electronic City"},
    "route_2": {"base_load":70,"base_passengers":80,"base_delay":1,
                "lat":12.9784,"lon":77.7480,"name":"Whitefield–KR Puram"},
    "route_3": {"base_load":45,"base_passengers":45,"base_delay":4,
                "lat":12.9279,"lon":77.5833,"name":"Jayanagar–Koramangala"},
    "route_5": {"base_load":75,"base_passengers":90,"base_delay":1,
                "lat":13.0358,"lon":77.5970,"name":"Hebbal–Silk Board"},
    "route_6": {"base_load":30,"base_passengers":30,"base_delay":0,
                "lat":12.9542,"lon":77.4980,"name":"Banashankari–Yeshwantpur"},
    "route_7": {"base_load":10,"base_passengers":8,"base_delay":0,
                "lat":13.1007,"lon":77.5963,"name":"Yelahanka–Devanahalli"},
}

# Simulated clock: starts at 08:00, 1 real second = 60 sim seconds
_SIM_START_REAL = datetime.now()
_SIM_SPEED = 60

def simulated_time() -> datetime:
    elapsed = (datetime.now() - _SIM_START_REAL).total_seconds() * _SIM_SPEED
    base = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
    return base + timedelta(seconds=elapsed)


@dataclass
class Message:
    topic: str; key: str; payload: Dict; timestamp: datetime = None
    def __post_init__(self):
        if not self.timestamp: self.timestamp = datetime.now()


class SimulatedBroker:
    def __init__(self, maxsize=1000):
        self._topics: Dict[str, queue.Queue] = {}
        self._lock = threading.Lock()
        self.message_count = 0

    def _ensure(self, topic):
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = queue.Queue(maxsize=1000)

    def publish(self, msg: Message):
        self._ensure(msg.topic)
        try: self._topics[msg.topic].put_nowait(msg); self.message_count += 1
        except queue.Full: pass

    def consume(self, topic, timeout=0.05) -> Optional[Message]:
        self._ensure(topic)
        try: return self._topics[topic].get(timeout=timeout)
        except queue.Empty: return None


# Live GPS positions (drift around base coords)
_live_positions: Dict[str, Dict] = {r: {"lat": ROUTE_PROFILES[r]["lat"],
                                         "lon": ROUTE_PROFILES[r]["lon"]} for r in ROUTES}


class GPSSensorProducer(threading.Thread):
    def __init__(self, broker, interval=2.0):
        super().__init__(daemon=True, name="GPS")
        self.broker = broker; self.interval = interval; self._running = True

    def run(self):
        while self._running:
            sim = simulated_time()
            for route in ROUTES:
                p = ROUTE_PROFILES[route]
                delay = p["base_delay"] + random.gauss(0, 1.5)
                if route == "route_3" and 13 <= sim.hour <= 15: delay += random.uniform(5,12)
                if route == "route_1" and sim.hour >= 9: delay += random.uniform(8,20)
                # Drift lat/lon slightly for moving bus effect
                _live_positions[route]["lat"] += random.uniform(-0.0003, 0.0003)
                _live_positions[route]["lon"] += random.uniform(-0.0003, 0.0003)
                self.broker.publish(Message("sensor.gps", route, {
                    "route": route, "name": p["name"],
                    "lat": round(_live_positions[route]["lat"], 6),
                    "lon": round(_live_positions[route]["lon"], 6),
                    "speed_kmh": round(random.uniform(10,55), 1),
                    "delay_minutes": round(max(0, delay), 2),
                }))
            time.sleep(self.interval)

    def stop(self): self._running = False


class LoadSensorProducer(threading.Thread):
    def __init__(self, broker, interval=2.5):
        super().__init__(daemon=True, name="Load")
        self.broker = broker; self.interval = interval; self._running = True

    def run(self):
        while self._running:
            sim = simulated_time(); hour = sim.hour
            for route in ROUTES:
                p = ROUTE_PROFILES[route]
                mult = 1.0
                if 8 <= hour <= 10: mult = random.uniform(1.2, 1.6)
                elif 17 <= hour <= 19: mult = random.uniform(1.1, 1.4)
                elif hour >= 22 or hour <= 5: mult = random.uniform(0.05, 0.2)
                load = min(100, p["base_load"] * mult + random.gauss(0,5))
                pax = int(load/100 * p["base_passengers"] * 1.1)
                self.broker.publish(Message("sensor.load", route, {
                    "route": route, "load_percent": round(max(0,load),1),
                    "passenger_count": max(0,pax), "capacity": 120,
                }))
            time.sleep(self.interval)

    def stop(self): self._running = False


class EventAggregator:
    def __init__(self, window=10.0):
        self._gps: Dict[str, Dict] = {}
        self._load: Dict[str, Dict] = {}
        self._window = window

    def update_gps(self, route, payload, ts): self._gps[route] = {**payload, "_ts": ts}
    def update_load(self, route, payload, ts): self._load[route] = {**payload, "_ts": ts}

    def get_event(self, route) -> Optional[SensorEvent]:
        gps = self._gps.get(route); load = self._load.get(route)
        if not gps or not load: return None
        now = datetime.now()
        if (now - gps["_ts"]).seconds > self._window: return None
        if (now - load["_ts"]).seconds > self._window: return None
        return SensorEvent(
            route=route, timestamp=simulated_time(),
            load_percent=load["load_percent"], delay_minutes=gps["delay_minutes"],
            headway_minutes=random.uniform(5,25), passengers=load["passenger_count"],
            lat=gps["lat"], lon=gps["lon"],
        )

    def get_all_snapshots(self) -> List[Dict]:
        """Return latest sensor snapshot for every route (for /routes endpoint)."""
        snaps = []
        for route in ROUTES:
            gps = self._gps.get(route, {}); load = self._load.get(route, {})
            p = ROUTE_PROFILES[route]
            snaps.append({
                "id": route, "name": p["name"],
                "lat": gps.get("lat", p["lat"]), "lon": gps.get("lon", p["lon"]),
                "speed_kmh": gps.get("speed_kmh", 0),
                "delay_minutes": gps.get("delay_minutes", 0),
                "load_percent": load.get("load_percent", 0),
                "passengers": load.get("passenger_count", 0),
                "capacity": 120,
            })
        return snaps


class EDAEngine(threading.Thread):
    def __init__(self, broker: SimulatedBroker, dsl: DSLEngine, poll=0.3):
        super().__init__(daemon=True, name="EDA")
        self.broker = broker; self.dsl = dsl; self.poll = poll
        self._running = True
        self._agg = EventAggregator()
        self.events_processed = 0; self.actions_fired = 0
        self.action_log: List[ActionResult] = []
        self._callbacks: List[Callable] = []
        self.MAX_LOG = 200

    def add_callback(self, fn): self._callbacks.append(fn)

    @property
    def aggregator(self): return self._agg

    def run(self):
        topics = ["sensor.gps", "sensor.load"]
        while self._running:
            got = False
            for topic in topics:
                msg = self.broker.consume(topic)
                if msg:
                    got = True; self._ingest(topic, msg)
            if not got: time.sleep(self.poll)

    def _ingest(self, topic, msg):
        route = msg.key
        if topic == "sensor.gps": self._agg.update_gps(route, msg.payload, msg.timestamp)
        elif topic == "sensor.load": self._agg.update_load(route, msg.payload, msg.timestamp)
        event = self._agg.get_event(route)
        if not event: return
        self.events_processed += 1
        for result in self.dsl.process_event(event):
            self.actions_fired += 1
            if len(self.action_log) >= self.MAX_LOG: self.action_log.pop(0)
            self.action_log.append(result)
            for cb in self._callbacks: cb(result)

    def stop(self): self._running = False

    def stats(self):
        return {"events_processed": self.events_processed,
                "actions_fired": self.actions_fired,
                "broker_messages": self.broker.message_count,
                "rules_loaded": self.dsl.rule_count}
