"""
Public Transport IoT System — FastAPI Backend
All endpoints in one file. Starts EDA engine on startup.
"""

import os, logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from dsl_interpreter import DSLEngine, SensorEvent
from eda_engine import EDAEngine, SimulatedBroker, GPSSensorProducer, LoadSensorProducer, ROUTE_PROFILES, ROUTES
from optimizer import solve_schedule, SAMPLE_ROUTES
from security import issue_ticket, verify_ticket, tamper_ticket

logging.basicConfig(level=logging.WARNING)

# ── Global state ──────────────────────────────────────────────────────────────
dsl = DSLEngine()
broker = SimulatedBroker()
eda = EDAEngine(broker, dsl)
gps_prod = GPSSensorProducer(broker)
load_prod = LoadSensorProducer(broker)

_rules_store: Dict[int, str] = {}
_next_id = 1

DEFAULT_RULES = [
    "IF load > 80% ON route_5 BETWEEN 08:00 AND 10:00 THEN add_trip EVERY 15min",
    'IF delay > 5 ON route_3 THEN notify_operator "Delay on route 3"',
    "IF passengers < 10 ON route_7 BETWEEN 22:00 AND 05:00 THEN cancel_trip",
    "IF load > 90% AND headway > 20 ON route_2 THEN increase_capacity",
    "IF delay > 15 ON route_1 THEN reroute TO route_6",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _next_id
    for raw in DEFAULT_RULES:
        try:
            dsl.load_rule(raw)
            _rules_store[_next_id] = raw
            _next_id += 1
        except Exception as e:
            print(f"Rule load error: {e}")

    gps_prod.start(); load_prod.start(); eda.start()
    yield
    gps_prod.stop(); load_prod.stop(); eda.stop()


app = FastAPI(title="Transit IoT API", version="1.0.0", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# Serve static files (dashboard)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Schemas ───────────────────────────────────────────────────────────────────

class RuleIn(BaseModel):
    rule: str = Field(..., example="IF load > 80% ON route_5 BETWEEN 08:00 AND 10:00 THEN add_trip EVERY 15min")

class RuleOut(BaseModel):
    id: int; rule: str

class SimulateIn(BaseModel):
    route: str = "route_5"
    load_percent: float = Field(85.0, ge=0, le=100)
    delay_minutes: float = 2.0
    passengers: int = 90
    headway_minutes: float = 10.0
    sim_hour: int = Field(9, ge=0, le=23)

class TicketIn(BaseModel):
    passenger_id: str = "student_001"
    route: str = "route_5"
    fare: float = 25.0

class VerifyIn(BaseModel):
    token: str


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"service": "Transit IoT API", "docs": "/docs",
            "endpoints": ["/routes","/schedule","/demand","/rules","/events","/simulate","/ticket","/stats"]}

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat(),
            "eda_alive": eda.is_alive(), "rules": dsl.rule_count}

@app.get("/routes")
async def get_routes():
    return eda.aggregator.get_all_snapshots()

@app.get("/routes/{route_id}")
async def get_route(route_id: str):
    snaps = eda.aggregator.get_all_snapshots()
    r = next((s for s in snaps if s["id"] == route_id), None)
    if not r: raise HTTPException(404, f"Route {route_id} not found")
    return r

@app.get("/schedule")
async def get_schedule(route_id: Optional[str] = None, hour_from: int = 6, hour_to: int = 23):
    routes = SAMPLE_ROUTES
    if route_id:
        routes = [r for r in routes if r.id == route_id]
        if not routes: raise HTTPException(404, f"Route {route_id} not found")
    result = solve_schedule(routes)
    slots = [s.__dict__ for s in result.schedule if hour_from <= s.hour <= hour_to]
    return {"status": result.status, "total_trips": result.total_trips,
            "avg_wait_minutes": result.avg_wait_minutes,
            "fleet_utilization": result.fleet_utilization,
            "solver_time_ms": result.solver_time_ms, "slots": slots}

@app.get("/demand")
async def get_demand(route_id: Optional[str] = None):
    from optimizer import SAMPLE_ROUTES
    routes = SAMPLE_ROUTES
    if route_id:
        routes = [r for r in routes if r.id == route_id]
        if not routes: raise HTTPException(404, f"Route {route_id} not found")
    out = []
    for r in routes:
        mx = max(r.demand)
        peaks = [h for h,d in enumerate(r.demand) if d >= mx * 0.7]
        out.append({"route_id": r.id, "name": r.name,
                    "peak_hours": peaks, "daily_total": sum(r.demand),
                    "peak_demand": mx, "demand_by_hour": r.demand})
    return out

@app.get("/rules", response_model=List[RuleOut])
async def list_rules():
    return [{"id": k, "rule": v} for k, v in _rules_store.items()]

@app.post("/rules", response_model=RuleOut, status_code=201)
async def add_rule(body: RuleIn):
    global _next_id
    try: dsl.load_rule(body.rule)
    except SyntaxError as e: raise HTTPException(422, str(e))
    rid = _next_id; _rules_store[rid] = body.rule; _next_id += 1
    return {"id": rid, "rule": body.rule}

@app.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int):
    if rule_id not in _rules_store: raise HTTPException(404, f"Rule {rule_id} not found")
    del _rules_store[rule_id]
    dsl.clear_rules()
    for raw in _rules_store.values(): dsl.load_rule(raw)
    return {"message": f"Rule {rule_id} deleted. {dsl.rule_count} rules remain."}

@app.get("/events")
async def get_events(limit: int = 50):
    log = eda.action_log[-limit:]
    return [{"rule_text": e.rule_text, "action_type": e.action_type,
             "route": e.route, "timestamp": e.timestamp.isoformat(),
             "details": e.details} for e in reversed(log)]

@app.post("/simulate")
async def simulate(body: SimulateIn):
    ts = datetime.now().replace(hour=body.sim_hour, minute=0, second=0)
    event = SensorEvent(route=body.route, timestamp=ts, load_percent=body.load_percent,
                        delay_minutes=body.delay_minutes, headway_minutes=body.headway_minutes,
                        passengers=body.passengers)
    results = dsl.process_event(event)
    return [{"action_type": r.action_type, "route": r.route,
             "details": r.details} for r in results]

@app.post("/ticket/issue")
async def issue(body: TicketIn):
    tid, token = issue_ticket(body.passenger_id, body.route, body.fare)
    return {"ticket_id": tid, "token": token, "route": body.route,
            "passenger_id": body.passenger_id, "fare": body.fare}

@app.post("/ticket/verify")
async def verify(body: VerifyIn):
    valid, msg, data = verify_ticket(body.token)
    return {"valid": valid, "message": msg, "ticket": data}

@app.post("/ticket/tamper-demo")
async def tamper_demo(body: VerifyIn):
    tampered = tamper_ticket(body.token)
    valid, msg, _ = verify_ticket(tampered)
    return {"tampered_token": tampered[:40]+"...", "valid": valid, "message": msg}

@app.get("/stats")
async def stats():
    s = eda.stats()
    return {**s, "uptime": "EDA running with simulated Kafka",
            "timestamp": datetime.now().isoformat()}
