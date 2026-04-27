"""
Public Transport IoT System — OR-Tools Optimizer
CP-SAT solver for trip scheduling optimization.
"""

import logging, random
from dataclasses import dataclass, field
from typing import List, Dict, Optional

log = logging.getLogger("Optimizer")

try:
    from ortools.sat.python import cp_model
    ORTOOLS_OK = True
except ImportError:
    ORTOOLS_OK = False


@dataclass
class RouteData:
    id: str; name: str; stops: int; length_km: float
    demand: List[int]; capacity: int = 120


@dataclass
class ScheduleSlot:
    route_id: str; hour: int; trips: int
    buses_assigned: int; expected_wait_min: float; demand_covered: int


@dataclass
class OptResult:
    status: str; schedule: List[ScheduleSlot]; total_trips: int
    avg_wait_minutes: float; fleet_utilization: float; solver_time_ms: float


def _demand_profile(base: int, peak: float = 2.0) -> List[int]:
    out = []
    for h in range(24):
        if h < 5: out.append(int(base * 0.1))
        elif h < 8: out.append(int(base * 0.5))
        elif h <= 10: out.append(int(base * peak))
        elif h <= 16: out.append(int(base * 0.8))
        elif h <= 19: out.append(int(base * peak * 0.9))
        elif h <= 21: out.append(int(base * 0.5))
        else: out.append(int(base * 0.15))
    return out


SAMPLE_ROUTES = [
    RouteData("route_1","Majestic–Electronic City",22,29,_demand_profile(280,2.2)),
    RouteData("route_2","Whitefield–KR Puram",14,16,_demand_profile(200,1.8)),
    RouteData("route_3","Jayanagar–Koramangala",10,8,_demand_profile(150,1.6)),
    RouteData("route_5","Hebbal–Silk Board",28,22,_demand_profile(320,2.5)),
    RouteData("route_6","Banashankari–Yeshwantpur",18,14,_demand_profile(180,1.7)),
    RouteData("route_7","Yelahanka–Devanahalli",12,18,_demand_profile(60,1.3)),
]

PEAK_HOURS = [8,9,10,17,18,19]


def solve_schedule(routes=None, total_fleet=30, peak_hours=None, horizon=18) -> OptResult:
    import time as _t
    if routes is None: routes = SAMPLE_ROUTES
    if peak_hours is None: peak_hours = PEAK_HOURS
    start = _t.time()

    if not ORTOOLS_OK:
        return _greedy(routes, total_fleet, peak_hours, horizon, _t.time()-start)

    model = cp_model.CpModel()
    trips = {}
    for r, route in enumerate(routes):
        for h in range(horizon):
            ha = h + 6
            demand = route.demand[ha] if ha < len(route.demand) else 0
            mn = 1; mx = max(2, -(-demand // route.capacity) + 2)
            trips[(r,h)] = model.NewIntVar(mn, mx, f"t_{r}_{h}")

    for h in range(horizon):
        model.Add(sum(trips[(r,h)] for r in range(len(routes))) <= total_fleet)

    for r, route in enumerate(routes):
        for h in range(horizon):
            ha = h + 6
            demand = route.demand[ha] if ha < len(route.demand) else 0
            if ha in peak_hours and demand > 0:
                model.Add(trips[(r,h)] >= max(1, -(-demand // route.capacity)))

    model.Maximize(sum(trips[(r,h)] for r in range(len(routes)) for h in range(horizon)))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 8.0
    status = solver.Solve(model)
    elapsed = (_t.time()-start)*1000

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        sched, tt, tw, td = [], 0, 0.0, 0
        for r, route in enumerate(routes):
            for h in range(horizon):
                ha = h+6; t = solver.Value(trips[(r,h)])
                demand = route.demand[ha] if ha < len(route.demand) else 0
                wait = (60/max(t,1))/2; covered = min(demand, t*route.capacity)
                sched.append(ScheduleSlot(route.id,ha,t,t,round(wait,1),covered))
                tt+=t; tw+=wait*demand; td+=demand
        st = {cp_model.OPTIMAL:"OPTIMAL",cp_model.FEASIBLE:"FEASIBLE"}.get(status,"OK")
        return OptResult(st,sched,tt,round(tw/max(td,1),2),round(tt/(total_fleet*horizon),3),round(elapsed,1))
    return _greedy(routes, total_fleet, peak_hours, horizon, elapsed)


def _greedy(routes, fleet, peaks, horizon, elapsed):
    sched, tt, tw, td = [], 0, 0.0, 0
    for route in routes:
        for h in range(horizon):
            ha = h+6; demand = route.demand[ha] if ha < len(route.demand) else 0
            t = max(1, -(-demand//route.capacity)) if demand > 0 else 1
            if ha in peaks: t = min(t+1, 8)
            wait = (60/t)/2; covered = min(demand, t*route.capacity)
            sched.append(ScheduleSlot(route.id,ha,t,t,round(wait,1),covered))
            tt+=t; tw+=wait*demand; td+=demand
    return OptResult("SIMULATED",sched,tt,round(tw/max(td,1),2),round(tt/(fleet*horizon),3),round(elapsed,1))
