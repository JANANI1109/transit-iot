"""
Public Transport IoT System — DSL Interpreter
Custom Domain-Specific Language for transit scheduling rules.

Grammar:
  IF metric comparator value [%] [ON route] [BETWEEN time AND time]
  [AND ...] THEN action [EVERY freq]

Examples:
  IF load > 80% ON route_5 BETWEEN 08:00 AND 10:00 THEN add_trip EVERY 15min
  IF delay > 5 ON route_3 THEN notify_operator "Delay on route 3"
  IF passengers < 10 ON route_7 BETWEEN 22:00 AND 05:00 THEN cancel_trip
"""

import re
import logging
from datetime import datetime, time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable
from enum import Enum

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)


class TT(Enum):
    IF="IF"; THEN="THEN"; AND="AND"; ON="ON"; BETWEEN="BETWEEN"
    AND_TIME="AND_TIME"; EVERY="EVERY"; TO="TO"
    LOAD="load"; DELAY="delay"; HEADWAY="headway"; PASSENGERS="passengers"
    ADD_TRIP="add_trip"; CANCEL_TRIP="cancel_trip"; INC_CAP="increase_capacity"
    NOTIFY="notify_operator"; REROUTE="reroute"
    GT=">"; LT="<"; GTE=">="; LTE="<="; EQ="=="; NEQ="!="
    NUMBER="NUMBER"; PERCENT="%"; TIME="TIME"; ROUTE="ROUTE"; STRING="STRING"; FREQ="FREQ"


@dataclass
class Token:
    type: TT; value: Any; pos: int


TOKEN_PATTERNS = [
    (TT.GTE,r">="),(TT.LTE,r"<="),(TT.NEQ,r"!="),(TT.EQ,r"=="),(TT.GT,r">"),(TT.LT,r"<"),
    (TT.PERCENT,r"%"),(TT.TIME,r"\b\d{2}:\d{2}\b"),(TT.FREQ,r"\b\d+(?:min|sec|hr)\b"),
    (TT.NUMBER,r"\b\d+(?:\.\d+)?\b"),(TT.ROUTE,r"\broute_\w+\b"),(TT.STRING,r'"[^"]*"'),
    (TT.IF,r"\bIF\b"),(TT.THEN,r"\bTHEN\b"),(TT.BETWEEN,r"\bBETWEEN\b"),(TT.AND,r"\bAND\b"),
    (TT.ON,r"\bON\b"),(TT.EVERY,r"\bEVERY\b"),(TT.TO,r"\bTO\b"),
    (TT.LOAD,r"\bload\b"),(TT.DELAY,r"\bdelay\b"),(TT.HEADWAY,r"\bheadway\b"),
    (TT.PASSENGERS,r"\bpassengers\b"),(TT.ADD_TRIP,r"\badd_trip\b"),
    (TT.CANCEL_TRIP,r"\bcancel_trip\b"),(TT.INC_CAP,r"\bincrease_capacity\b"),
    (TT.NOTIFY,r"\bnotify_operator\b"),(TT.REROUTE,r"\breroute\b"),
]
_WS = re.compile(r"\s+")


def tokenize(source: str) -> List[Token]:
    tokens, pos = [], 0
    compiled = [(tt, re.compile(p)) for tt, p in TOKEN_PATTERNS]
    while pos < len(source):
        if ws := _WS.match(source, pos):
            pos = ws.end(); continue
        matched = False
        for tt, pat in compiled:
            if m := pat.match(source, pos):
                raw = m.group(0)
                tokens.append(Token(tt, raw[1:-1] if tt == TT.STRING else raw, pos))
                pos = m.end(); matched = True; break
        if not matched:
            raise SyntaxError(f"Unexpected character at {pos}: '{source[pos]}'")
    return tokens


@dataclass
class TimeRange:
    start: time; end: time

@dataclass
class Condition:
    metric: str; comparator: str; value: float
    is_percent: bool; route: Optional[str]; time_range: Optional[TimeRange]

@dataclass
class Action:
    type: str; frequency_seconds: Optional[int]
    message: Optional[str]; target_route: Optional[str]

@dataclass
class Rule:
    conditions: List[Condition]; action: Action; raw_text: str


class Parser:
    def __init__(self, tokens, source):
        self.tokens = tokens; self.pos = 0; self.source = source

    def peek(self): return self.tokens[self.pos] if self.pos < len(self.tokens) else None
    def consume(self, t):
        tok = self.peek()
        if not tok: raise SyntaxError(f"Expected {t}, got end of input")
        if tok.type != t: raise SyntaxError(f"Expected {t} got {tok.type}")
        self.pos += 1; return tok
    def match(self, *types): tok = self.peek(); return tok and tok.type in types
    def optional(self, *types):
        if self.match(*types): tok = self.peek(); self.pos += 1; return tok
        return None

    def parse_rule(self):
        raw = self.source; self.consume(TT.IF)
        conds = [self.parse_condition()]
        while self.match(TT.AND): self.consume(TT.AND); conds.append(self.parse_condition())
        self.consume(TT.THEN)
        return Rule(conds, self.parse_action(), raw)

    def parse_condition(self):
        tok = self.peek()
        if not tok or tok.type not in (TT.LOAD,TT.DELAY,TT.HEADWAY,TT.PASSENGERS):
            raise SyntaxError(f"Expected metric, got {tok}")
        self.pos += 1; metric = tok.value
        cmp = self.peek()
        if not cmp or cmp.type not in (TT.GT,TT.LT,TT.GTE,TT.LTE,TT.EQ,TT.NEQ):
            raise SyntaxError(f"Expected comparator")
        self.pos += 1; comparator = cmp.value
        val = float(self.consume(TT.NUMBER).value)
        is_pct = self.optional(TT.PERCENT) is not None
        route = None
        if self.match(TT.ON): self.consume(TT.ON); route = self.consume(TT.ROUTE).value
        tr = None
        if self.match(TT.BETWEEN):
            self.consume(TT.BETWEEN); t1 = self._pt()
            self.consume(TT.AND); t2 = self._pt(); tr = TimeRange(t1, t2)
        return Condition(metric, comparator, val, is_pct, route, tr)

    def _pt(self):
        tok = self.consume(TT.TIME); h, m = map(int, tok.value.split(":")); return time(h, m)

    def parse_action(self):
        tok = self.peek()
        if not tok: raise SyntaxError("Expected action after THEN")
        freq = msg = target = None
        if tok.type == TT.ADD_TRIP:
            self.pos += 1
            if self.match(TT.EVERY):
                self.consume(TT.EVERY); freq = self._fs(self.consume(TT.FREQ).value)
            return Action("add_trip", freq, msg, target)
        elif tok.type == TT.CANCEL_TRIP: self.pos += 1; return Action("cancel_trip", freq, msg, target)
        elif tok.type == TT.INC_CAP: self.pos += 1; return Action("increase_capacity", freq, msg, target)
        elif tok.type == TT.NOTIFY:
            self.pos += 1; return Action("notify_operator", freq, self.consume(TT.STRING).value, target)
        elif tok.type == TT.REROUTE:
            self.pos += 1; self.consume(TT.TO)
            return Action("reroute", freq, msg, self.consume(TT.ROUTE).value)
        raise SyntaxError(f"Unknown action: {tok.type}")

    @staticmethod
    def _fs(s):
        m = re.match(r"(\d+)(min|sec|hr)", s)
        return int(m.group(1)) * {"min":60,"sec":1,"hr":3600}[m.group(2)]


@dataclass
class SensorEvent:
    route: str; timestamp: datetime; load_percent: float
    delay_minutes: float; headway_minutes: float; passengers: int
    lat: float = 12.9716; lon: float = 77.5946


@dataclass
class ActionResult:
    rule_text: str; action_type: str; route: str
    timestamp: datetime; details: Dict[str, Any] = field(default_factory=dict)


class Evaluator:
    _CMP = {">":lambda a,b:a>b,"<":lambda a,b:a<b,">=":lambda a,b:a>=b,
            "<=":lambda a,b:a<=b,"==":lambda a,b:a==b,"!=":lambda a,b:a!=b}
    _ATTR = {"load":"load_percent","delay":"delay_minutes",
             "headway":"headway_minutes","passengers":"passengers"}

    def evaluate(self, rule, event):
        return all(self._check(c, event) for c in rule.conditions)

    def _check(self, cond, event):
        if cond.route and cond.route != event.route: return False
        if cond.time_range:
            cur = event.timestamp.time(); tr = cond.time_range
            if tr.start <= tr.end:
                if not (tr.start <= cur <= tr.end): return False
            else:
                if not (cur >= tr.start or cur <= tr.end): return False
        actual = getattr(event, self._ATTR[cond.metric])
        return self._CMP[cond.comparator](actual, cond.value)


class ActionExecutor:
    def execute(self, rule, event):
        act = rule.action
        r = ActionResult(rule.raw_text, act.type, event.route, event.timestamp)
        if act.type == "add_trip":
            r.details = {"message": f"Adding extra trip on {event.route}",
                         "frequency_minutes": (act.frequency_seconds or 900)//60,
                         "reason": f"Load at {event.load_percent:.1f}%"}
        elif act.type == "cancel_trip":
            r.details = {"message": f"Cancelling low-demand trip", "passengers": event.passengers}
        elif act.type == "increase_capacity":
            r.details = {"message": f"Switching to high-capacity vehicle", "current_load": event.load_percent}
        elif act.type == "notify_operator":
            r.details = {"message": act.message, "route": event.route, "delay": event.delay_minutes}
        elif act.type == "reroute":
            r.details = {"from_route": event.route, "to_route": act.target_route,
                         "reason": f"Delay of {event.delay_minutes:.1f} min"}
        return r


class DSLEngine:
    def __init__(self):
        self._rules: List[Rule] = []
        self._evaluator = Evaluator()
        self._executor = ActionExecutor()

    def load_rule(self, source: str) -> Rule:
        tokens = tokenize(source.strip())
        rule = Parser(tokens, source.strip()).parse_rule()
        self._rules.append(rule); return rule

    def load_rules(self, sources): return [self.load_rule(s) for s in sources]
    def clear_rules(self): self._rules.clear()

    @property
    def rule_count(self): return len(self._rules)

    def process_event(self, event: SensorEvent) -> List[ActionResult]:
        fired = []
        for rule in self._rules:
            if self._evaluator.evaluate(rule, event):
                fired.append(self._executor.execute(rule, event))
        return fired
