"""
Hydrogen fuel-cell physics simulation.
No imports from other project packages — purely self-contained.
"""
import datetime as dt
import math
import random
from typing import Dict

import numpy as np

_RHO_H2 = 0.0899   # kg/m³
_LHV = 120e6        # J/kg  (lower heating value)


def initial_state() -> Dict[str, object]:
    return {
        "timestamp": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        "FC_STATE": 100.0,
        "H2_001FT": 0.0,
        "H2_001PT": 0.0,
        "H2_002PT": 0.0,
        "H2_003PT": 0.0,
        "H2_005PT": 0.0,
        "CA_001FC": 0.0,
        "H2_001TT": 0.0,
        "H2_002TT": 0.0,
        "H2_003TT": 0.0,
        "H2_005TT": 0.0,
        "FC_STACK_V": 0.0,
        "FC_STACK_i": 0.0,
    }


def generate_physical_state(
    prev: Dict[str, object],
    introduce_issues: bool = True,) -> Dict[str, object]:
    """
    Advance the simulation by one time step.

    Constants:
        RHO_H2 = 0.0899 kg/m³   (hydrogen density)
        LHV    = 120 MJ/kg       (lower heating value)
    """
    FC_STATE = prev.get("FC_STATE", 0)
    if random.random() < 0.01:
        FC_STATE = 100

    flow = prev.get("H2_001FT", 0)
    if FC_STATE == 100:
        flow = min(float(flow) + random.uniform(0.0, 0.2), 5.5)
    else:
        flow = float(flow) * 0.9

    P1 = float(prev.get("H2_001PT", 0)) + 0.05 * flow
    P2 = float(prev.get("H2_002PT", 0)) + 0.04 * P1
    P3 = float(prev.get("H2_003PT", 0)) + 0.03 * P2
    P4 = min(float(prev.get("H2_005PT", 0)) + 0.1 * P3, 200.0)

    base_temp = 13.0
    T1 = base_temp + math.sin(random.random()) + random.gauss(0, 0.1)
    T2 = base_temp - 0.5 + random.gauss(0, 0.1)
    T3 = base_temp - 1.0 + random.gauss(0, 0.1)
    T4 = base_temp - 1.5 + random.gauss(0, 0.1)

    voltage = 1.8 * (FC_STATE / 100)
    current = 20 * (flow / 5.5) + random.gauss(0, 0.5)

    def noisy(val: float) -> float:
        return val + random.gauss(0, 0.02)

    state: Dict[str, object] = {
        "timestamp": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        "FC_STATE": FC_STATE,
        "H2_001FT": noisy(flow),
        "H2_001PT": noisy(P1),
        "H2_002PT": noisy(P2),
        "H2_003PT": noisy(P3),
        "H2_005PT": noisy(P4),
        "CA_001FC": max(0.0, 0.5 * noisy(P3) + float(np.random.normal(0, 0.1))),
        "H2_001TT": noisy(T1),
        "H2_002TT": noisy(T2),
        "H2_003TT": noisy(T3),
        "H2_005TT": noisy(T4),
        "FC_STACK_V": noisy(voltage),
        "FC_STACK_i": noisy(current),
    }

    if introduce_issues:
        for key in list(state.keys()):
            if key == "timestamp":
                continue
            r = random.random()
            if r < 0.01:
                state[key] = float("nan")
            elif r < 0.02:
                state[key] = float("inf")
            elif r < 0.03:
                state[key] = float(state[key]) * random.choice([-10, 10])

    return state
