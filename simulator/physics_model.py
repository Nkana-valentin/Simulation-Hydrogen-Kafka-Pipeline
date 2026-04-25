"""
Compatibility shim — wraps HydrogenPlantSimulator for the producer loop.

cmd/producer.py calls:
    state = initial_state()
    while True:
        state = generate_physical_state(state)

The simulator is stateful (purge timers, buffer pressure, storage rise…)
so a module-level instance is created once and advanced each call.
"""
from typing import Any, Dict, Optional

from simulator.hydrogen_plant import HydrogenPlantSimulator

_sim: Optional[HydrogenPlantSimulator] = None


def initial_state() -> Dict[str, Any]:
    """Instantiate the plant simulator and return the first tick."""
    global _sim
    _sim = HydrogenPlantSimulator()
    return _sim.step()


def generate_physical_state(
    prev: Dict[str, Any],
    introduce_issues: bool = False,
) -> Dict[str, Any]:
    """
    Advance the simulation by one timestep.
    `prev` is accepted for API compatibility but not used — state is
    maintained internally by the simulator instance.
    """
    global _sim
    if _sim is None:
        _sim = HydrogenPlantSimulator()
    return _sim.step()
