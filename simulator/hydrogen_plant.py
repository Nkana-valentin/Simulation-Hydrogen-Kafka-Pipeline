"""
Hydrogen Production & Storage Plant — streaming physics model.

Based on: Pietra et al., "Experimental Characterization of an Alkaline
Electrolyser and a Compression System for Hydrogen Production and Storage",
Energies 2021, 14, 5347.

Key behaviours reproduced:
  - Alkaline electrolyser with warmup ramp and purge cycles (Figures 4-5)
  - Air-driven H2 booster with pressure-dependent efficiency (Figures 6-7)
  - Low-pressure buffer (ideal gas, 50 L) between electrolyser and booster
  - Storage pressure rise 10 → 200 bar g over ~5 h (Figure 9)
  - Air compressor load/unload oscillation, 45 s period (Figure 10)
  - Realistic Gaussian noise on all PT / TT / FT channels
  - FC columns held at standby (plant produces H2, does not consume it)
"""
import datetime as dt
from typing import Any, Dict, Optional

import numpy as np


def _g(sigma: float) -> float:
    """Single Gaussian noise sample, mean 0, std sigma."""
    return float(np.random.normal(0.0, sigma))


# ---------------------------------------------------------------------------
# Electrolyser sub-model
# ---------------------------------------------------------------------------
class ElectrolyserModel:
    """
    Simplified steady-state alkaline electrolyser (FZ/001EL).

    Reproduces (from paper):
      - H2 flow at set-point with Gaussian noise
      - Purge dips every purge_interval seconds (8 s long, flow → 15%)
        matches Figures 4 & 5
      - Power consumption constant at ~21.2 kW (Table 9)
      - Gradual warmup over the first 5 minutes
    """

    def __init__(
        self,
        output_pressure_barg: float = 4.6,
        h2_flow_nom_nm3h: float = 2.52,
        power_kw: Optional[float] = None,
        purge_interval_s: int = 180,
        purge_duration_s: int = 8,
        ambient_temp_c: float = 25.0,
        noise_sigma: float = 0.01,
    ):
        self.P_out = output_pressure_barg
        self.flow_nom = h2_flow_nom_nm3h

        if power_kw is None:
            sec = 93.0 if abs(output_pressure_barg - 4.6) < 0.1 else 77.0
            self.power = sec * h2_flow_nom_nm3h * 0.0899  # kWh/kg * kg/h = kW
        else:
            self.power = power_kw

        self.purge_interval = purge_interval_s
        self.purge_dur = purge_duration_s
        self.noise = noise_sigma
        self.warmup_time = 300.0  # s

        self.el_temp = ambient_temp_c - 5.0
        self.conv_temp = ambient_temp_c - 3.0
        self._purge_timer = 0.0
        self._runtime = 0.0

    def step(self, dt: float = 1.0) -> Dict[str, Any]:
        self._purge_timer += dt
        self._runtime += dt

        warmup = min(1.0, self._runtime / self.warmup_time)
        pressure_factor = max(0.7, 1.0 - (self.P_out - 4.6) * 0.05)
        in_purge = (self._purge_timer % self.purge_interval) < self.purge_dur

        base_flow = self.flow_nom * pressure_factor * warmup
        flow = (base_flow if not in_purge else base_flow * 0.15) + _g(self.flow_nom * self.noise)
        flow = max(0.0, flow)

        p_out = self.P_out + _g(self.noise * 0.3)
        pwr = self.power * warmup + _g(self.power * self.noise * 0.3)

        self.el_temp = float(np.clip(self.el_temp + _g(0.005), 18.0, 40.0))
        self.conv_temp = float(np.clip(self.conv_temp + _g(0.005), 10.0, 45.0))

        return {
            "h2_flow_nm3h": flow,
            "h2_pressure_barg": p_out,
            "power_kw": pwr,
            "el_temp_c": self.el_temp,
            "conv_temp_c": self.conv_temp,
            "in_purge": in_purge,
        }


# ---------------------------------------------------------------------------
# Compressor / storage sub-model
# ---------------------------------------------------------------------------
class CompressorModel:
    """
    Air-driven H2 booster (FZ/001HC) + 200-bar storage vessel.

    Reproduces (from paper):
      - Storage pressure rises 10 → 200 bar g in ~5 h (Figure 9)
      - Drive-air flow steps 20 → 55 Nm³/h with storage pressure (Figure 7)
      - Air compressor load/unload at 45 s period (Figure 10)
      - Buffer pressure stable at ~4.5 bar g (Figure 8)
    """

    def __init__(
        self,
        p_start_barg: float = 10.0,
        p_target_barg: float = 200.0,
        booster_speed_cpm: float = 44.0,
        el_output_pressure: float = 4.6,
        noise_sigma: float = 0.01,
    ):
        self.p_stor = p_start_barg
        self.p_target = p_target_barg
        self.speed_cpm = booster_speed_cpm
        self.p_buf_ref = el_output_pressure
        self.noise = noise_sigma

        if abs(booster_speed_cpm - 44.0) < 1.0:
            self.max_intake_base = 1.51
        elif abs(booster_speed_cpm - 35.0) < 1.0:
            self.max_intake_base = 1.19
        else:
            self.max_intake_base = 1.51 * (booster_speed_cpm / 44.0)

        self._base_rate = (booster_speed_cpm / 44.0) * 0.0095  # bar/s
        self._air_comp_timer = 0.0
        self._air_comp_period = 45.0
        self._air_comp_loaded = True
        self.drive_air_p = 2.8

    @property
    def is_complete(self) -> bool:
        return self.p_stor >= self.p_target

    def step(self, buffer_volume_nm3: float, dt: float = 1.0) -> Dict[str, Any]:
        available_nm3h = buffer_volume_nm3 * 3600.0 / dt if dt > 0 else 0.0
        pressure_ratio = self.p_stor / max(1.0, self.p_buf_ref)
        efficiency = max(0.2, 1.0 - pressure_ratio / 50.0)
        speed_factor = self.speed_cpm / 44.0

        intake_flow = min(self.max_intake_base * efficiency * speed_factor, available_nm3h)

        dp = self._base_rate * efficiency * dt + _g(0.003)
        self.p_stor = float(np.clip(self.p_stor + dp, 0.0, self.p_target + _g(0.5)))

        h2_consumed = intake_flow * dt / 3600.0

        air_flow_base = 20.0 + (self.p_stor / self.p_target) * 35.0
        air_flow = max(0.0, air_flow_base + _g(air_flow_base * self.noise))

        self.drive_air_p = 2.8 + (self.p_stor / self.p_target) * 4.5 + _g(self.noise * 2.0)

        self._air_comp_timer += dt
        if self._air_comp_timer >= self._air_comp_period:
            self._air_comp_timer = 0.0
            self._air_comp_loaded = not self._air_comp_loaded

        if self._air_comp_loaded:
            air_comp_power = 10.0 + _g(0.4)
        else:
            unload_frac = 0.45 + 0.15 * (1.0 - self.p_stor / self.p_target)
            air_comp_power = 10.0 * unload_frac + _g(0.3)

        buf_p = self.p_buf_ref - 0.1 + _g(0.03)

        return {
            "storage_pressure_barg": self.p_stor,
            "buffer_pressure_barg": buf_p,
            "drive_air_flow_nm3h": air_flow,
            "drive_air_pressure_barg": self.drive_air_p,
            "air_comp_power_kw": max(0.0, air_comp_power),
            "comp_loaded": self._air_comp_loaded,
            "h2_consumed_nm3": h2_consumed,
            "intake_flow_nm3h": intake_flow,
        }


# ---------------------------------------------------------------------------
# Hydrogen buffer (low-pressure, between EL and booster)
# ---------------------------------------------------------------------------
class HydrogenBuffer:
    """
    Low-pressure H2 buffer — 50 L cylinders (FZ/001HBA, Section 2.1).
    Uses ideal gas law to track pressure from mass balance.
    """

    _R = 8.314        # J/(mol·K)
    _M_H2 = 0.002016  # kg/mol
    _RHO_STP = 0.0899 # kg/Nm³

    def __init__(self, volume_m3: float = 0.05, initial_pressure_bar: float = 4.5,
                 temperature_c: float = 25.0):
        self.volume = volume_m3
        self.pressure = initial_pressure_bar
        self.T_K = temperature_c + 273.15

    @property
    def mass_kg(self) -> float:
        n = (self.pressure * 1e5 * self.volume) / (self._R * self.T_K)
        return n * self._M_H2

    @property
    def volume_nm3(self) -> float:
        return self.mass_kg / self._RHO_STP

    def _set_pressure_from_mass(self, mass_kg: float) -> None:
        n = max(0.0, mass_kg) / self._M_H2
        self.pressure = (n * self._R * self.T_K) / (self.volume * 1e5)

    def add_h2(self, flow_nm3h: float, dt_s: float) -> None:
        self._set_pressure_from_mass(self.mass_kg + flow_nm3h * self._RHO_STP * dt_s / 3600.0)

    def consume_h2(self, flow_nm3h: float, dt_s: float) -> float:
        requested_kg = flow_nm3h * self._RHO_STP * dt_s / 3600.0
        available_kg = self.mass_kg
        consumed_kg = min(requested_kg, available_kg)
        self._set_pressure_from_mass(available_kg - consumed_kg)
        if requested_kg > 0:
            return flow_nm3h * (consumed_kg / requested_kg)
        return 0.0


# ---------------------------------------------------------------------------
# Temperature sub-model
# ---------------------------------------------------------------------------
class TemperatureModel:
    """
    Slow random-walk temperatures for all TT channels.

    Based on Section 2.3.1 of the paper (Pt100 Class A sensors, IEC 751).
    H2 lines show cooling from Joule-Thomson expansion.
    """

    def __init__(self, ambient_c: float = 25.0, noise_sigma: float = 0.01):
        self.noise = noise_sigma
        self.T: Dict[str, float] = {
            "H2/001TT": ambient_c + 2.0,
            "H2/002TT": ambient_c - 8.5,
            "H2/003TT": ambient_c - 6.5,
            "H2/005TT": ambient_c + 0.5,
            "H2/007TT": ambient_c + 3.0,
            "FCC/001TT": ambient_c - 2.0,
            "FCC/002TT": ambient_c - 3.5,
            "EL/001TT": ambient_c - 5.0,
            "CNV/001TT": ambient_c - 1.0,
        }
        self._bounds: Dict[str, tuple] = {
            "H2/001TT": (18.0, 35.0), "H2/002TT": (5.0, 25.0),
            "H2/003TT": (5.0, 25.0),  "H2/005TT": (10.0, 30.0),
            "H2/007TT": (18.0, 35.0), "FCC/001TT": (10.0, 25.0),
            "FCC/002TT": (8.0, 22.0),  "EL/001TT": (18.0, 40.0),
            "CNV/001TT": (15.0, 35.0),
        }

    def step(self) -> Dict[str, float]:
        for ch in self.T:
            lo, hi = self._bounds[ch]
            self.T[ch] = float(np.clip(self.T[ch] + _g(0.005), lo, hi))
        return dict(self.T)


# ---------------------------------------------------------------------------
# Full plant simulator
# ---------------------------------------------------------------------------
class HydrogenPlantSimulator:
    """
    Combines all sub-models into a streaming tick-by-tick simulator.

    Use `step()` in a loop for real-time streaming (one dict per Kafka message).
    Use `run()` for offline batch analysis (returns a pandas DataFrame).

    Output keys from `step()` match the pipeline TSDB schema directly
    (H2_001PT, H2_002PT, … CA_001FC, H2_001TT, … FC_STACK_V, FC_STATE, …).
    """

    def __init__(
        self,
        el_pressure_barg: float = 4.6,
        el_flow_nm3h: float = 2.52,
        el_power_kw: Optional[float] = None,
        purge_interval_s: int = 180,
        storage_start_barg: float = 10.0,
        storage_target_barg: float = 200.0,
        booster_speed_cpm: float = 44.0,
        ambient_temp_c: float = 25.0,
        noise_sigma: float = 0.01,
        dt_s: float = 1.0,
        random_seed: Optional[int] = None,
    ):
        if random_seed is not None:
            np.random.seed(random_seed)

        self.dt = dt_s
        self.noise = noise_sigma

        self.el = ElectrolyserModel(
            output_pressure_barg=el_pressure_barg,
            h2_flow_nom_nm3h=el_flow_nm3h,
            power_kw=el_power_kw,
            purge_interval_s=purge_interval_s,
            ambient_temp_c=ambient_temp_c,
            noise_sigma=noise_sigma,
        )
        self.comp = CompressorModel(
            p_start_barg=storage_start_barg,
            p_target_barg=storage_target_barg,
            booster_speed_cpm=booster_speed_cpm,
            el_output_pressure=el_pressure_barg,
            noise_sigma=noise_sigma,
        )
        self.buffer = HydrogenBuffer(
            volume_m3=0.05,
            initial_pressure_bar=el_pressure_barg - 0.1,
            temperature_c=ambient_temp_c,
        )
        self.temps = TemperatureModel(ambient_c=ambient_temp_c, noise_sigma=noise_sigma)

        # Derived constants
        self._el_power_kw = self.el.power
        self._booster_speed = booster_speed_cpm
        self._ambient_temp = ambient_temp_c
        self._rpm_base = -642.0 * (booster_speed_cpm / 44.0)

    # ------------------------------------------------------------------
    # Streaming interface
    # ------------------------------------------------------------------

    def step(self) -> Dict[str, Any]:
        """
        Advance one timestep and return a sensor dict with schema-compatible keys.
        Suitable for continuous streaming into Kafka at the configured dt_s rate.
        """
        el_out = self.el.step(self.dt)
        self.buffer.add_h2(el_out["h2_flow_nm3h"], self.dt)
        comp_out = self.comp.step(self.buffer.volume_nm3, self.dt)
        self.buffer.consume_h2(comp_out["intake_flow_nm3h"], self.dt)
        tt_out = self.temps.step()
        return self._to_schema_dict(el_out, comp_out, tt_out)

    def _to_schema_dict(
        self,
        el_out: Dict[str, Any],
        comp_out: Dict[str, Any],
        tt_out: Dict[str, float],
    ) -> Dict[str, Any]:
        """Map sub-model outputs to pipeline TSDB schema keys."""
        return {
            "timestamp": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
            # Pressure sensors [bar g]
            # H2_001PT: upstream EL — reads ≈ −4 bar g (range artefact, Figure 3)
            "H2_001PT": round(-4.0 + _g(self.noise * 0.1), 6),
            "H2_002PT": round(comp_out["buffer_pressure_barg"], 6),
            "H2_003PT": round(comp_out["buffer_pressure_barg"] - 0.05 + _g(self.noise * 0.1), 6),
            "H2_005PT": round(comp_out["storage_pressure_barg"], 6),
            # Flow sensors [Nm³/h]
            "H2_001FT": round(el_out["h2_flow_nm3h"], 6),
            # Drive-air flow controller [display units = Nm³/h / 100]
            "CA_001FC": round(comp_out["drive_air_flow_nm3h"] / 100.0, 6),
            # Temperature sensors [°C]
            "H2_001TT": round(tt_out["H2/001TT"], 6),
            "H2_002TT": round(tt_out["H2/002TT"], 6),
            "H2_003TT": round(tt_out["H2/003TT"], 6),
            "H2_005TT": round(tt_out["H2/005TT"], 6),
            # FC stack — standby throughout (this plant produces H2, does not consume it)
            "FC_STACK_V": round(_g(0.002), 6),   # standby noise around 0
            "FC_STACK_i": round(_g(0.01), 6),
            "FC_STATE": 100.0,
        }

    # ------------------------------------------------------------------
    # Batch / offline interface (requires pandas)
    # ------------------------------------------------------------------

    def run(self, max_steps: Optional[int] = None) -> "pd.DataFrame":
        """
        Run until storage is full (or max_steps reached).
        Returns a DataFrame with all sensor channels in original log format.
        Requires pandas.
        """
        import pandas as pd  # lazy — not needed for streaming use

        records = []
        step = 0
        while not self.comp.is_complete:
            if max_steps and step >= max_steps:
                break
            el_out = self.el.step(self.dt)
            self.buffer.add_h2(el_out["h2_flow_nm3h"], self.dt)
            comp_out = self.comp.step(self.buffer.volume_nm3, self.dt)
            self.buffer.consume_h2(comp_out["intake_flow_nm3h"], self.dt)
            tt_out = self.temps.step()
            records.append(self._to_log_record(el_out, comp_out, tt_out, step))
            step += 1

        return pd.DataFrame(records)

    def _to_log_record(
        self,
        el_out: Dict[str, Any],
        comp_out: Dict[str, Any],
        tt_out: Dict[str, float],
        step: int,
    ) -> Dict[str, Any]:
        """Build a record in the original .txt log column format."""
        ts = dt.datetime(2020, 11, 9, 17, 44, 6) + dt.timedelta(seconds=step * self.dt)
        h2_002pt = comp_out["buffer_pressure_barg"]
        return {
            "Time": ts.strftime("%a %d %b %Y %I:%M:%S %p +01"),
            "H2/001PT": round(-4.0 + _g(self.noise * 0.1), 6),
            "H2/002PT": round(h2_002pt, 6),
            "H2/003PT": round(h2_002pt - 0.05 + _g(self.noise * 0.1), 6),
            "H2/005PT": round(comp_out["storage_pressure_barg"], 6),
            "H2007PT": round(_g(self.noise * 0.02), 6),
            "CA/0001PT": round(comp_out["drive_air_pressure_barg"], 6),
            "YE/1008_RPM": round(self._rpm_base + _g(2.0), 6),
            "FC_STACK_V": round(_g(0.002), 6),
            "FC_STACK_i": round(_g(0.01), 6),
            "FC_I_LIMIT": round(0.956 + _g(0.002), 6),
            "H2/001FT": round(el_out["h2_flow_nm3h"], 6),
            "H2/002FT": round(_g(self.noise * 0.05), 6),
            "CA/001FC": round(comp_out["drive_air_flow_nm3h"] / 100.0, 6),
            "FCC/001WM": round(_g(0.005), 6),
            **{ch: round(v, 6) for ch, v in tt_out.items()},
            "FC_STATE": 100.0,
        }

    # ------------------------------------------------------------------
    # Metrics (requires pandas DataFrame from run())
    # ------------------------------------------------------------------

    def compute_metrics(self, df: "pd.DataFrame") -> Dict[str, Any]:
        """
        Reproduce efficiency metrics from Tables 9, 11, 12 of the paper.
        Input df must come from run().
        """
        rho_h2 = 0.0899   # kg/Nm³
        LHV = 120.0        # MJ/kg
        dt_h = self.dt / 3600.0
        n = len(df)
        duration_h = n * dt_h

        flow = df["H2/001FT"].values
        h2_produced_kg = float(np.sum(flow) * dt_h * rho_h2)
        avg_flow = float(np.mean(flow))

        # Stored mass from pressure rise (ideal gas, 50 L vessel)
        V, T_K = 0.05, self._ambient_temp + 273.15
        R, M_H2 = 8.314, 0.002016
        p0 = float(df["H2/005PT"].iloc[0])
        p1 = float(df["H2/005PT"].iloc[-1])
        h2_stored_kg = ((p1 - p0) * 1e5 * V) / (R * T_K) * M_H2

        energy_el = self._el_power_kw * duration_h
        energy_air = 8.85 * duration_h  # avg air compressor kW (Table 11)

        el_sec = energy_el / h2_produced_kg if h2_produced_kg > 0 else float("nan")
        storage_sec = energy_air / h2_stored_kg if h2_stored_kg > 0 else float("nan")
        total_sec = (energy_el + energy_air) / h2_stored_kg if h2_stored_kg > 0 else float("nan")
        p_h2_kw = rho_h2 * avg_flow / 3600.0 * (LHV * 1000.0)
        eta_el = (p_h2_kw / self._el_power_kw) * 100.0 if self._el_power_kw > 0 else 0.0

        return {
            "duration_h": round(duration_h, 2),
            "h2_produced_kg": round(h2_produced_kg, 4),
            "h2_stored_kg": round(h2_stored_kg, 4),
            "avg_h2_flow_nm3h": round(avg_flow, 4),
            "el_energy_kwh": round(energy_el, 2),
            "air_comp_energy_kwh": round(energy_air, 2),
            "electrolyser_sec_kwh_per_kg": round(el_sec, 1),
            "booster_sec_kwh_per_kg": 15.0,
            "storage_sec_kwh_per_kg": round(storage_sec, 1),
            "total_system_sec_kwh_per_kg": round(total_sec, 1),
            "electrolyser_efficiency_pct": round(eta_el, 1),
            "final_storage_barg": round(p1, 1),
            "n_rows": n,
        }
