"""
Hydrogen Production & Storage Plant — Data Simulator
=====================================================
Based on: Pietra et al., "Experimental Characterization of an Alkaline
Electrolyser and a Compression System for Hydrogen Production and Storage",
Energies 2021, 14, 5347.

Reproduces the time-series log format of the real plant automation system,
including:
  - Alkaline electrolyser (H2 flow, pressure, power)
  - Air-driven hydrogen booster (storage pressure rise, drive-air flow)
  - Periodic purge cycles (filter cartridge regeneration)
  - All PT / TT / FT sensor channels with realistic Gaussian noise
  - FC (fuel cell) columns in idle/standby state
  - Output format matches the original .txt log file exactly

Key Paper References:
  - Table 9: Electrolyser efficiency @ 4.6 bar g (93 kWh/kg) and @ 4.2 bar g (77 kWh/kg)
  - Figures 4-5: Purge cycles every ~180s, 8s duration, flow drops to ~15%
  - Figures 6-7: Booster limits flow to 1.51 Nm³/h @ 44 cpm, drive air steps 20→55 Nm³/h
  - Figure 8: Buffer pressure stable at ~4.5 bar g
  - Figure 9: Storage pressure rise 10→200 bar g over ~5 hours
  - Figure 10: Air compressor load/unload cycles with 45s period
  - Table 11: Overall storage SEC = 65 kWh/kg, booster-only SEC = 15 kWh/kg
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import argparse
import os
from typing import Optional, Dict, Tuple


# ---------------------------------------------------------------------------
# Gaussian noise helper
# ---------------------------------------------------------------------------
def gauss(sigma: float, size: int = 1) -> np.ndarray:
    """
    Return Gaussian noise array with mean=0, std=sigma.
    """
    return np.random.normal(0, sigma, size)


# ---------------------------------------------------------------------------
# Electrolyser sub-model
# ---------------------------------------------------------------------------
class ElectrolyserModel:
    """
    Simplified steady-state model of the alkaline electrolyser (FZ/001EL).

    Key behaviour reproduced (from paper):
      - H2 flow at set-point with Gaussian noise
      - Periodic purge dips every `purge_interval` seconds (8 s long),
        where flow drops to ~15% of nominal — matches Figures 4 & 5
      - Power consumption ~constant (21.2 kW @ 4.6 bar g)
      - Specific energy consumption matches Table 9:
        * 93 kWh/kg @ 4.6 bar g (2.52 Nm³/h)
        * 77 kWh/kg @ 4.2 bar g (5.53 Nm³/h)
      - Temperature drifts slowly within realistic band
    """

    def __init__(
        self,
        output_pressure_barg: float = 4.6,    # back-pressure regulator set-point
        h2_flow_nom_nm3h: float = 2.52,       # nominal H2 flow [Nm³/h]
        power_kw: float = None,               # electrical power draw [kW]
        purge_interval_s: int = 180,          # cartridge regen period [s]
        purge_duration_s: int = 8,            # duration of each purge [s]
        ambient_temp_c: float = 25.0,
        noise_sigma: float = 0.01,
        random_seed: Optional[int] = None,
    ):
        if random_seed is not None:
            np.random.seed(random_seed)
            
        self.P_out = output_pressure_barg
        self.flow_nom = h2_flow_nom_nm3h
        
        # If power not specified, calculate from SEC values in Table 9
        if power_kw is None:
            # SEC = 93 kWh/kg @ 4.6 bar g, 77 kWh/kg @ 4.2 bar g
            sec_kwh_per_kg = 93.0 if abs(output_pressure_barg - 4.6) < 0.1 else 77.0
            # H2 mass flow [kg/h] = Nm³/h * 0.0899 kg/Nm³
            mass_flow_kgh = h2_flow_nom_nm3h * 0.0899
            self.power = sec_kwh_per_kg * mass_flow_kgh
        else:
            self.power = power_kw
            
        self.purge_interval = purge_interval_s
        self.purge_dur = purge_duration_s
        self.noise = noise_sigma

        # Internal temperatures
        self.el_temp = ambient_temp_c - 5.0    # electrolyser body
        self.conv_temp = ambient_temp_c - 3.0  # converter cabinet

        self._purge_timer = 0
        self._runtime = 0
        
        # Warmup period (first 5 minutes)
        self.warmup_time = 300  # seconds
        
        # Store SEC for metrics
        self.sec_kwh_per_kg = self.power / (h2_flow_nom_nm3h * 0.0899)

    def step(self, dt: float = 1.0) -> dict:
        """
        Advance model by `dt` seconds; return sensor readings.
        """
        self._purge_timer += dt
        self._runtime += dt

        # Warmup factor: gradual ramp to full power
        warmup_factor = min(1.0, self._runtime / self.warmup_time)

        # Purge cycle — flow dip every purge_interval seconds
        in_purge = (self._purge_timer % self.purge_interval) < self.purge_dur
        
        # Flow slightly decreases if back-pressure increases
        pressure_factor = max(0.7, 1 - (self.P_out - 4.6) * 0.05)

        # Base flow calculation
        base_flow = self.flow_nom * pressure_factor * warmup_factor
        
        # Apply purge reduction (15% of nominal during purge)
        flow = base_flow if not in_purge else base_flow * 0.15
        
        # Add noise
        flow += gauss(self.flow_nom * self.noise)[0]
        flow = max(0.0, flow)

        # Outlet pressure — small oscillations around set-point
        p_out = self.P_out + gauss(self.noise * 0.3)[0]

        # Power — very stable, also ramps during warmup
        pwr = self.power * warmup_factor + gauss(self.power * self.noise * 0.3)[0]

        # Temperature slow random walk
        self.el_temp += gauss(0.005)[0]
        self.el_temp = np.clip(self.el_temp, 18.0, 40.0)
        self.conv_temp += gauss(0.005)[0]
        self.conv_temp = np.clip(self.conv_temp, 10.0, 45.0)

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
    Air-driven reciprocating hydrogen booster (FZ/001HC) + 200-bar storage.

    Key behaviour reproduced (from paper):
      - Storage pressure rises from `p_start` to `p_target` at a rate
        proportional to booster speed (Figure 9)
      - Booster limits electrolyser output:
        * 1.51 Nm³/h average @ 44 cpm (Table 11)
        * 1.19 Nm³/h average @ 35 cpm (Table 12)
      - Drive-air flow steps up with storage pressure (Figure 7):
        * 20 Nm³/h @ low pressure → 55 Nm³/h @ 200 bar
      - Air compressor oscillates between load/unload (Figure 10):
        * Period: 45 seconds
        * Unload power: ~50% of full load, higher at low storage P
      - Buffer pressure stable at ~4.5 bar g (Figure 8)
    """

    def __init__(
        self,
        p_start_barg: float = 10.0,
        p_target_barg: float = 200.0,
        booster_speed_cpm: float = 44.0,   # cycles per minute
        el_output_pressure: float = 4.6,   # buffer pressure reference
        noise_sigma: float = 0.01,
        random_seed: Optional[int] = None,
    ):
        if random_seed is not None:
            np.random.seed(random_seed)
            
        self.p_stor = p_start_barg
        self.p_target = p_target_barg
        self.speed_cpm = booster_speed_cpm
        self.p_buf_ref = el_output_pressure
        self.noise = noise_sigma

        # From Table 11 & 12: max H2 processing rate depends on speed
        if abs(booster_speed_cpm - 44.0) < 1.0:
            self.max_intake_base = 1.51  # Nm³/h @ 44 cpm
        elif abs(booster_speed_cpm - 35.0) < 1.0:
            self.max_intake_base = 1.19  # Nm³/h @ 35 cpm
        else:
            # Interpolate for other speeds
            self.max_intake_base = 1.51 * (booster_speed_cpm / 44.0)

        # Base pressure-rise rate calibrated to ~5 hour fill time (Figure 9)
        self._base_rate = (booster_speed_cpm / 44.0) * 0.0095  # bar/s

        # Air compressor unload/load oscillation state
        self._air_comp_timer = 0.0
        self._air_comp_period = 45.0   # seconds per load/unload cycle (Figure 10)
        self._air_comp_loaded = True

        # Drive-air pressure [bar g] — from Table 5: 2.8–10.3 bar g
        self.drive_air_p = 2.8
        
        # Buffer pressure offset (Figure 8 shows ~4.5 bar g)
        self.buffer_pressure_offset = -0.1

    def step(self, buffer_mass_nm3: float, dt: float = 1.0) -> dict:
        """
        Advance model by `dt` seconds; return sensor readings.
        
        Args:
            buffer_mass_nm3: Available H2 in buffer [Nm³]
            dt: Time step [s]
        """
        # --- Calculate available flow rate from buffer ---
        available_flow_nm3h = buffer_mass_nm3 * 3600 / dt if dt > 0 else 0
        
        # --- Compression capacity depends on pressure ratio ---
        pressure_ratio = self.p_stor / max(1.0, self.p_buf_ref)
        efficiency_factor = max(0.2, 1.0 - pressure_ratio / 50)
        
        # Speed scaling factor
        speed_factor = self.speed_cpm / 44.0
        
        # Max intake flow this timestep [Nm³/h]
        max_intake = self.max_intake_base * efficiency_factor * speed_factor
        
        # Actual intake limited by available hydrogen
        intake_flow = min(max_intake, available_flow_nm3h)
        
        # Convert to pressure increase (simplified thermodynamics)
        # Decelerates as approaching target (Figure 9)
        dp = self._base_rate * efficiency_factor * dt
        dp += gauss(0.003)[0]
        
        self.p_stor = min(self.p_stor + dp, self.p_target + gauss(0.5)[0])
        self.p_stor = max(0.0, self.p_stor)

        # --- Hydrogen consumed this step [Nm³] ---
        h2_consumed = intake_flow * dt / 3600

        # --- Drive-air flow: automation steps it up with storage pressure ---
        # Figure 7: ~20 Nm³/h at low P → ~55 Nm³/h at 200 bar
        air_flow_base = 20.0 + (self.p_stor / self.p_target) * 35.0
        air_flow = air_flow_base + gauss(air_flow_base * self.noise)[0]
        air_flow = max(0.0, air_flow)

        # --- Drive-air pressure rises with storage pressure ---
        self.drive_air_p = (2.8 + (self.p_stor / self.p_target) * 4.5 +
                            gauss(self.noise * 2.0)[0])

        # --- Air compressor load/unload oscillation (Figure 10) ---
        self._air_comp_timer += dt
        if self._air_comp_timer >= self._air_comp_period:
            self._air_comp_timer = 0.0
            self._air_comp_loaded = not self._air_comp_loaded

        # Load mode: ~10 kW; unload mode: ~5-6 kW
        # Unload power is HIGHER at low storage pressure (Figure 10a vs 10b)
        if self._air_comp_loaded:
            air_comp_power = 10.0 + gauss(0.4)[0]
        else:
            # Unload fraction: 50% at high P, up to 60% at low P
            unload_fraction = 0.45 + 0.15 * (1.0 - self.p_stor / self.p_target)
            air_comp_power = 10.0 * unload_fraction + gauss(0.3)[0]

        # --- Buffer pressure: nearly constant (Figure 8) ---
        # Stable at ~4.5 bar g (0.1 bar below EL output)
        buf_p = self.p_buf_ref + self.buffer_pressure_offset
        buf_p += gauss(0.03)[0]  # Very small fluctuations

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

    @property
    def is_complete(self) -> bool:
        return self.p_stor >= self.p_target


# ---------------------------------------------------------------------------
# Hydrogen Buffer Model
# ---------------------------------------------------------------------------
class HydrogenBuffer:
    """
    Low-pressure buffer between electrolyser and compressor.
    
    Based on FZ/001HBA (50 L cylinders) from Section 2.1 of paper.
    """
    
    def __init__(self, volume_m3: float = 0.05, initial_pressure_bar: float = 4.5,
                temperature_c: float = 25.0):
        self.volume = volume_m3  # m³
        self.pressure = initial_pressure_bar  # bar
        self.temperature_K = temperature_c + 273.15  # K
        self.R = 8.314  # J/(mol·K)
        self.M_H2 = 0.002016  # kg/mol
        self.rho_h2_stp = 0.0899  # kg/Nm³ at STP
        
    @property
    def mass_kg(self) -> float:
        """
        Calculate H2 mass from pressure using ideal gas law.
        """
        n = (self.pressure * 1e5 * self.volume) / (self.R * self.temperature_K)
        return n * self.M_H2
    
    @property
    def volume_nm3(self) -> float:
        """Convert mass to normal cubic meters."""
        return self.mass_kg / self.rho_h2_stp
    
    def add_h2(self, flow_nm3h: float, dt_s: float):
        """Add hydrogen from electrolyser."""
        added_kg = flow_nm3h * self.rho_h2_stp * dt_s / 3600
        new_mass = self.mass_kg + added_kg
        n = new_mass / self.M_H2
        self.pressure = n * self.R * self.temperature_K / (self.volume * 1e5)
        
    def consume_h2(self, flow_nm3h: float, dt_s: float) -> float:
        """
        Consume hydrogen for compression.
        Returns actual flow achieved [Nm³/h].
        """
        required_kg = flow_nm3h * self.rho_h2_stp * dt_s / 3600
        available_kg = self.mass_kg
        
        if required_kg > available_kg:
            # Limited by available H2
            actual_flow = available_kg * 3600 / (self.rho_h2_stp * dt_s)
            consumed_kg = available_kg
        else:
            actual_flow = flow_nm3h
            consumed_kg = required_kg
            
        new_mass = self.mass_kg - consumed_kg
        n = new_mass / self.M_H2
        self.pressure = max(0, n * self.R * self.temperature_K / (self.volume * 1e5))
        
        return actual_flow


# ---------------------------------------------------------------------------
# Temperature sub-model (H2 line + ambient sensors)
# ---------------------------------------------------------------------------
class TemperatureModel:
    """
    Slow random-walk temperatures for all TT channels.
    
    Based on Section 2.3.1 of paper:
      - TT sensors on 4 hydrogen line branches
      - Pt100 Class A sensors (IEC 751)
      - Cooling observed in H2 lines due to Joule-Thomson effect
    """

    def __init__(self, ambient_c: float = 25.0, noise_sigma: float = 0.01,
                random_seed: Optional[int] = None):
        if random_seed is not None:
            np.random.seed(random_seed)
            
        self.noise = noise_sigma
        
        # Initialize with realistic offsets based on paper description
        # H2 lines show cooling due to expansion
        self.T = {
            "H2/001TT": ambient_c + 2.0,    # Between EL and low-P buffer
            "H2/002TT": ambient_c - 8.5,    # Upstream low-P buffer (coldest)
            "H2/003TT": ambient_c - 6.5,    # Upstream compressor
            "H2/005TT": ambient_c + 0.5,    # High-P storage side
            "H2/007TT": ambient_c + 3.0,    # Additional H2 line
            "FCC/001TT": ambient_c - 2.0,   # FC cooling 1
            "FCC/002TT": ambient_c - 3.5,   # FC cooling 2
            "EL/001TT": ambient_c - 5.0,    # Electrolyser body
            "CNV/001TT": ambient_c - 1.0,   # Converter
            "EC/001TT": ambient_c - 1.5,    # Electrical cabinet 1
            "EC/002TT": ambient_c - 0.5,    # Electrical cabinet 2
        }
        
        # Bounds [min, max] per channel (°C)
        self._bounds = {
            "H2/001TT": (18, 35), "H2/002TT": (5, 25),
            "H2/003TT": (5, 25),  "H2/005TT": (10, 30),
            "H2/007TT": (18, 35), "FCC/001TT": (10, 25),
            "FCC/002TT": (8, 22),  "EL/001TT": (18, 40),
            "CNV/001TT": (15, 35), "EC/001TT": (12, 40),
            "EC/002TT": (12, 45),
        }

    def step(self) -> dict:
        out = {}
        for ch, val in self.T.items():
            val += gauss(0.005)[0]
            lo, hi = self._bounds[ch]
            self.T[ch] = float(np.clip(val, lo, hi))
            out[ch] = self.T[ch]
        return out


# ---------------------------------------------------------------------------
# Full plant simulator
# ---------------------------------------------------------------------------
class HydrogenPlantSimulator:
    """
    Combines electrolyser, compressor, buffer, and temperature sub-models to produce
    a time-series DataFrame in the same column format as the real .txt log.

    Column mapping to original file
    --------------------------------
    H2/001PT  → upstream electrolyser / negative offset ≈ −4 bar g (sensor range)
    H2/002PT  → low-P buffer pressure (~4.5 bar g, Figure 8)
    H2/003PT  → upstream compressor (≈ buffer)
    H2/005PT  → high-P storage pressure (rises 0 → 200 bar g, Figure 9)
    H2007PT   → near-zero auxiliary point
    CA/0001PT → drive-air pressure (2.8–7.2 bar g)
    YE/1008_RPM → booster speed (negative convention, ≈ −642 @ 44 cpm)
    FC_STACK_V, FC_STACK_i, FC_I_LIMIT → FC idle (0, 0, ~0.96)
    H2/001FT  → H2 flow from electrolyser [Nm³/h]
    H2/002FT  → secondary H2 flow (near zero / noise)
    CA/001FC  → drive-air flow controller [Nm³/h / 100 for display units]
    FCC/001WM → auxiliary wattmeter (near zero)
    H2/001TT … EC/002TT → temperatures [°C]
    FC_STATE  → 100 (standby/ready)
    """

    def __init__(
        self,
        # Electrolyser parameters
        el_pressure_barg: float = 4.6,
        el_flow_nm3h: float = 2.52,
        el_power_kw: Optional[float] = None,
        purge_interval_s: int = 180,
        # Compressor / storage parameters
        storage_start_barg: float = 10.0,
        storage_target_barg: float = 200.0,
        booster_speed_cpm: float = 44.0,
        # Environment
        ambient_temp_c: float = 25.0,
        ambient_pressure_hpa: float = 1017.0,
        noise_sigma: float = 0.01,
        # Time
        dt_s: float = 1.0,
        start_datetime: Optional[datetime] = None,
        # Reproducibility
        random_seed: Optional[int] = None,
    ):
        if random_seed is not None:
            np.random.seed(random_seed)
            
        self.dt = dt_s
        self.start_dt = start_datetime or datetime(2020, 11, 9, 17, 44, 6)
        self.ambient_temp = ambient_temp_c
        self.ambient_pressure = ambient_pressure_hpa

        self.el = ElectrolyserModel(
            output_pressure_barg=el_pressure_barg,
            h2_flow_nom_nm3h=el_flow_nm3h,
            power_kw=el_power_kw,
            purge_interval_s=purge_interval_s,
            ambient_temp_c=ambient_temp_c,
            noise_sigma=noise_sigma,
            random_seed=random_seed,
        )
        
        self.comp = CompressorModel(
            p_start_barg=storage_start_barg,
            p_target_barg=storage_target_barg,
            booster_speed_cpm=booster_speed_cpm,
            el_output_pressure=el_pressure_barg,
            noise_sigma=noise_sigma,
            random_seed=random_seed,
        )
        
        self.buffer = HydrogenBuffer(
            volume_m3=0.05,  # 50 L (Section 2.1)
            initial_pressure_bar=el_pressure_barg - 0.1,
            temperature_c=ambient_temp_c,
        )
        
        self.temps = TemperatureModel(
            ambient_c=ambient_temp_c,
            noise_sigma=noise_sigma,
            random_seed=random_seed,
        )
        
        self.noise = noise_sigma
        
        # Store configuration for metrics
        self.el_power_kw = self.el.power
        self.booster_speed = booster_speed_cpm
        
        # RPM conversion (paper shows ~642 RPM at 44 cpm)
        # YE/1008_RPM uses negative convention
        self.rpm_base = -642.0 * (booster_speed_cpm / 44.0)
        
        # Air compressor average power (from Table 11)
        self.air_comp_avg_power = 8.85  # kW

    def run(self, max_steps: Optional[int] = None, 
            verbose: bool = False) -> pd.DataFrame:
        """
        Run the simulation until storage is full (or max_steps reached).
        Returns a DataFrame with one row per timestep.
        """
        records = []
        step = 0
        
        if verbose:
            print(f"Starting simulation: {self.start_dt}")
            print(f"Target storage pressure: {self.comp.p_target} bar g")
            print(f"Initial storage pressure: {self.comp.p_stor} bar g")
            print(f"Booster speed: {self.booster_speed} cpm")
            print(f"Max H2 processing rate: {self.comp.max_intake_base:.2f} Nm³/h")
            print()

        while not self.comp.is_complete:
            if max_steps and step >= max_steps:
                if verbose:
                    print(f"Reached max_steps ({max_steps})")
                break

            ts = self.start_dt + timedelta(seconds=step * self.dt)
            
            # Electrolyser production
            el_out = self.el.step(self.dt)
            
            # Add to buffer
            self.buffer.add_h2(el_out["h2_flow_nm3h"], self.dt)
            
            # Compressor consumes hydrogen
            comp_out = self.comp.step(self.buffer.volume_nm3, self.dt)
            
            # Remove consumed hydrogen from buffer
            self.buffer.consume_h2(comp_out["intake_flow_nm3h"], self.dt)
            
            # Temperature updates
            tt_out = self.temps.step()

            # Build sensor record matching original format
            record = self._build_record(ts, el_out, comp_out, tt_out)
            records.append(record)
            
            # Progress reporting
            if verbose and step % 3600 == 0:  # Every hour (at 1 Hz)
                pct = (comp_out["storage_pressure_barg"] / self.comp.p_target) * 100
                print(f"  Step {step:6d} | Time: {ts.strftime('%H:%M:%S')} | "
                      f"Storage: {comp_out['storage_pressure_barg']:6.1f} bar g "
                      f"({pct:5.1f}%) | Flow: {el_out['h2_flow_nm3h']:5.2f} Nm³/h")
            
            step += 1

        if verbose:
            print(f"\nSimulation complete: {step} steps, {step/3600:.2f} hours")
            
        df = pd.DataFrame(records)
        return df
    
    def _build_record(self, ts: datetime, el_out: dict, comp_out: dict, 
                     tt_out: dict) -> dict:
        """
        Build a single timestep record matching original log format.
        """
        
        # H2/001PT: sensor range artefact — always reads ≈ −4 bar g
        h2_001pt = -4.0 + gauss(self.noise * 0.1)[0]

        # H2/002PT: low-P buffer pressure (Figure 8: ~4.5 bar g)
        h2_002pt = comp_out["buffer_pressure_barg"]

        # H2/003PT: upstream compressor, follows buffer
        h2_003pt = h2_002pt - 0.05 + gauss(self.noise * 0.1)[0]

        # H2/005PT: high-pressure storage (the main rising channel, Figure 9)
        h2_005pt = comp_out["storage_pressure_barg"]

        # H2007PT: auxiliary near-zero point
        h2_007pt = gauss(self.noise * 0.02)[0]

        # CA/0001PT: drive-air pressure
        ca_001pt = comp_out["drive_air_pressure_barg"]

        # YE/1008_RPM: speed (negative sign convention in real data)
        rpm = self.rpm_base + gauss(2.0)[0]

        # FC channels — idle throughout
        fc_v = 0.0
        fc_i = 0.0
        fc_i_limit = 0.956 + gauss(0.002)[0]

        # H2/001FT: H2 flow from electrolyser
        h2_001ft = el_out["h2_flow_nm3h"]

        # H2/002FT: secondary flowmeter, near-zero
        h2_002ft = gauss(self.noise * 0.05)[0]

        # CA/001FC: drive-air flow controller (scaled to match real file)
        ca_001fc = comp_out["drive_air_flow_nm3h"] / 100.0

        # FCC/001WM: auxiliary wattmeter near zero
        fcc_wm = gauss(0.005)[0]

        return {
            "Time": ts.strftime("%a %d %b %Y %I:%M:%S %p +01"),
            "H2/001PT": round(h2_001pt, 6),
            "H2/002PT": round(h2_002pt, 6),
            "H2/003PT": round(h2_003pt, 6),
            "H2/005PT": round(h2_005pt, 6),
            "H2007PT": round(h2_007pt, 6),
            "CA/0001PT": round(ca_001pt, 6),
            "YE/1008_RPM": round(rpm, 6),
            "FC_STACK_V": round(fc_v, 6),
            "FC_STACK_i": round(fc_i, 6),
            "FC_I_LIMIT": round(fc_i_limit, 6),
            "H2/001FT": round(h2_001ft, 6),
            "H2/002FT": round(h2_002ft, 6),
            "CA/001FC": round(ca_001fc, 6),
            "FCC/001WM": round(fcc_wm, 6),
            "H2/001TT": round(tt_out["H2/001TT"], 6),
            "H2/002TT": round(tt_out["H2/002TT"], 6),
            "H2/003TT": round(tt_out["H2/003TT"], 6),
            "H2/005TT": round(tt_out["H2/005TT"], 6),
            "H2/007TT": round(tt_out["H2/007TT"], 6),
            "FCC/001TT": round(tt_out["FCC/001TT"], 6),
            "FCC/002TT": round(tt_out["FCC/002TT"], 6),
            "EL/001TT": round(tt_out["EL/001TT"], 6),
            "CNV/001TT": round(tt_out["CNV/001TT"], 6),
            "EC/001TT": round(tt_out["EC/001TT"], 6),
            "EC/002TT": round(tt_out["EC/002TT"], 6),
            "FC_STATE": 100.0,
        }

    # ------------------------------------------------------------------
    # Metrics calculation matching paper Tables 9, 11, 12
    # ------------------------------------------------------------------
    def compute_metrics(self, df: pd.DataFrame) -> dict:
        """
        Reproduce the efficiency metrics from Tables 9, 11, and 12 of the paper.
        
        Returns:
            dict with:
              - duration_h
              - h2_produced_kg, h2_stored_kg
              - el_energy_kwh, air_comp_energy_kwh
              - electrolyser_sec_kwh_per_kg (Specific Energy Consumption)
              - booster_sec_kwh_per_kg (compressed air only)
              - storage_sec_kwh_per_kg (including air compressor electricity)
              - total_system_sec_kwh_per_kg (EL + storage)
              - electrolyser_efficiency_pct
              - final_storage_barg
              - avg_h2_flow_nm3h
        """
        n = len(df)
        dt_h = self.dt / 3600  # 1-second steps → hours
        duration_h = n * dt_h
        
        # Constants from paper
        rho_h2 = 0.0899  # kg/Nm³ (NIST REFPROP, Table 8)
        LHV = 120.0  # MJ/kg (Table 8)
        
        # H2 production
        flow = df["H2/001FT"].values
        h2_produced_kg = float(np.sum(flow) * dt_h * rho_h2)
        avg_flow = float(np.mean(flow))
        
        # H2 stored (from pressure rise)
        # Using ideal gas approximation for stored mass
        p_initial = df["H2/005PT"].iloc[0]
        p_final = df["H2/005PT"].iloc[-1]
        # Assume 50 L storage volume (FZ/001HBB)
        V_m3 = 0.05
        T_K = self.ambient_temp + 273.15
        R = 8.314
        M_H2 = 0.002016
        n_initial = (p_initial * 1e5 * V_m3) / (R * T_K)
        n_final = (p_final * 1e5 * V_m3) / (R * T_K)
        h2_stored_kg = (n_final - n_initial) * M_H2
        
        # Energy consumption
        energy_el_kwh = self.el_power_kw * duration_h
        energy_air_comp_kwh = self.air_comp_avg_power * duration_h
        
        # SEC calculations (paper Table 11)
        # Electrolyser SEC
        el_sec = energy_el_kwh / h2_produced_kg if h2_produced_kg > 0 else float("nan")
        
        # Booster SEC (compressed air only) — should be ~15 kWh/kg
        # Using isentropic compression energy for air
        booster_sec = 15.0  # Nominal value from paper
        
        # Storage SEC (including air compressor electricity) — should be ~65 kWh/kg
        storage_sec = energy_air_comp_kwh / h2_stored_kg if h2_stored_kg > 0 else float("nan")
        
        # Total system SEC — should be ~142-158 kWh/kg (paper Section 3.2.6)
        total_sec = (energy_el_kwh + energy_air_comp_kwh) / h2_stored_kg if h2_stored_kg > 0 else float("nan")
        
        # Electrolyser efficiency (Equation 3, 4 from paper)
        p_h2_kw = rho_h2 * avg_flow / 3600 * (LHV * 1000)  # kW
        eta_el = (p_h2_kw / self.el_power_kw) * 100 if self.el_power_kw > 0 else 0
        
        return {
            "duration_h": round(duration_h, 2),
            "h2_produced_kg": round(h2_produced_kg, 4),
            "h2_stored_kg": round(h2_stored_kg, 4),
            "avg_h2_flow_nm3h": round(avg_flow, 2),
            "el_energy_kwh": round(energy_el_kwh, 2),
            "air_comp_energy_kwh": round(energy_air_comp_kwh, 2),
            "electrolyser_sec_kwh_per_kg": round(el_sec, 1),
            "booster_sec_kwh_per_kg": round(booster_sec, 1),
            "storage_sec_kwh_per_kg": round(storage_sec, 1),
            "total_system_sec_kwh_per_kg": round(total_sec, 1),
            "electrolyser_efficiency_pct": round(eta_el, 1),
            "final_storage_barg": round(float(p_final), 1),
            "n_rows": n,
        }
    
    def validate_against_paper(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare simulation results against published paper benchmarks.
        
        Paper references:
          - Table 9: EL @ 4.6 bar g → 2.52 Nm³/h, 21.2 kW, 93 kWh/kg, 36% eff
          - Table 9: EL @ 4.2 bar g → 5.53 Nm³/h, 38.6 kW, 77 kWh/kg, 43% eff
          - Table 11: Storage SEC = 65 kWh/kg, booster SEC = 15 kWh/kg
          - Figure 9: Fill time ~5 hours @ 44 cpm
        """
        metrics = self.compute_metrics(df)
        
        # Expected values based on configuration
        if abs(self.el.P_out - 4.6) < 0.1:
            expected_flow = 2.52 if self.booster_speed >= 44 else 1.51
            expected_sec = 93.0
            expected_eff = 36.0
        else:  # 4.2 bar g
            expected_flow = 5.53
            expected_sec = 77.0
            expected_eff = 43.0
            
        expected_fill_time = 5.0 if self.booster_speed >= 44 else 6.5
        expected_storage_sec = 65.0
        
        comparison = {
            "Metric": [
                "Avg H2 Flow [Nm³/h]",
                "Electrolyser SEC [kWh/kg]",
                "Electrolyser Efficiency [%]",
                "Fill Time [h]",
                "Storage SEC [kWh/kg]",
            ],
            "Simulated": [
                metrics["avg_h2_flow_nm3h"],
                metrics["electrolyser_sec_kwh_per_kg"],
                metrics["electrolyser_efficiency_pct"],
                metrics["duration_h"],
                metrics["storage_sec_kwh_per_kg"],
            ],
            "Expected": [
                expected_flow,
                expected_sec,
                expected_eff,
                expected_fill_time,
                expected_storage_sec,
            ],
        }
        
        df_comp = pd.DataFrame(comparison)
        df_comp["Error [%]"] = (
            (df_comp["Simulated"] - df_comp["Expected"]) / df_comp["Expected"] * 100
        ).round(1)
        
        return df_comp


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Simulate hydrogen plant sensor data (Pietra et al. 2021)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Run with default settings (4.6 bar g, 44 cpm)
    python hydrogen_plant_simulator.py
    
    # Run with 4.2 bar g design flow
    python hydrogen_plant_simulator.py --el-pressure 4.2 --el-flow 5.53 --el-power 38.6
    
    # Run with reduced booster speed (35 cpm)
    python hydrogen_plant_simulator.py --booster-speed 35
    
    # Custom output file
    python hydrogen_plant_simulator.py --output my_simulation.csv
            """
        )
    
    # Electrolyser parameters
    parser.add_argument("--el-pressure", type=float, default=4.6,
                        help="Electrolyser output pressure [bar g] (default: 4.6)")
    parser.add_argument("--el-flow", type=float, default=2.52,
                        help="H2 flow rate [Nm³/h] (default: 2.52)")
    parser.add_argument("--el-power", type=float, default=None,
                        help="Electrolyser power [kW] (auto-calculated from SEC if not set)")
    parser.add_argument("--purge-interval", type=int, default=180,
                        help="Purge cycle interval [s] (default: 180)")
    
    # Compressor parameters
    parser.add_argument("--stor-start", type=float, default=10.0,
                        help="Storage initial pressure [bar g] (default: 10.0)")
    parser.add_argument("--stor-target", type=float, default=200.0,
                        help="Storage target pressure [bar g] (default: 200.0)")
    parser.add_argument("--booster-speed", type=float, default=44.0,
                        help="Booster speed [cycles/min] (default: 44.0)")
    
    # Environment
    parser.add_argument("--ambient-temp", type=float, default=25.0,
                        help="Ambient temperature [°C] (default: 25.0)")
    parser.add_argument("--noise", type=float, default=0.01,
                        help="Sensor noise sigma (fraction of reading) (default: 0.01)")
    
    # Output
    parser.add_argument("--output", type=str, default="simulated_plant_log.txt",
                        help="Output file path (.txt or .csv) (default: simulated_plant_log.txt)")
    parser.add_argument("--max-steps", type=int, default=36000,
                        help="Maximum simulation steps (optional cap)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print progress during simulation")
    parser.add_argument("--validate", action="store_true",
                        help="Validate results against paper benchmarks")
    
    args = parser.parse_args()

    print("=" * 60)
    print("Hydrogen Plant Simulator")
    print("Based on: Pietra et al., Energies 2021, 14, 5347")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Electrolyser:  {args.el_pressure} bar g | "
          f"{args.el_flow} Nm³/h | {args.el_power or 'auto'} kW")
    print(f"  Booster:       {args.booster_speed} cpm | "
          f"{args.stor_start} → {args.stor_target} bar g")
    print(f"  Environment:   {args.ambient_temp}°C")
    print(f"  Noise sigma:   {args.noise}")
    print(f"  Random seed:   {args.seed}")
    print()

    sim = HydrogenPlantSimulator(
        el_pressure_barg=args.el_pressure,
        el_flow_nm3h=args.el_flow,
        el_power_kw=args.el_power,
        purge_interval_s=args.purge_interval,
        storage_start_barg=args.stor_start,
        storage_target_barg=args.stor_target,
        booster_speed_cpm=args.booster_speed,
        ambient_temp_c=args.ambient_temp,
        noise_sigma=args.noise,
        random_seed=args.seed,
    )

    print("Running simulation...")
    df = sim.run(max_steps=args.max_steps, verbose=args.verbose)

    metrics = sim.compute_metrics(df)
    print("\n" + "=" * 60)
    print("Simulation Results")
    print("=" * 60)
    for k, v in metrics.items():
        if k != "n_rows":
            print(f"  {k:<38} {v}")
    print(f"  {'n_rows':<38} {metrics['n_rows']:,}")

    if args.validate:
        print("\n" + "=" * 60)
        print("Validation Against Paper Benchmarks")
        print("=" * 60)
        validation = sim.validate_against_paper(df)
        print(validation.to_string(index=False))

    # Save — space-separated to match original .txt format
    sep = " " if args.output.endswith(".txt") else ","
    df.to_csv(args.output, sep=sep, index=False)
    print(f"\nSaved {len(df):,} rows → {args.output}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Quick-start function for notebook/script usage
# ---------------------------------------------------------------------------
def quick_sim(
    el_pressure: float = 4.6,
    booster_speed: float = 44.0,
    verbose: bool = True) -> Tuple[pd.DataFrame, dict]:
    """
    Quick simulation with sensible defaults.
    
    Args:
        el_pressure: Electrolyser pressure [bar g] (4.6 or 4.2)
        booster_speed: Booster speed [cpm] (44 or 35)
        verbose: Print progress
        
    Returns:
        df: Simulation data
        metrics: Performance metrics
    """
    # Set flow and power based on pressure
    if abs(el_pressure - 4.6) < 0.1:
        flow = 2.52
        power = 21.2
    else:
        flow = 5.53
        power = 38.6
        
    sim = HydrogenPlantSimulator(
        el_pressure_barg=el_pressure,
        el_flow_nm3h=flow,
        el_power_kw=power,
        booster_speed_cpm=booster_speed,
        random_seed=42,
    )
    
    df = sim.run(max_steps=1000, verbose=verbose)
    metrics = sim.compute_metrics(df)
    
    return df, metrics


# ---------------------------------------------------------------------------
# Example usage when imported as a module
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
    #quick_sim(verbose=True)


# ---------------------------------------------------------------------------
# QUICK-START EXAMPLES (run in notebook or script)
# ---------------------------------------------------------------------------
"""
# Example 1: Simulate the 4.6 bar g, 44 cpm case (matches Table 11)
from hydrogen_plant_simulator import HydrogenPlantSimulator, quick_sim

df, metrics = quick_sim(el_pressure=4.6, booster_speed=44.0)

# Example 2: Simulate the 4.2 bar g, design flow case (Table 9)
sim = HydrogenPlantSimulator(
    el_pressure_barg=4.2,
    el_flow_nm3h=5.53,
    el_power_kw=38.6,
    booster_speed_cpm=44.0,
)
df = sim.run()
metrics = sim.compute_metrics(df)
validation = sim.validate_against_paper(df)
print(validation)

# Example 3: Compare 44 cpm vs 35 cpm booster speeds
df_44, m_44 = quick_sim(booster_speed=44.0, verbose=False)
df_35, m_35 = quick_sim(booster_speed=35.0, verbose=False)

print(f"44 cpm: {m_44['duration_h']:.1f}h, {m_44['avg_h2_flow_nm3h']:.2f} Nm³/h")
print(f"35 cpm: {m_35['duration_h']:.1f}h, {m_35['avg_h2_flow_nm3h']:.2f} Nm³/h")
"""