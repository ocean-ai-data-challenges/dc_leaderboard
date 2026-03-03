#!/usr/bin/env python3
"""
Generate a test per_bins JSON file with proper lat/lon grid bins.

This simulates what the Class4Evaluator would produce when configured
with 1° lat/lon binning (the default), after pd.Interval objects are
serialised to their string representation (e.g. ``"(10, 11]"``).

Each entry in ``per_bins_by_time`` carries a ``ref_alias`` field that
identifies which reference dataset was used.  The generator produces
distinct data characteristics per reference type:

* **Grid references** (glorys, argo_profiles): dense, full lat/lon
  coverage — every grid cell has a value.
* **Satellite altimetry references** (saral, jason3, swot): sparse,
  track-like coverage — only cells along simulated ascending /
  descending satellite passes are populated.

Usage::

    python -m dcleaderboard.gen_per_bins_latlon [--resolution 5] [--output results/results_glonet_per_bins.json]
"""
from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple
from loguru import logger


# Reference datasets used in the data challenge.
# "grid" refs → dense lat/lon coverage.
# "track" refs → sparse satellite-track coverage.
GRID_REFS: List[str] = ["glorys", "argo_profiles"]
TRACK_REFS: List[str] = ["saral", "jason3", "swot"]


def _interval_str(left: int, right: int) -> str:
    """Format an interval like pd.Interval.__str__: ``'(left, right]'``."""
    return f"({left}, {right}]"


def _satellite_track_cells(
    lat_edges: List[int],
    lon_edges: List[int],
    n_passes: int,
    rng: random.Random,
    seed_offset: int = 0,
) -> Set[Tuple[int, int]]:
    """Return a set of (i_lat, i_lon) indices that fall along simulated
    ascending + descending satellite passes.

    Each pass is a diagonal stripe across the globe with a width of
    roughly one grid cell.  Passes are spaced evenly in longitude so
    that across 10 lead-times we get sparse but realistic coverage.
    """
    n_lat = len(lat_edges) - 1
    n_lon = len(lon_edges) - 1
    cells: Set[Tuple[int, int]] = set()

    # Inclination slightly different per satellite family
    # (SARAL/Jason: ~66°, SWOT: ~77°)
    inclination = 66 + (seed_offset % 3) * 5.5  # degrees

    # Slope: how many lon indices per lat index (ascending pass)
    slope = math.tan(math.radians(inclination))

    lon_spacing = max(1, n_lon // n_passes)

    for p in range(n_passes):
        # Starting longitude index for this pass (ascending)
        lon_start_idx = (p * lon_spacing + seed_offset * 3) % n_lon

        for i_lat in range(n_lat):
            # Ascending pass
            i_lon_asc = int(lon_start_idx + slope * i_lat) % n_lon
            cells.add((i_lat, i_lon_asc))
            # Width ±1 cell to make track slightly wider
            cells.add((i_lat, (i_lon_asc + 1) % n_lon))

            # Descending pass (same slope, shifted ~half orbit)
            i_lon_desc = int(lon_start_idx + n_lon // 2 - slope * i_lat) % n_lon
            cells.add((i_lat, i_lon_desc))
            cells.add((i_lat, (i_lon_desc + 1) % n_lon))

    return cells


def _make_per_bins_for_ref(
    ref_alias: str,
    is_track: bool,
    variables: List[str],
    lat_edges: List[int],
    lon_edges: List[int],
    lt: int,
    t_idx: int,
    base_rmse: Dict[str, float],
    rng: random.Random,
    n_passes: int = 6,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build the ``per_bins`` dict for a single time entry and reference."""
    n_lat = len(lat_edges) - 1
    n_lon = len(lon_edges) - 1

    # For track references, compute which cells are visible in this pass
    if is_track:
        # Each lead_time / time_step shifts the track slightly (orbit precession)
        track_cells = _satellite_track_cells(
            lat_edges, lon_edges,
            n_passes=n_passes,
            rng=rng,
            seed_offset=lt * 3 + t_idx,
        )
    else:
        track_cells = None  # all cells populated

    # Small bias offset per ref so refs are visually distinguishable
    ref_bias_offset = {
        "glorys": 0.0,
        "argo_profiles": +0.015,
        "saral": -0.02,
        "jason3": +0.025,
        "swot": -0.01,
    }.get(ref_alias, 0.0)

    per_bins: Dict[str, List[Dict[str, Any]]] = {}
    for var in variables:
        bins_list: List[Dict[str, Any]] = []
        base = base_rmse.get(var, 0.3)
        # Satellite refs tend to have lower RMSE for altimetry variables
        if is_track and "height" in var.lower():
            base *= 0.6

        for i_lat in range(n_lat):
            lat_left = lat_edges[i_lat]
            lat_right = lat_edges[i_lat + 1]
            lat_center = (lat_left + lat_right) / 2

            lat_factor = 1.0 + 0.5 * abs(lat_center) / 90.0

            for i_lon in range(n_lon):
                # Skip cells not on the satellite track
                if track_cells is not None and (i_lat, i_lon) not in track_cells:
                    continue

                lon_left = lon_edges[i_lon]
                lon_right = lon_edges[i_lon + 1]

                noise = rng.gauss(0, 0.15)
                rmse_val = max(0.001, base * lat_factor * (1 + 0.3 * lt / 10) + noise * base)
                bias_val = rng.gauss(ref_bias_offset, base * 0.1)
                n_pts = max(1, int(rng.gauss(500 if not is_track else 80, 100)
                                   * max(0.05, math.cos(math.radians(lat_center)))))

                bins_list.append({
                    "lat_bin": _interval_str(lat_left, lat_right),
                    "lon_bin": _interval_str(lon_left, lon_right),
                    "rmse": round(rmse_val, 6),
                    "bias": round(bias_val, 6),
                    "n_points": n_pts,
                })

        per_bins[var] = bins_list

    return per_bins


def generate_per_bins_data(
    resolution: int = 5,
    n_lead_times: int = 10,
    n_time_entries_per_lead: int = 2,
    variables: list[str] | None = None,
    ref_aliases: list[str] | None = None,
    seed: int = 42,
) -> Dict[str, Any]:
    """Generate synthetic per_bins data with lat/lon grid bins.

    Parameters
    ----------
    resolution : int
        Grid resolution in degrees (default 5 → 36×72 = 2592 cells).
    n_lead_times : int
        Number of lead-time days.
    n_time_entries_per_lead : int
        Number of time entries per lead time.
    variables : list[str] | None
        Variable names. Defaults to a representative subset.
    ref_aliases : list[str] | None
        Reference dataset identifiers to generate entries for.
        Defaults to GRID_REFS + TRACK_REFS.
    seed : int
        Random seed for reproducibility.
    """
    rng = random.Random(seed)

    if variables is None:
        variables = [
            "Surface salinity",
            "Surface temperature",
            "Surface eastward velocity",
            "Surface northward velocity",
            "Surface height",
        ]

    if ref_aliases is None:
        ref_aliases = GRID_REFS + TRACK_REFS

    lat_edges = list(range(-90, 91, resolution))
    lon_edges = list(range(-180, 180, resolution))

    n_lat = len(lat_edges) - 1
    n_lon = len(lon_edges) - 1
    logger.info("Grid: {} lat × {} lon = {} cells at {}° resolution",
                 n_lat, n_lon, n_lat * n_lon, resolution)
    logger.info("Reference datasets: {}", ref_aliases)

    base_rmse: Dict[str, float] = {
        "Surface salinity": 0.35,
        "Surface temperature": 0.5,
        "Surface eastward velocity": 0.08,
        "Surface northward velocity": 0.07,
        "Surface height": 0.04,
    }

    entries: List[Dict[str, Any]] = []
    base_date = datetime(2024, 1, 10)

    for ref_alias in ref_aliases:
        is_track = ref_alias in TRACK_REFS
        for lt in range(n_lead_times):
            for t_idx in range(n_time_entries_per_lead):
                ref_time = base_date + timedelta(days=t_idx * 10)
                valid_time = ref_time + timedelta(days=lt)

                per_bins = _make_per_bins_for_ref(
                    ref_alias=ref_alias,
                    is_track=is_track,
                    variables=variables,
                    lat_edges=lat_edges,
                    lon_edges=lon_edges,
                    lt=lt,
                    t_idx=t_idx,
                    base_rmse=base_rmse,
                    rng=rng,
                )

                entry: Dict[str, Any] = {
                    "valid_time": valid_time.isoformat(),
                    "forecast_reference_time": ref_time.isoformat(),
                    "lead_time": lt,
                    "ref_alias": ref_alias,
                    "ref_type": "observation" if is_track else "gridded",
                    "per_bins": per_bins,
                }
                entries.append(entry)

    return {
        "dataset": "glonet",
        "per_bins_by_time": entries,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate test per_bins with lat/lon grid")
    parser.add_argument("--resolution", type=int, default=5,
                        help="Grid resolution in degrees (default: 5)")
    parser.add_argument("--output", type=str,
                        default="dcleaderboard/results/results_glonet_per_bins.json",
                        help="Output JSON file path")
    parser.add_argument("--n-lead-times", type=int, default=10)
    parser.add_argument("--n-time-entries", type=int, default=2,
                        help="Number of time entries per lead time")
    parser.add_argument("--refs", type=str, nargs="*",
                        default=None,
                        help="Reference dataset aliases to include (default: all)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    data = generate_per_bins_data(
        resolution=args.resolution,
        n_lead_times=args.n_lead_times,
        n_time_entries_per_lead=args.n_time_entries,
        ref_aliases=args.refs,
        seed=args.seed,
    )

    logger.info("Writing {} entries to {} ...", len(data['per_bins_by_time']), args.output)
    with open(args.output, "w") as f:
        json.dump(data, f, indent=2)
    logger.success("Done.")


if __name__ == "__main__":
    main()
