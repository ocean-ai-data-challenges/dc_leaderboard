"""
Pre-processing module for per-bins results data.

Reads *_per_bins.json files, aggregates spatial data by
(model, variable, metric, lead_time), and writes compact JSON
files for the interactive map page.
"""
from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger

# Known aliases: base variable names used in regular results that
# differ from the canonical per_bins names.
_VARIABLE_ALIASES: Dict[str, str] = {
    "height": "ssh",
    "sea surface height": "ssh",
}


def _mean(values: List[float]) -> float:
    """Compute mean, ignoring NaN."""
    valid = [v for v in values if v is not None and not math.isnan(v)]
    return sum(valid) / len(valid) if valid else float("nan")


# Regex for pd.Interval string representation: "(left, right]"
_INTERVAL_RE = re.compile(
    r"^[\(\[]\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*[\)\]]$"
)


def _parse_interval_string(label: str) -> Tuple[float, float]:
    """Parse a pd.Interval string like ``'(-90, -85]'`` into (left, right).

    Also handles dict bins ``{"left": …, "right": …}`` (caller should
    check for dict first) and named-band labels ``'80S-70S'``.
    """
    m = _INTERVAL_RE.match(label)
    if m:
        return float(m.group(1)), float(m.group(2))
    # Fall back to named-band parser
    return _parse_named_band(label)


def _parse_named_band(label: str) -> Tuple[float, float]:
    """Parse a named band label like ``'80S-70S'`` or ``'30W-0'`` into numeric bounds.

    Handles latitude (N/S suffixes) and longitude (E/W suffixes).

    Returns (left, right) in degrees where left <= right.
    Examples::

        '80S-70S'    -> (-80.0, -70.0)
        '10S-0'      -> (-10.0, 0.0)
        '0-10N'      -> (0.0, 10.0)
        '70N-80N'    -> (70.0, 80.0)
        '180W-150W'  -> (-180.0, -150.0)
        '0-30E'      -> (0.0, 30.0)
    """
    parts = label.split("-")
    if len(parts) != 2:
        raise ValueError(f"Cannot parse bin label: {label!r}")

    def _to_float(s: str) -> float:
        s = s.strip()
        if not s or s == "0":
            return 0.0
        su = s.upper()
        if su.endswith("S"):
            return -float(s[:-1])
        if su.endswith("N"):
            return float(s[:-1])
        if su.endswith("W"):
            return -float(s[:-1])
        if su.endswith("E"):
            return float(s[:-1])
        return float(s)

    south = _to_float(parts[0])
    north = _to_float(parts[1])
    if south > north:
        south, north = north, south
    return (south, north)


def _parse_bin_value(raw: Any) -> Tuple[float, float]:
    """Parse any bin value (dict, interval-string, or named-band) into (left, right)."""
    if isinstance(raw, dict):
        return float(raw["left"]), float(raw["right"])
    return _parse_interval_string(str(raw))


def load_per_bins_files(results_dir: Path) -> List[Dict[str, Any]]:
    """Load all *_per_bins.json/.jsonl files from the results directory.

    Supports two formats:
    - Legacy ``.json``: a single JSON object with a ``per_bins_by_time`` list.
    - New ``.jsonl``: one compact JSON object per line (no wrapper dict).
      Each line is a per-bins entry.  The file is read into the same
      in-memory structure as the legacy format so the rest of the
      pipeline is unchanged.
    """
    # Collect all per-bins files; JSONL takes precedence over JSON when both
    # exist for the same stem (e.g. after a format migration).
    jsonl_files = sorted(results_dir.glob("*_per_bins.jsonl"))
    json_files = sorted(results_dir.glob("*_per_bins.json"))

    # Filter out any legacy .json files that have a .jsonl counterpart.
    jsonl_stems = {f.stem for f in jsonl_files}
    json_files = [f for f in json_files if f.stem not in jsonl_stems]

    files = jsonl_files + json_files

    datasets = []
    for f in files:
        logger.debug("Loading per-bins file: {} ...", f.name)
        if f.suffix == ".jsonl":
            entries: List[Dict[str, Any]] = []
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
            # Re-wrap in the legacy dict structure expected downstream.
            dataset_name = f.stem.replace("_per_bins", "").lstrip("results_")
            data: Dict[str, Any] = {
                "dataset": dataset_name,
                "per_bins_by_time": entries,
            }
        else:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
        logger.debug("  {} entries loaded from {}", len(data.get('per_bins_by_time', [])), f.name)
        datasets.append(data)
    return datasets


def _normalize_variable_name(var_raw: str) -> str:
    """Extract the canonical base variable name from a result variable string.

    Strips depth/level prefixes ("Surface ", "200m ", …) and applies
    alias normalisation so that e.g. ``"Surface height"`` becomes ``"ssh"``.
    """
    base = re.sub(
        r"^(Surface\s+|\d+m\s+)", "", var_raw, flags=re.IGNORECASE
    ).strip().lower()
    return _VARIABLE_ALIASES.get(base, base)


def extract_ref_variable_mapping(
    results_dir: Path,
    per_bins_variables: Optional[set] = None,
) -> Dict[str, List[str]]:
    """Extract reference-dataset → variable mapping from regular results.

    Reads the standard ``results_*.json`` files (excluding ``*_per_bins.json``)
    and builds, for every ``ref_alias``, the list of per-bins variable names
    that can be associated with that reference.

    Variable names are normalised: depth prefixes are stripped and known
    aliases are resolved (e.g. ``"Surface height"`` → ``"ssh"``).  Only
    variables that also exist in *per_bins_variables* (when provided) are
    kept, so the dropdown only offers variables for which map data exists.
    """
    ref_to_vars: Dict[str, set] = defaultdict(set)

    # Build a quick lookup that maps both raw names *and* normalised
    # names back to the canonical per-bins variable name.  This way
    # ``"Surface salinity"`` (raw match) as well as ``"salinity"``
    # (normalised match) both resolve correctly.
    _pb_lookup: Dict[str, str] = {}
    if per_bins_variables is not None:
        for pv in per_bins_variables:
            _pb_lookup[pv] = pv                         # raw name
            _pb_lookup[_normalize_variable_name(pv)] = pv  # normalised

    for f in sorted(results_dir.glob("results_*.json")):
        if "per_bins" in f.name:
            continue
        with open(f) as fh:
            data = json.load(fh)
        entries_by_dc = data.get("results", {})
        for dc_key, entries in entries_by_dc.items():
            for entry in entries:
                result = entry.get("result")
                if result is None:
                    continue
                ref = entry.get("ref_alias")
                if not ref:
                    continue
                for item in result:
                    var_raw = item.get("Variable", "")
                    if not var_raw:
                        continue
                    if per_bins_variables is not None:
                        # Try raw name first, then normalised
                        canonical = _pb_lookup.get(var_raw) or _pb_lookup.get(
                            _normalize_variable_name(var_raw)
                        )
                        if canonical:
                            ref_to_vars[ref].add(canonical)
                    else:
                        ref_to_vars[ref].add(var_raw)

    # Sort for deterministic output
    return {ref: sorted(vars) for ref, vars in sorted(ref_to_vars.items())}


def _extract_ref_type_map(results_dir: Path) -> Dict[str, str]:
    """Build a {ref_alias: "gridded"|"observation"} mapping from regular results.

    Reads the ``ref_is_observation`` boolean field present in standard
    ``results_*.json`` entries (\'True\' → ``"observation"``,
    ``False`` / absent → ``"gridded"``).
    """
    mapping: Dict[str, str] = {}
    for f in sorted(results_dir.glob("results_*.json")):
        if "per_bins" in f.name:
            continue
        with open(f) as fh:
            data = json.load(fh)
        for entries in data.get("results", {}).values():
            for entry in entries:
                ra = entry.get("ref_alias")
                if ra and ra not in mapping:
                    is_obs = entry.get("ref_is_observation")
                    if is_obs is not None:
                        mapping[ra] = "observation" if is_obs else "gridded"
    return mapping


def discover_metadata(
    datasets: List[Dict[str, Any]],
    results_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Discover available models, variables, metrics, lead times, depth bins."""
    models = []
    variables: set = set()
    metrics: set = set()
    lead_times: set = set()
    depth_bins_by_var: Dict[str, set] = defaultdict(set)

    skip_keys = {"time_bin", "lat_bin", "lon_bin", "depth_bin", "count", "n_points"}

    for ds in datasets:
        model = ds["dataset"]
        models.append(model)
        for entry in ds["per_bins_by_time"]:
            lead_times.add(entry["lead_time"])
            for var_name, bins in entry["per_bins"].items():
                variables.add(var_name)
                if bins:
                    sample = bins[0]
                    for k in sample:
                        if k not in skip_keys:
                            metrics.add(k)
                    if "depth_bin" in sample:
                        for b in bins:
                            db = b["depth_bin"]
                            depth_bins_by_var[var_name].add(
                                (db["left"], db["right"])
                            )

    # Sort depth bins
    sorted_depths: Dict[str, List[Tuple[float, float]]] = {}
    for var, dbs in depth_bins_by_var.items():
        sorted_depths[var] = sorted(dbs, key=lambda x: x[0])

    # "Reference Dataset" dropdown and rendering style come from the
    # REGULAR results files (results_*.json), not from per_bins.
    # Per_bins entries carry ref_type ("gridded"/"observation") which
    # determines rendering; the actual reference dataset NAMES (glorys,
    # saral, ...) are stored in the regular results' ref_alias field.
    ref_variables: Dict[str, List[str]] = {}
    ref_type_map: Dict[str, str] = {}  # ref_alias → "gridded" | "observation"
    if results_dir is not None:
        ref_variables = extract_ref_variable_mapping(
            results_dir, per_bins_variables=variables,
        )
        ref_type_map = _extract_ref_type_map(results_dir)

    # Include a special "all" lead-time for the composite across all days.
    all_lead_times = sorted(lead_times) + ["all"]

    return {
        "models": sorted(models),
        "variables": sorted(variables),
        "metrics": sorted(metrics),
        "lead_times": all_lead_times,
        "depth_bins": sorted_depths,
        "ref_variables": ref_variables,
        "ref_type_map": ref_type_map,
        "grid_type": "lat_band" if _is_lat_band_only_from_sets(variables, datasets) else "spatial",
    }


def _extract_lat_lon(b: Dict[str, Any]) -> Tuple[float, float]:
    """Extract (lat, lon) centre from a per-bins record.

    Supports three bin formats:
    * **dict** bins: ``{"lat_bin": {"left": …, "right": …}, "lon_bin": …}``
    * **interval string**: ``{"lat_bin": "(-90, -85]", "lon_bin": "(-180, -175]"}``
    * **named band** (latitude only): ``{"lat_bin": "80S-70S"}``

    When ``lon_bin`` is absent, lon is set to 0.0.
    """
    lat_left, lat_right = _parse_bin_value(b["lat_bin"])
    lat = round((lat_left + lat_right) / 2, 2)

    if "lon_bin" in b:
        lon_left, lon_right = _parse_bin_value(b["lon_bin"])
        lon = round((lon_left + lon_right) / 2, 2)
    else:
        lon = 0.0

    return lat, lon


def _extract_lat_lon_bounds(b: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """Extract (lat_left, lat_right, lon_left, lon_right) from a per-bins record.

    When ``lon_bin`` is absent, returns (-180, 180) spanning all longitudes.
    """
    lat_left, lat_right = _parse_bin_value(b["lat_bin"])
    if "lon_bin" in b:
        lon_left, lon_right = _parse_bin_value(b["lon_bin"])
    else:
        lon_left, lon_right = -180.0, 180.0
    return lat_left, lat_right, lon_left, lon_right


def _is_lat_band_only(datasets: List[Dict[str, Any]]) -> bool:
    """Return True if the data contains only latitude-band bins (no lon_bin)."""
    for ds in datasets:
        for entry in ds.get("per_bins_by_time", []):
            for var_name, bins in entry.get("per_bins", {}).items():
                if bins:
                    sample = bins[0]
                    return "lon_bin" not in sample
    return False


def _is_lat_band_only_from_sets(
    variables: set,
    datasets: List[Dict[str, Any]],
) -> bool:
    """Convenience wrapper used during metadata discovery."""
    return _is_lat_band_only(datasets)


def _iter_grid_data(
    datasets: List[Dict[str, Any]],
    metadata: Dict[str, Any],
):
    """Generator: yield ``(key, grid_info)`` pairs one at a time.

    This avoids accumulating the entire grid dictionary in memory, which
    can be prohibitively large for big datasets.  Callers should consume
    each ``(key, grid_info)`` immediately (e.g. write it to disk or update
    running statistics) rather than collecting all results in a list.

    The key format is the same as before:
    ``"{model}|{ref_prefix}{var_name}|{metric}|{lead_time}[|{depth_label}]"``

    In addition, a composite ``"all"`` lead-time entry is emitted for each
    ``(model, ref_alias)`` by aggregating across **all** lead-times.  This
    gives a much denser spatial coverage for observation data (satellite
    altimetry, Argo profiles) where each individual lead-time only covers
    a few satellite passes.
    """
    all_metrics = metadata["metrics"]
    lat_band_mode = _is_lat_band_only(datasets)

    for ds in datasets:
        model = ds["dataset"]
        # Group entries by (ref_type, lead_time).
        # ref_type ("gridded" / "observation") comes from the per_bins entry
        # and determines both the file key prefix AND the visual rendering style.
        # The actual reference dataset NAMES (glorys, saral ...) live in the
        # regular results files and are mapped to a ref_type via REF_TYPE_MAP
        # in the JS; the map data files are shared by all refs of the same type.
        by_ref_lt: Dict[tuple, List[Dict]] = defaultdict(list)
        for entry in ds["per_bins_by_time"]:
            rt = entry.get("ref_type") or "gridded"
            # Use ref_alias as key so each reference dataset (saral, jason3,
            # swot, glorys, argo_profiles …) gets its own data file.
            # Fall back to ref_type when ref_alias is absent (old format).
            ra = entry.get("ref_alias") or rt
            by_ref_lt[(ra, rt, entry["lead_time"])].append(entry)

        for (ref_alias, ref_type, lt), entries in sorted(by_ref_lt.items()):
            # Key prefix is the ref alias so every dataset writes a distinct file.
            ref_prefix = f"{ref_alias}|"
            if lat_band_mode:
                eff_grid_type = "lat_band_obs" if ref_type == "observation" else "lat_band"
            elif ref_type == "observation":
                eff_grid_type = "points"
            else:
                eff_grid_type = "spatial"
            for var_name in metadata["variables"]:
                has_depth = var_name in metadata["depth_bins"]

                # --- Lat-band mode: accumulate by (lat_south, lat_north[, depth]) ---
                if lat_band_mode:
                    accum_band: Dict[tuple, Dict[str, List[float]]] = defaultdict(
                        lambda: defaultdict(list)
                    )
                    for entry in entries:
                        if var_name not in entry["per_bins"]:
                            continue
                        for b in entry["per_bins"][var_name]:
                            south, north = _parse_bin_value(b["lat_bin"])
                            if has_depth and "depth_bin" in b:
                                db = b["depth_bin"]
                                if isinstance(db, dict):
                                    dk = (db["left"], db["right"])
                                else:
                                    dk = (float(db), float(db))
                                cell_key = (south, north, dk[0], dk[1])
                            else:
                                cell_key = (south, north)
                            for metric in all_metrics:
                                if metric in b and b[metric] is not None:
                                    accum_band[cell_key][metric].append(b[metric])

                    if not accum_band:
                        continue

                    for metric in all_metrics:
                        if has_depth:
                            depth_bands: Dict[Tuple[float, float], List[List[float]]] = defaultdict(list)
                            all_depth_accum_b: Dict[Tuple[float, float], List[float]] = defaultdict(list)
                            for cell_key, mvals in accum_band.items():
                                if metric not in mvals:
                                    continue
                                s, n, dl, dr = cell_key
                                val = _mean(mvals[metric])
                                if math.isnan(val):
                                    continue
                                depth_bands[(dl, dr)].append([s, n, round(val, 6)])
                                all_depth_accum_b[(s, n)].append(val)

                            for (dl, dr), band_data in depth_bands.items():
                                depth_label = f"{dl:.1f}-{dr:.1f}"
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}|{depth_label}"
                                values = [r[2] for r in band_data]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(band_data, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }

                            avg_bands: List[List[float]] = []
                            for (s, n), vals in all_depth_accum_b.items():
                                avg_val = _mean(vals)
                                if not math.isnan(avg_val):
                                    avg_bands.append([s, n, round(avg_val, 6)])
                            if avg_bands:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}|all_depths"
                                values = [r[2] for r in avg_bands]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(avg_bands, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                        else:
                            band_data_list: List[List[float]] = []
                            for cell_key, mvals in accum_band.items():
                                if metric not in mvals:
                                    continue
                                s, n = cell_key
                                val = _mean(mvals[metric])
                                if not math.isnan(val):
                                    band_data_list.append([s, n, round(val, 6)])
                            if band_data_list:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}"
                                values = [r[2] for r in band_data_list]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(band_data_list, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                else:
                    # --- Spatial mode (lat/lon grid) ---
                    # Cell key: (lat_left, lat_right, lon_left, lon_right[, depth_l, depth_r])
                    # Output:   [lat_s, lat_n, lon_w, lon_e, value]
                    accum: Dict[tuple, Dict[str, List[float]]] = defaultdict(
                        lambda: defaultdict(list)
                    )
                    for entry in entries:
                        if var_name not in entry["per_bins"]:
                            continue
                        for b in entry["per_bins"][var_name]:
                            lat_l, lat_r, lon_l, lon_r = _extract_lat_lon_bounds(b)
                            if has_depth and "depth_bin" in b:
                                db = b["depth_bin"]
                                if isinstance(db, dict):
                                    depth_key = (db["left"], db["right"])
                                else:
                                    dl, dr = _parse_bin_value(db)
                                    depth_key = (dl, dr)
                                cell_key = (lat_l, lat_r, lon_l, lon_r, depth_key[0], depth_key[1])
                            else:
                                cell_key = (lat_l, lat_r, lon_l, lon_r)
                            for metric in all_metrics:
                                if metric in b and b[metric] is not None:
                                    accum[cell_key][metric].append(b[metric])

                    if not accum:
                        continue

                    for metric in all_metrics:
                        if has_depth:
                            depth_grids: Dict[Tuple[float, float], List[List[float]]] = defaultdict(list)
                            all_depth_accum: Dict[Tuple[float, float, float, float], List[float]] = defaultdict(list)
                            for cell_key, metric_vals in accum.items():
                                if metric not in metric_vals:
                                    continue
                                lat_l, lat_r, lon_l, lon_r, dl, dr = cell_key
                                val = _mean(metric_vals[metric])
                                if math.isnan(val):
                                    continue
                                depth_grids[(dl, dr)].append([lat_l, lat_r, lon_l, lon_r, round(val, 6)])
                                all_depth_accum[(lat_l, lat_r, lon_l, lon_r)].append(val)

                            for (dl, dr), raw_data in depth_grids.items():
                                depth_label = f"{dl:.1f}-{dr:.1f}"
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}|{depth_label}"
                                values = [row[4] for row in raw_data]
                                if eff_grid_type == "points":
                                    out_data = [
                                        [(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]]
                                        for r in raw_data
                                    ]
                                else:
                                    out_data = raw_data
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_data,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }

                            avg_grid: List[List[float]] = []
                            for (lat_l, lat_r, lon_l, lon_r), vals in all_depth_accum.items():
                                avg_val = _mean(vals)
                                if not math.isnan(avg_val):
                                    avg_grid.append([lat_l, lat_r, lon_l, lon_r, round(avg_val, 6)])
                            if avg_grid:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}|all_depths"
                                values = [row[4] for row in avg_grid]
                                if eff_grid_type == "points":
                                    out_avg = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in avg_grid]
                                else:
                                    out_avg = avg_grid
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_avg,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                        else:
                            grid_data: List[List[float]] = []
                            for cell_key, metric_vals in accum.items():
                                if metric not in metric_vals:
                                    continue
                                lat_l, lat_r, lon_l, lon_r = cell_key
                                val = _mean(metric_vals[metric])
                                if not math.isnan(val):
                                    grid_data.append([lat_l, lat_r, lon_l, lon_r, round(val, 6)])
                            if grid_data:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|{lt}"
                                values = [row[4] for row in grid_data]
                                if eff_grid_type == "points":
                                    out_pts = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in grid_data]
                                else:
                                    out_pts = grid_data
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_pts,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }

        # -----------------------------------------------------------------
        # Composite "all" lead-time: aggregate across ALL lead-times for
        # each (ref_alias, ref_type) to give denser spatial coverage,
        # especially for sparse observation data (satellite / Argo).
        # -----------------------------------------------------------------
        by_ref: Dict[tuple, List[Dict]] = defaultdict(list)
        for (ra, rt, _lt), entries in by_ref_lt.items():
            by_ref[(ra, rt)].extend(entries)

        for (ref_alias, ref_type), entries in sorted(by_ref.items()):
            ref_prefix = f"{ref_alias}|"
            if lat_band_mode:
                eff_grid_type = "lat_band_obs" if ref_type == "observation" else "lat_band"
            elif ref_type == "observation":
                eff_grid_type = "points"
            else:
                eff_grid_type = "spatial"

            for var_name in metadata["variables"]:
                has_depth = var_name in metadata["depth_bins"]

                if lat_band_mode:
                    accum_band_all: Dict[tuple, Dict[str, List[float]]] = defaultdict(
                        lambda: defaultdict(list)
                    )
                    for entry in entries:
                        if var_name not in entry["per_bins"]:
                            continue
                        for b in entry["per_bins"][var_name]:
                            south, north = _parse_bin_value(b["lat_bin"])
                            if has_depth and "depth_bin" in b:
                                db = b["depth_bin"]
                                if isinstance(db, dict):
                                    dk = (db["left"], db["right"])
                                else:
                                    dk = (float(db), float(db))
                                cell_key = (south, north, dk[0], dk[1])
                            else:
                                cell_key = (south, north)
                            for metric in all_metrics:
                                if metric in b and b[metric] is not None:
                                    accum_band_all[cell_key][metric].append(b[metric])

                    if not accum_band_all:
                        continue

                    for metric in all_metrics:
                        if has_depth:
                            depth_bands_a: Dict[Tuple[float, float], List[List[float]]] = defaultdict(list)
                            all_depth_accum_ba: Dict[Tuple[float, float], List[float]] = defaultdict(list)
                            for cell_key, mvals in accum_band_all.items():
                                if metric not in mvals:
                                    continue
                                s, n, dl, dr = cell_key
                                val = _mean(mvals[metric])
                                if math.isnan(val):
                                    continue
                                depth_bands_a[(dl, dr)].append([s, n, round(val, 6)])
                                all_depth_accum_ba[(s, n)].append(val)

                            for (dl, dr), band_data in depth_bands_a.items():
                                depth_label = f"{dl:.1f}-{dr:.1f}"
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all|{depth_label}"
                                values = [r[2] for r in band_data]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(band_data, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }

                            avg_bands_a: List[List[float]] = []
                            for (s, n), vals in all_depth_accum_ba.items():
                                avg_val = _mean(vals)
                                if not math.isnan(avg_val):
                                    avg_bands_a.append([s, n, round(avg_val, 6)])
                            if avg_bands_a:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all|all_depths"
                                values = [r[2] for r in avg_bands_a]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(avg_bands_a, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                        else:
                            band_data_list_a: List[List[float]] = []
                            for cell_key, mvals in accum_band_all.items():
                                if metric not in mvals:
                                    continue
                                s, n = cell_key
                                val = _mean(mvals[metric])
                                if not math.isnan(val):
                                    band_data_list_a.append([s, n, round(val, 6)])
                            if band_data_list_a:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all"
                                values = [r[2] for r in band_data_list_a]
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": sorted(band_data_list_a, key=lambda r: r[0]),
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                else:
                    # --- Spatial composite across all lead-times ---
                    accum_all: Dict[tuple, Dict[str, List[float]]] = defaultdict(
                        lambda: defaultdict(list)
                    )
                    for entry in entries:
                        if var_name not in entry["per_bins"]:
                            continue
                        for b in entry["per_bins"][var_name]:
                            lat_l, lat_r, lon_l, lon_r = _extract_lat_lon_bounds(b)
                            if has_depth and "depth_bin" in b:
                                db = b["depth_bin"]
                                if isinstance(db, dict):
                                    depth_key = (db["left"], db["right"])
                                else:
                                    dl, dr = _parse_bin_value(db)
                                    depth_key = (dl, dr)
                                cell_key = (lat_l, lat_r, lon_l, lon_r, depth_key[0], depth_key[1])
                            else:
                                cell_key = (lat_l, lat_r, lon_l, lon_r)
                            for metric in all_metrics:
                                if metric in b and b[metric] is not None:
                                    accum_all[cell_key][metric].append(b[metric])

                    if not accum_all:
                        continue

                    for metric in all_metrics:
                        if has_depth:
                            depth_grids_a: Dict[Tuple[float, float], List[List[float]]] = defaultdict(list)
                            all_depth_accum_a: Dict[Tuple[float, float, float, float], List[float]] = defaultdict(list)
                            for cell_key, metric_vals in accum_all.items():
                                if metric not in metric_vals:
                                    continue
                                lat_l, lat_r, lon_l, lon_r, dl, dr = cell_key
                                val = _mean(metric_vals[metric])
                                if math.isnan(val):
                                    continue
                                depth_grids_a[(dl, dr)].append([lat_l, lat_r, lon_l, lon_r, round(val, 6)])
                                all_depth_accum_a[(lat_l, lat_r, lon_l, lon_r)].append(val)

                            for (dl, dr), raw_data in depth_grids_a.items():
                                depth_label = f"{dl:.1f}-{dr:.1f}"
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all|{depth_label}"
                                values = [row[4] for row in raw_data]
                                if eff_grid_type == "points":
                                    out_data = [
                                        [(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]]
                                        for r in raw_data
                                    ]
                                else:
                                    out_data = raw_data
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_data,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }

                            avg_grid_a: List[List[float]] = []
                            for (lat_l, lat_r, lon_l, lon_r), vals in all_depth_accum_a.items():
                                avg_val = _mean(vals)
                                if not math.isnan(avg_val):
                                    avg_grid_a.append([lat_l, lat_r, lon_l, lon_r, round(avg_val, 6)])
                            if avg_grid_a:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all|all_depths"
                                values = [row[4] for row in avg_grid_a]
                                if eff_grid_type == "points":
                                    out_avg_a = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in avg_grid_a]
                                else:
                                    out_avg_a = avg_grid_a
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_avg_a,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }
                        else:
                            grid_data_a: List[List[float]] = []
                            for cell_key, metric_vals in accum_all.items():
                                if metric not in metric_vals:
                                    continue
                                lat_l, lat_r, lon_l, lon_r = cell_key
                                val = _mean(metric_vals[metric])
                                if not math.isnan(val):
                                    grid_data_a.append([lat_l, lat_r, lon_l, lon_r, round(val, 6)])
                            if grid_data_a:
                                key = f"{model}|{ref_prefix}{var_name}|{metric}|all"
                                values = [row[4] for row in grid_data_a]
                                if eff_grid_type == "points":
                                    out_pts_a = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in grid_data_a]
                                else:
                                    out_pts_a = grid_data_a
                                yield key, {
                                    "grid_type": eff_grid_type,
                                    "data": out_pts_a,
                                    "vmin": round(min(values), 6),
                                    "vmax": round(max(values), 6),
                                }


def _compute_color_scale_stats(
    datasets: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> Tuple[Dict[tuple, float], Dict[tuple, float]]:
    """First pass: compute global (vmin, vmax) per scale group without storing grid data.

    Returns two dicts: ``group_min`` and ``group_max`` keyed by
    ``(ref_alias, var_name, metric, depth_label)`` – the same grouping used by
    :func:`_apply_global_color_scales`.

    This is O(n_combinations) memory instead of O(n_cells × n_combinations).
    """
    group_min: Dict[tuple, float] = {}
    group_max: Dict[tuple, float] = {}

    for key, grid_info in _iter_grid_data(datasets, metadata):
        parts = key.split("|")
        if len(parts) < 5:
            continue
        depth_label = parts[5] if len(parts) >= 6 else ""
        group = (parts[1], parts[2], parts[3], depth_label)

        vmin = grid_info.get("vmin")
        vmax = grid_info.get("vmax")
        if vmin is None or vmax is None:
            continue

        if group not in group_min:
            group_min[group] = vmin
            group_max[group] = vmax
        else:
            group_min[group] = min(group_min[group], vmin)
            group_max[group] = max(group_max[group], vmax)

    return group_min, group_max


def aggregate_grid_data(
    datasets: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Aggregate per-bins data into grids for each combination.

    .. deprecated::
        Kept for backward compatibility only.  For large datasets use
        :func:`preprocess_per_bins` which streams results to disk without
        accumulating the entire grid dict in memory.
    """
    return dict(_iter_grid_data(datasets, metadata))


def _apply_global_color_scales(grids: Dict[str, Dict[str, Any]]) -> None:
    """Normalise vmin/vmax so that every grid sharing the same
    (ref_alias, variable, metric[, depth_label]) uses identical color
    scale limits across all models and lead times.

    Key format (pipe-separated):
        ``{model}|{ref_alias}|{var_name}|{metric}|{lead_time}[|{depth_label}]``

    .. note::
        Only used when ``grids`` is already in memory (small datasets or
        backward-compatibility path).  The streaming path in
        :func:`preprocess_per_bins` uses :func:`_compute_color_scale_stats`
        instead.
    """
    # 1. Collect global (vmin, vmax) per scale group.
    group_min: Dict[tuple, float] = {}
    group_max: Dict[tuple, float] = {}

    for key, grid_info in grids.items():
        parts = key.split("|")
        # parts: [model, ref_alias, var_name, metric, lead_time (, depth_label)]
        if len(parts) < 5:
            continue
        # Group key: everything that should share the same colour scale
        # (ref_alias, var_name, metric) – depth_label when present.
        depth_label = parts[5] if len(parts) >= 6 else ""
        group = (parts[1], parts[2], parts[3], depth_label)

        vmin = grid_info.get("vmin")
        vmax = grid_info.get("vmax")
        if vmin is None or vmax is None:
            continue

        if group not in group_min:
            group_min[group] = vmin
            group_max[group] = vmax
        else:
            group_min[group] = min(group_min[group], vmin)
            group_max[group] = max(group_max[group], vmax)

    # 2. Apply global limits back to every grid entry.
    for key, grid_info in grids.items():
        parts = key.split("|")
        if len(parts) < 5:
            continue
        depth_label = parts[5] if len(parts) >= 6 else ""
        group = (parts[1], parts[2], parts[3], depth_label)
        if group in group_min:
            grid_info["vmin"] = group_min[group]
            grid_info["vmax"] = group_max[group]


def write_map_data(
    grids: Dict[str, Dict[str, Any]],
    metadata: Dict[str, Any],
    output_dir: Path,
) -> None:
    """Write pre-aggregated grid data as JSONP .js files to output_dir/map_data/.

    Each file is a small JavaScript snippet that calls
    ``_mapDataCallback({...})`` so it can be loaded via a dynamic
    ``<script>`` tag.  This is the only technique that works reliably
    with the ``file://`` protocol in all modern browsers (both
    ``fetch()`` and ``XMLHttpRequest`` are blocked by CORS for local
    files).

    .. note::
        This function accepts either a plain ``dict`` or any iterable of
        ``(key, grid_info)`` pairs so it can be driven by the streaming
        generator :func:`_iter_grid_data` directly (no full in-memory
        accumulation required).
    """
    map_dir = output_dir / "map_data"
    map_dir.mkdir(parents=True, exist_ok=True)

    # Normalise input: accept both a dict and any (key, grid_info) iterable.
    items = grids.items() if isinstance(grids, dict) else grids

    # Write individual grid files as JSONP .js
    manifest = {}
    for key, grid_info in items:
        # Create safe filename
        safe_name = key.replace("|", "_").replace(" ", "_")
        filename = f"{safe_name}.js"
        filepath = map_dir / filename

        payload = json.dumps(grid_info, separators=(",", ":"))
        with open(filepath, "w") as f:
            f.write(f"_mapDataCallback({payload});\n")

        manifest[key] = filename

    # Write manifest + metadata
    meta_file = map_dir / "manifest.json"
    with open(meta_file, "w") as f:
        json.dump(
            {"metadata": metadata, "files": manifest},
            f,
            indent=2,
            default=str,
        )

    logger.debug("Wrote {} grid files + manifest to {}", len(manifest), map_dir)


def preprocess_per_bins(results_dir: Path, output_dir: Path) -> Optional[Dict[str, Any]]:
    """
    Main entry point: load per-bins files, aggregate, write output.

    Returns metadata dict if per-bins files were found, None otherwise.
    """
    datasets = load_per_bins_files(results_dir)
    if not datasets:
        logger.info("No *_per_bins.json/.jsonl files found, skipping map page.")
        return None

    logger.debug("Discovering metadata...")
    metadata = discover_metadata(datasets, results_dir=results_dir)
    logger.debug("  Models: {}", metadata['models'])
    logger.debug("  Variables: {}", metadata['variables'])
    logger.debug("  Metrics: {}", metadata['metrics'])
    logger.debug("  Lead times: {}", metadata['lead_times'])
    if metadata["depth_bins"]:
        for var, dbs in metadata["depth_bins"].items():
            logger.debug("  Depth bins for {}: {} levels", var, len(dbs))
    if metadata["ref_variables"]:
        logger.debug("  Reference datasets: {}", list(metadata['ref_variables'].keys()))

    logger.debug("Computing global colour scales (pass 1/2 - lightweight)...")
    group_min, group_max = _compute_color_scale_stats(datasets, metadata)
    logger.debug("  Found {} colour-scale groups", len(group_min))

    def _streaming_grids_with_global_scales():
        """Generate (key, grid_info) pairs with global vmin/vmax applied on the fly."""
        for key, grid_info in _iter_grid_data(datasets, metadata):
            parts = key.split("|")
            if len(parts) >= 5:
                depth_label = parts[5] if len(parts) >= 6 else ""
                group = (parts[1], parts[2], parts[3], depth_label)
                if group in group_min:
                    grid_info["vmin"] = group_min[group]
                    grid_info["vmax"] = group_max[group]
            yield key, grid_info

    logger.debug("Writing grid files (pass 2/2 - streaming to disk)...")
    write_map_data(_streaming_grids_with_global_scales(), metadata, output_dir)
    return metadata
