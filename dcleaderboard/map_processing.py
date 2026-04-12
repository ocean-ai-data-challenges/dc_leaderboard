"""
Pre-processing module for per-bins results data.

Reads *_per_bins.json files, aggregates spatial data by
(model, variable, metric, lead_time), and writes compact JSON
files for the interactive map page.
"""
from __future__ import annotations

import gzip
import json
import math
import re
from array import array as _array
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Tuple
from loguru import logger

try:
    import numpy as _np
    _HAS_NUMPY = True
except ImportError:  # pragma: no cover
    _HAS_NUMPY = False

try:
    import pandas as _pd
    _HAS_PANDAS = True
except ImportError:  # pragma: no cover
    _HAS_PANDAS = False

try:
    import orjson as _orjson
    def _json_loads(s: Any) -> Any:
        return _orjson.loads(s)
except ImportError:
    def _json_loads(s: Any) -> Any:  # type: ignore[misc]
        return json.loads(s)

try:
    from rich import progress as _rich_progress
    _HAS_RICH = True
except ImportError:  # pragma: no cover
    _HAS_RICH = False

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


class _V2BinView:
    """Lazy per-variable bin iterator for v2 columnar format.

    Wraps the columnar arrays ``(yl, yr, xl, xr[, dl, dr], metric…)`` from a
    single v2 per-bins entry for one variable.  Iterating over it yields one
    temporary ``dict`` per spatial bin — no list materialisation.

    Columns are stored as compact ``array.array('f')`` (float32) to reduce
    memory by ~7× compared to Python lists of floats.

    Also supports ``__len__`` (for ``discover_metadata``) and ``__getitem__``
    index-0 to peek at a sample bin without iterating everything.
    """

    # Column names that map to special bin keys.
    _Y_COLS = ("yl", "yr")   # lat_bin left/right
    _X_COLS = ("xl", "xr")   # lon_bin left/right
    _D_COLS = ("dl", "dr")   # depth_bin left/right (optional)

    __slots__ = ("_cols",)

    def __init__(self, cols: Dict[str, Any]) -> None:
        # Convert lists to compact float32 arrays.  This reduces memory from
        # ~28 bytes/element (Python float in list) to 4 bytes/element.
        compact: Dict[str, Any] = {}
        for k, v in cols.items():
            if isinstance(v, list):
                compact[k] = _array("f", v)
            else:
                compact[k] = v
        self._cols = compact

    def __len__(self) -> int:
        yl = self._cols.get("yl")
        return len(yl) if yl is not None else 0

    def __bool__(self) -> bool:
        return len(self) > 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        cols = self._cols
        yl = cols.get("yl", ())
        yr = cols.get("yr", ())
        xl = cols.get("xl")
        xr = cols.get("xr")
        dl = cols.get("dl")
        dr = cols.get("dr")
        # Collect metric columns (everything that is not a spatial/depth col).
        _spatial = {"yl", "yr", "xl", "xr", "dl", "dr"}
        metric_cols = [(k, v) for k, v in cols.items() if k not in _spatial]
        has_lon = xl is not None and xr is not None
        has_depth = dl is not None and dr is not None

        for i in range(len(yl)):
            b: Dict[str, Any] = {
                "lat_bin": {"left": yl[i], "right": yr[i]},
            }
            if has_lon:
                b["lon_bin"] = {"left": xl[i], "right": xr[i]}
            if has_depth:
                b["depth_bin"] = {"left": dl[i], "right": dr[i]}
            for k, v in metric_cols:
                b[k] = v[i]
            yield b

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        """Return a single bin dict at *idx* (supports index 0 for sample peek)."""
        cols = self._cols
        yl = cols.get("yl", ())
        yr = cols.get("yr", ())
        xl = cols.get("xl")
        xr = cols.get("xr")
        dl = cols.get("dl")
        dr = cols.get("dr")
        _spatial = {"yl", "yr", "xl", "xr", "dl", "dr"}
        b: Dict[str, Any] = {
            "lat_bin": {"left": yl[idx], "right": yr[idx]},
        }
        if xl is not None:
            b["lon_bin"] = {"left": xl[idx], "right": xr[idx]}
        if dl is not None:
            b["depth_bin"] = {"left": dl[idx], "right": dr[idx]}
        for k, v in cols.items():
            if k not in _spatial:
                b[k] = v[idx]
        return b


def _decode_v2_line(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Decode a single v2 JSONL data line into the legacy entry format.

    The v2 format uses short-coded top-level keys and stores per-variable
    data in columnar arrays.  This function returns an entry dict whose
    ``per_bins`` values are :class:`_V2BinView` iterables (not materialised
    lists), so downstream code can iterate over bins without RAM overhead.
    """
    per_bins_v2: Dict[str, Any] = raw.get("pb", {})
    per_bins: Dict[str, Any] = {
        var: _V2BinView(cols) for var, cols in per_bins_v2.items()
    }
    return {
        "ref_alias": raw.get("ra"),
        "ref_type": raw.get("rt"),
        "lead_time": raw.get("lt"),
        "forecast_reference_time": raw.get("ft"),
        "per_bins": per_bins,
    }


class _PerBinsStream:
    """Lazy iterable over per-bins entries from a ``.jsonl[.gz]`` file.

    Each iteration re-opens the file and yields decoded entry dicts one at a
    time.  The file is never fully materialised in memory, so this class can
    safely be iterated 3+ times (once per pass in the pipeline) at the cost of
    one sequential gzip read per pass (~83 MB → very fast).

    Also supports ``len()`` (counts entries) and ``bool()`` (non-empty check).
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._is_gz = path.suffix == ".gz"
        self._v2: bool = False
        # Count entries and detect v2 in a single quick pass.
        self._n_entries = 0
        self._v2 = self._probe()

    def _open(self):
        if self._is_gz:
            return gzip.open(self._path, "rt", encoding="utf-8")
        return open(self._path, encoding="utf-8")

    def _probe(self) -> bool:
        """Return True if the file is v2 format (first line has ``_v: 2``).
        Also count entries stored in the file."""
        v2 = False
        count = 0
        with self._open() as fh:
            for i, line in enumerate(fh):
                raw = line.strip()
                if not raw:
                    continue
                if i == 0:
                    obj = _json_loads(raw)
                    if isinstance(obj, dict) and obj.get("_v") == 2:
                        v2 = True
                        # First line is header, not data
                        continue
                count += 1
        self._n_entries = count
        return v2

    def __len__(self) -> int:
        return self._n_entries

    def __bool__(self) -> bool:
        return self._n_entries > 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        with self._open() as fh:
            for i, line in enumerate(fh):
                raw = line.strip()
                if not raw:
                    continue
                if i == 0 and self._v2:
                    # Skip v2 header line
                    continue
                obj = _json_loads(raw)
                if self._v2:
                    yield _decode_v2_line(obj)
                else:
                    yield obj


# ---------------------------------------------------------------------------
# Numpy folder cache  ("_per_bins_cache/" directory with .npy files)
# ---------------------------------------------------------------------------

class _NpzBinStore:
    """Columnar numpy cache for per-bins data.

    First use converts the source ``.jsonl[.gz]`` stream into a directory of
    ``.npy`` files stored next to the source (one file per column per
    variable).  Subsequent calls load only the columns needed, keeping peak
    RAM around the size of a single variable's data (~50–100 MB).

    Directory layout: ``{stem}_per_bins_cache/``
      meta.npz          – entry-level metadata arrays (ra, rt, lt, frt)
      {var}__{col}.npy  – one float32 per-bin array per (variable, column)
      {var}__ei.npy     – int16 entry-index per bin row

    The cache is rebuilt automatically whenever the source file is newer.
    """

    _SPATIAL = frozenset({"yl", "yr", "xl", "xr", "dl", "dr"})

    def __init__(self, path: Path, stream: "_PerBinsStream") -> None:
        stem = path.name
        for ext in (".jsonl.gz", ".jsonl"):
            if stem.endswith(ext):
                stem = stem[: -len(ext)]
                break
        # Place cache in a dedicated subdirectory so that results/ stays clean.
        cache_base = path.parent / "_npy_cache"
        self._cache_dir = cache_base / (stem + "_per_bins_cache")
        self._meta_path = self._cache_dir / "meta.npz"
        self._stream = stream
        self._source_path = path
        self._meta: Optional[Any] = None  # lazy np.load of meta.npz

    # ------------------------------------------------------------------
    def _needs_rebuild(self) -> bool:
        if not self._cache_dir.exists() or not self._meta_path.exists():
            return True
        src_mtime = self._source_path.stat().st_mtime
        cache_mtime = self._meta_path.stat().st_mtime
        return src_mtime > cache_mtime

    # ------------------------------------------------------------------
    def _build_cache(self) -> None:
        """Scan the stream once per variable and write compact .npy files.

        Memory profile: at most one variable's full data lives in RAM at a
        time (~50–100 MB depending on bin count).
        """
        import numpy as _np

        self._cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("Building numpy cache {} …", self._cache_dir.name)

        # ---- pass 0: entry metadata + variable/column discovery -----------
        meta_ra:  List[str] = []
        meta_rt:  List[str] = []
        meta_lt:  List[int] = []
        meta_frt: List[str] = []
        all_vars: List[str] = []
        # var → set of all column names seen across every entry
        var_all_cols: Dict[str, set] = {}

        for entry in self._stream:
            ra = entry.get("ref_alias") or entry.get("ref_type") or "unknown"
            rt = entry.get("ref_type") or "gridded"
            lt = int(entry["lead_time"])
            frt = str(entry.get("forecast_reference_time", ""))[:10]
            meta_ra.append(ra)
            meta_rt.append(rt)
            meta_lt.append(lt)
            meta_frt.append(frt)
            for var, bins in entry["per_bins"].items():
                if var not in all_vars:
                    all_vars.append(var)
                if bins:
                    var_all_cols.setdefault(var, set()).update(bins._cols.keys())

        _np.savez_compressed(
            str(self._meta_path),
            ra=_np.array(meta_ra),
            rt=_np.array(meta_rt),
            lt=_np.array(meta_lt, dtype=_np.int16),
            frt=_np.array(meta_frt),
            variables=_np.array(all_vars),
        )

        # ---- pass 1…N: one variable at a time -------------------------
        for var in all_vars:
            all_cols = var_all_cols.get(var, set())
            # Collect all columns for this variable across all entries.
            # Use bytearray buffers to avoid Python list overhead.
            col_bufs: Dict[str, bytearray] = {c: bytearray() for c in all_cols}
            ei_buf = bytearray()

            for ei, entry in enumerate(self._stream):
                bins = entry["per_bins"].get(var)
                if not bins:
                    continue
                cols = bins._cols
                n = len(bins)
                ei_bytes = _np.full(n, ei, dtype=_np.int16).tobytes()
                ei_buf.extend(ei_bytes)

                for col in all_cols:
                    if col in cols:
                        arr = cols[col]
                        if isinstance(arr, _array):
                            data = _np.frombuffer(arr, dtype=_np.float32).tobytes()
                        else:
                            data = _np.array(arr, dtype=_np.float32).tobytes()
                    else:
                        # Fill missing metric column with NaN for this entry
                        data = _np.full(n, _np.nan, dtype=_np.float32).tobytes()
                    col_bufs[col].extend(data)

            # Write
            n_total = len(ei_buf) // 2  # int16 = 2 bytes
            ei_arr = _np.frombuffer(ei_buf, dtype=_np.int16)
            _np.save(str(self._cache_dir / f"{var}__ei.npy"), ei_arr)

            for col, buf in col_bufs.items():
                arr = _np.frombuffer(bytes(buf), dtype=_np.float32)
                _np.save(str(self._cache_dir / f"{var}__{col}.npy"), arr)

            logger.debug(
                "  {} : {} bins, cols={}",
                var, n_total, sorted(col_bufs.keys()),
            )
            del col_bufs, ei_buf, ei_arr

        logger.debug(
            "  Numpy cache built in {}", self._cache_dir.name,
        )

    # ------------------------------------------------------------------
    def _ensure(self) -> None:
        if self._needs_rebuild():
            self._build_cache()
        if self._meta is None:
            import numpy as _np
            self._meta = _np.load(str(self._meta_path), allow_pickle=True)

    # ------------------------------------------------------------------
    @property
    def meta_ra(self):
        self._ensure(); return self._meta["ra"]
    @property
    def meta_rt(self):
        self._ensure(); return self._meta["rt"]
    @property
    def meta_lt(self):
        self._ensure(); return self._meta["lt"]
    @property
    def meta_frt(self):
        self._ensure(); return self._meta["frt"]

    def variables(self) -> List[str]:
        self._ensure()
        return list(self._meta["variables"])

    def var_array(self, var: str, col: str):
        import numpy as _np
        self._ensure()
        return _np.load(str(self._cache_dir / f"{var}__{col}.npy"), mmap_mode="r")

    def has_col(self, var: str, col: str) -> bool:
        self._ensure()
        return (self._cache_dir / f"{var}__{col}.npy").exists()

    def metric_cols(self, var: str) -> List[str]:
        self._ensure()
        skip = self._SPATIAL | {"ei"}
        return [
            p.stem.split("__", 1)[1]
            for p in sorted(self._cache_dir.glob(f"{var}__*.npy"))
            if p.stem.split("__", 1)[1] not in skip
        ]

    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return int(len(self.meta_lt))

    def __bool__(self) -> bool:
        return len(self) > 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Yield one legacy-format entry dict per index.

        Only the first bin per variable is materialised so that
        :func:`discover_metadata` can inspect bin structure cheaply.
        """
        n = len(self)
        for ei in range(n):
            per_bins: Dict[str, Any] = {}
            for var in self.variables():
                ei_arr = self.var_array(var, "ei")
                # np.searchsorted is faster than np.where for sorted arrays,
                # but ei may not be sorted globally; use where.
                idxs = _np.where(ei_arr == ei)[0]
                if len(idxs) == 0:
                    continue
                per_bins[var] = _NpzVarProxy(self, var, idxs)
            yield {
                "ref_alias": str(self.meta_ra[ei]),
                "ref_type":  str(self.meta_rt[ei]),
                "lead_time": int(self.meta_lt[ei]),
                "forecast_reference_time": str(self.meta_frt[ei]),
                "per_bins": per_bins,
            }


class _NpzVarProxy:
    """Thin proxy exposing a subset of cached bin rows as a bin iterable."""
    __slots__ = ("_store", "_var", "_idxs")

    def __init__(self, store: "_NpzBinStore", var: str, idxs) -> None:
        self._store = store
        self._var = var
        self._idxs = idxs

    def __len__(self) -> int:
        return len(self._idxs)

    def __bool__(self) -> bool:
        return len(self._idxs) > 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        store = self._store
        var = self._var
        idxs = self._idxs
        yl = store.var_array(var, "yl")[idxs]
        yr = store.var_array(var, "yr")[idxs]
        has_lon   = store.has_col(var, "xl")
        has_depth = store.has_col(var, "dl")
        xl = store.var_array(var, "xl")[idxs] if has_lon else None
        xr = store.var_array(var, "xr")[idxs] if has_lon else None
        dl = store.var_array(var, "dl")[idxs] if has_depth else None
        dr = store.var_array(var, "dr")[idxs] if has_depth else None
        metrics = store.metric_cols(var)
        m_arrs = [store.var_array(var, m)[idxs] for m in metrics]

        for i in range(len(idxs)):
            b: Dict[str, Any] = {
                "lat_bin": {"left": float(yl[i]), "right": float(yr[i])}
            }
            if has_lon:
                b["lon_bin"] = {"left": float(xl[i]), "right": float(xr[i])}
            if has_depth:
                b["depth_bin"] = {"left": float(dl[i]), "right": float(dr[i])}
            for metric, arr in zip(metrics, m_arrs):
                b[metric] = float(arr[i])
            yield b

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        return next(x for i, x in enumerate(self) if i == idx)



def load_per_bins_files(results_dir: Path) -> List[Dict[str, Any]]:
    """Load all *_per_bins.json/.jsonl/.jsonl.gz files from the results directory.

    Supports three formats:
    - Legacy ``.json``: a single JSON object with a ``per_bins_by_time`` list.
    - Plain ``.jsonl``: one compact JSON object per line; loaded eagerly
      (files are typically small).
    - Compressed ``.jsonl.gz``: may be v1 (plain JSONL lines) or v2 (columnar
      format with header + short-coded keys).  Wrapped in :class:`_PerBinsStream`
      which re-reads the gzip file on each iteration instead of materialising
      the full dataset in RAM.

    In all cases a list of ``{"dataset": name, "per_bins_by_time": iterable}``
    dicts is returned.  The ``per_bins_by_time`` value is either a plain Python
    list (json/.jsonl) or a :class:`_PerBinsStream` (jsonl.gz).
    """
    # Collect all per-bins files; prefer compressed > uncompressed > json.
    gz_files = sorted(results_dir.glob("*_per_bins.jsonl.gz"))
    jsonl_files = sorted(results_dir.glob("*_per_bins.jsonl"))
    json_files = sorted(results_dir.glob("*_per_bins.json"))

    gz_stems = {f.name[: f.name.index(".jsonl.gz")] for f in gz_files}
    jsonl_stems = {f.stem for f in jsonl_files}
    # Drop files superseded by a higher-priority format.
    jsonl_files = [f for f in jsonl_files if f.stem not in gz_stems]
    json_files = [f for f in json_files if f.stem not in gz_stems and f.stem not in jsonl_stems]

    files = gz_files + jsonl_files + json_files

    datasets = []
    for f in files:
        logger.debug("Loading per-bins file: {} ...", f.name)
        if f.name.endswith(".jsonl.gz"):
            stream = _PerBinsStream(f)
            dataset_name = f.name[: f.name.index("_per_bins")]
            if dataset_name.startswith("results_"):
                dataset_name = dataset_name[len("results_"):]
            if _HAS_NUMPY and _HAS_PANDAS:
                store = _NpzBinStore(f, stream)
                per_bins_src: Any = store
                logger.debug(
                    "  {} entries (NPZ cache, v2={}) from {}",
                    len(stream), stream._v2, f.name,
                )
            else:
                per_bins_src = stream
                logger.debug(
                    "  {} entries (lazy stream, v2={}) from {}",
                    len(stream), stream._v2, f.name,
                )
            data: Dict[str, Any] = {
                "dataset": dataset_name,
                "per_bins_by_time": per_bins_src,
            }
        elif f.suffix == ".jsonl":
            entries: List[Dict[str, Any]] = []
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        entries.append(_json_loads(stripped))
            dataset_name = f.stem[: f.stem.index("_per_bins")]
            if dataset_name.startswith("results_"):
                dataset_name = dataset_name[len("results_"):]
            data = {
                "dataset": dataset_name,
                "per_bins_by_time": entries,
            }
            logger.debug("  {} entries loaded from {}", len(entries), f.name)
        else:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            logger.debug(
                "  {} entries loaded from {}",
                len(data.get("per_bins_by_time", [])), f.name,
            )
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
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Discover available models, variables, metrics, lead times, depth bins."""
    models = []
    variables: set = set()
    metrics: set = set()
    lead_times: set = set()
    depth_bins_by_var: Dict[str, set] = defaultdict(set)
    frt_per_ref: Dict[str, set] = defaultdict(set)

    skip_keys = {"time_bin", "lat_bin", "lon_bin", "depth_bin", "count", "n_points"}

    for ds in datasets:
        model = ds["dataset"]
        models.append(model)
        for entry in ds["per_bins_by_time"]:
            lead_times.add(entry["lead_time"])
            ra = entry.get("ref_alias") or entry.get("ref_type") or "unknown"
            frt_raw = entry.get("forecast_reference_time", "")
            if frt_raw:
                frt_per_ref[ra].add(frt_raw[:10])
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

    # When the pipeline provides an explicit allowed_metrics list
    # (derived from the sources' metrics in the project YAML), use it as a
    # whitelist for the map dropdown.  metrics_names is NOT used as a filter.
    if config:
        _allowed = config.get("allowed_metrics")
        if _allowed:
            _filtered_metrics = metrics & set(_allowed)
            if _filtered_metrics:
                metrics = _filtered_metrics

    return {
        "models": sorted(models),
        "variables": sorted(variables),
        "metrics": sorted(metrics),
        "lead_times": all_lead_times,
        "depth_bins": sorted_depths,
        "ref_variables": ref_variables,
        "ref_type_map": ref_type_map,
        "forecast_reference_times": {
            ra: sorted(frts) for ra, frts in frt_per_ref.items()
        },
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


def _is_depth_label(s: str) -> bool:
    """Return True if *s* looks like a depth label (e.g. ``"50.0-200.0"`` or ``"all_depths"``)."""
    return s == "all_depths" or ("." in s and len(s) > 0 and s[0].isdigit())


def _extract_depth_from_key(key: str) -> str:
    """Extract the depth label from a grid key, handling variable-length formats.

    Keys may have 5–7 pipe-separated parts depending on whether a
    forecast-reference-time and/or depth segment is present.
    """
    parts = key.split("|")
    for i in range(5, len(parts)):
        if _is_depth_label(parts[i]):
            return parts[i]
    return ""


def _yield_grids_from_entries(
    entries: List[Dict],
    model: str,
    ref_prefix: str,
    eff_grid_type: str,
    key_suffix: str,
    metadata: Dict[str, Any],
    lat_band_mode: bool,
):
    """Yield ``(key, grid_info)`` pairs for a batch of per_bins entries.

    Parameters
    ----------
    entries : list[dict]
        Per-bins entries to aggregate (may be 1 for per-FRT, or many for
        aggregated views).
    model : str
        Model name (first key segment).
    ref_prefix : str
        Reference-alias prefix including trailing pipe, e.g. ``"saral|"``.
    eff_grid_type : str
        One of ``"spatial"``, ``"points"``, ``"lat_band"``, ``"lat_band_obs"``.
    key_suffix : str
        Appended after the metric in the key.  Examples:
        ``"|0"`` (lead-time 0), ``"|all"`` (all-lt composite),
        ``"|0|2024-01-03"`` (per-FRT for lt 0, date 2024-01-03).
    metadata : dict
        Metadata from :func:`discover_metadata`.
    lat_band_mode : bool
        Whether the data uses latitude-band bins only (no ``lon_bin``).
    """
    all_metrics = metadata["metrics"]

    for var_name in metadata["variables"]:
        has_depth = var_name in metadata["depth_bins"]

        # --- Lat-band mode ---
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|{depth_label}"
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|all_depths"
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}"
                        values = [r[2] for r in band_data_list]
                        yield key, {
                            "grid_type": eff_grid_type,
                            "data": sorted(band_data_list, key=lambda r: r[0]),
                            "vmin": round(min(values), 6),
                            "vmax": round(max(values), 6),
                        }
        else:
            # --- Spatial mode (lat/lon grid) ---
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|{depth_label}"
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|all_depths"
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
                        key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}"
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


def _iter_grid_data(
    datasets: List[Dict[str, Any]],
    metadata: Dict[str, Any],
):
    """Generator: yield ``(key, grid_info)`` pairs one at a time.

    When the ``per_bins_by_time`` value is a :class:`_NpzBinStore`,
    aggregation is done with **vectorised pandas/numpy** operations and
    completes in seconds rather than minutes.  For plain list / stream
    sources the legacy dict-accumulator path is used as fallback.

    Three aggregation levels are emitted per ``(model, ref_alias)`` pair:

    1. **Per lead-time** key suffix ``|{lt}``
    2. **All-lt composite** key suffix ``|all``
    3. **Per FRT** key suffix ``|{lt}|{frt_date}``
    """
    lat_band_mode = _is_lat_band_only(datasets)

    def _grid_type(ref_type: str) -> str:
        if lat_band_mode:
            return "lat_band_obs" if ref_type == "observation" else "lat_band"
        return "points" if ref_type == "observation" else "spatial"

    for ds in datasets:
        model = ds["dataset"]
        src = ds["per_bins_by_time"]

        if _HAS_NUMPY and _HAS_PANDAS and isinstance(src, _NpzBinStore):
            yield from _iter_grid_data_npz(src, model, metadata, lat_band_mode, _grid_type)
        else:
            yield from _iter_grid_data_legacy(src, model, metadata, lat_band_mode, _grid_type)


# ---------------------------------------------------------------------------
# NPZ vectorised path
# ---------------------------------------------------------------------------

def _iter_grid_data_npz(
    store: "_NpzBinStore",
    model: str,
    metadata: Dict[str, Any],
    lat_band_mode: bool,
    grid_type_fn,
):
    """Vectorised aggregation using pandas groupby on the .npz arrays."""
    import numpy as _np
    import pandas as _pd

    meta_ra  = store.meta_ra   # object array of strings, length = n_entries
    meta_rt  = store.meta_rt
    meta_lt  = store.meta_lt   # int16 array
    meta_frt = store.meta_frt

    # Build integer-coded lookup tables for ra/rt/frt so that per-row arrays
    # are int8/int16 (1-2 bytes) instead of Python object strings (~50 bytes).
    ra_vals  = list(dict.fromkeys(str(s) for s in meta_ra))   # unique, order-preserved
    rt_vals  = list(dict.fromkeys(str(s) for s in meta_rt))
    frt_vals = list(dict.fromkeys(str(s) for s in meta_frt))
    ra_code  = {s: i for i, s in enumerate(ra_vals)}
    rt_code  = {s: i for i, s in enumerate(rt_vals)}
    frt_code = {s: i for i, s in enumerate(frt_vals)}

    # Maps from entry-index → integer code (length = n_entries, dtype int8)
    n_ent = len(meta_ra)
    ei_to_rac  = _np.array([ra_code[str(meta_ra[i])]  for i in range(n_ent)], dtype=_np.int8)
    ei_to_rtc  = _np.array([rt_code[str(meta_rt[i])]  for i in range(n_ent)], dtype=_np.int8)
    ei_to_lt   = _np.array([int(meta_lt[i])             for i in range(n_ent)], dtype=_np.int16)
    ei_to_frtc = _np.array([frt_code[str(meta_frt[i])] for i in range(n_ent)], dtype=_np.int8)

    for var in sorted(store.variables()):
        has_depth = var in metadata["depth_bins"]
        has_lon   = store.has_col(var, "xl")

        # Build full DataFrame for this variable once — use int codes instead
        # of string arrays to keep per-row memory at 1-2 bytes per column.
        ei  = store.var_array(var, "ei").astype(_np.int16)
        yl  = store.var_array(var, "yl")
        yr  = store.var_array(var, "yr")

        cols: Dict[str, Any] = {
            "ei": ei,
            "rac":  ei_to_rac[ei],   # int8  — ref_alias code
            "rtc":  ei_to_rtc[ei],   # int8  — ref_type code
            "lt":   ei_to_lt[ei],    # int16 — lead time
            "frtc": ei_to_frtc[ei],  # int8  — frt code
            "yl": yl,
            "yr": yr,
        }
        if has_lon:
            cols["xl"] = store.var_array(var, "xl")
            cols["xr"] = store.var_array(var, "xr")
        if has_depth:
            cols["dl"] = store.var_array(var, "dl")
            cols["dr"] = store.var_array(var, "dr")

        metrics = store.metric_cols(var)
        for m in metrics:
            cols[m] = store.var_array(var, m)

        df = _pd.DataFrame(cols)
        # Drop rows where ALL metrics are NaN (bins not observed for any metric)
        df = df.dropna(subset=metrics, how="all")

        # Determine group-by columns for spatial cells
        if lat_band_mode:
            cell_cols = ["yl", "yr"]
            if has_depth:
                cell_cols += ["dl", "dr"]
        else:
            if has_lon:
                cell_cols = ["yl", "yr", "xl", "xr"]
            else:
                cell_cols = ["yl", "yr"]
            if has_depth:
                cell_cols += ["dl", "dr"]

        def _emit_agg(df_sub: "_pd.DataFrame", ra: str, rt: str, key_suffix: str):
            """Aggregate df_sub by spatial cell and emit grid entries."""
            if df_sub.empty:
                return
            eff_gt = grid_type_fn(rt)
            ref_prefix = f"{ra}|"

            agg: Dict[str, str] = {m: "mean" for m in metrics}
            grouped = df_sub.groupby(cell_cols, sort=False)[metrics].mean()
            grouped = grouped.reset_index()

            for metric in metrics:
                if metric not in grouped.columns:
                    continue
                m_vals = grouped[metric].to_numpy(dtype=float)
                valid = ~_np.isnan(m_vals)
                if not valid.any():
                    continue

                if lat_band_mode:
                    if has_depth:
                        # emit per depth level
                        dl_arr = grouped["dl"].to_numpy()
                        dr_arr = grouped["dr"].to_numpy()
                        depth_groups: Dict[tuple, list] = {}
                        for row in zip(grouped["yl"].to_numpy(),
                                       grouped["yr"].to_numpy(),
                                       dl_arr, dr_arr, m_vals):
                            s, n, dl_, dr_, v = row
                            if _np.isnan(v): continue
                            depth_groups.setdefault((dl_, dr_), []).append([float(s), float(n), round(float(v), 6)])
                        all_depth: Dict[tuple, list] = {}
                        for (dl_, dr_), bdata in depth_groups.items():
                            depth_label = f"{dl_:.1f}-{dr_:.1f}"
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}|{depth_label}"
                            vals = [r[2] for r in bdata]
                            yield key, {"grid_type": eff_gt, "data": sorted(bdata, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                            for r in bdata:
                                all_depth.setdefault((r[0], r[1]), []).append(r[2])
                        avg = [[s, n, round(sum(vs)/len(vs), 6)] for (s, n), vs in all_depth.items()]
                        if avg:
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}|all_depths"
                            vals = [r[2] for r in avg]
                            yield key, {"grid_type": eff_gt, "data": sorted(avg, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                    else:
                        band = [[float(r["yl"]), float(r["yr"]), round(float(r[metric]), 6)]
                                for _, r in grouped[valid].iterrows()]
                        if band:
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}"
                            vals = [r[2] for r in band]
                            yield key, {"grid_type": eff_gt, "data": sorted(band, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                else:
                    xl_col = "xl" if has_lon else None
                    xr_col = "xr" if has_lon else None
                    if has_depth:
                        dl_arr = grouped["dl"].to_numpy()
                        dr_arr = grouped["dr"].to_numpy()
                        depth_grids: Dict[tuple, list] = {}
                        all_depth2: Dict[tuple, list] = {}
                        for row in zip(grouped["yl"].to_numpy(),
                                       grouped["yr"].to_numpy(),
                                       grouped["xl"].to_numpy() if has_lon else _np.zeros(len(grouped)),
                                       grouped["xr"].to_numpy() if has_lon else _np.zeros(len(grouped)),
                                       dl_arr, dr_arr, m_vals):
                            ll, lr, xll, xlr, dl_, dr_, v = row
                            if _np.isnan(v): continue
                            depth_grids.setdefault((dl_, dr_), []).append([float(ll), float(lr), float(xll), float(xlr), round(float(v), 6)])
                            all_depth2.setdefault((float(ll), float(lr), float(xll), float(xlr)), []).append(float(v))
                        for (dl_, dr_), raw in depth_grids.items():
                            depth_label = f"{dl_:.1f}-{dr_:.1f}"
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}|{depth_label}"
                            vals = [r[4] for r in raw]
                            out = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in raw] if eff_gt == "points" else raw
                            yield key, {"grid_type": eff_gt, "data": out, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                        avg2 = [[ll, lr, xll, xlr, round(sum(vs)/len(vs), 6)] for (ll, lr, xll, xlr), vs in all_depth2.items()]
                        if avg2:
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}|all_depths"
                            vals = [r[4] for r in avg2]
                            out2 = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in avg2] if eff_gt == "points" else avg2
                            yield key, {"grid_type": eff_gt, "data": out2, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                    else:
                        if has_lon:
                            grid = [[float(r["yl"]), float(r["yr"]), float(r["xl"]), float(r["xr"]), round(float(r[metric]), 6)]
                                    for _, r in grouped.iterrows() if not _np.isnan(r[metric])]
                        else:
                            grid = [[float(r["yl"]), float(r["yr"]), -180.0, 180.0, round(float(r[metric]), 6)]
                                    for _, r in grouped.iterrows() if not _np.isnan(r[metric])]
                        if grid:
                            key = f"{model}|{ref_prefix}{var}|{metric}{key_suffix}"
                            vals = [r[4] for r in grid]
                            out3 = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in grid] if eff_gt == "points" else grid
                            yield key, {"grid_type": eff_gt, "data": out3, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}

        # Unique (rac, rtc) integer-coded pairs → decode to strings for key building
        rac_rtc_pairs = list(dict.fromkeys(zip(df["rac"].tolist(), df["rtc"].tolist())))

        # Pass 1: per lt
        for rac, rtc in rac_rtc_pairs:
            ra, rt = ra_vals[rac], rt_vals[rtc]
            mask_rart = (df["rac"] == rac) & (df["rtc"] == rtc)
            for lt in sorted(df.loc[mask_rart, "lt"].unique()):
                mask = mask_rart & (df["lt"] == lt)
                yield from _emit_agg(df[mask], ra, rt, f"|{lt}")

        # Pass 2: all lt
        for rac, rtc in rac_rtc_pairs:
            ra, rt = ra_vals[rac], rt_vals[rtc]
            mask = (df["rac"] == rac) & (df["rtc"] == rtc)
            yield from _emit_agg(df[mask], ra, rt, "|all")

        # Pass 3: per FRT
        for rac, rtc in rac_rtc_pairs:
            ra, rt = ra_vals[rac], rt_vals[rtc]
            mask_rart = (df["rac"] == rac) & (df["rtc"] == rtc)
            for lt in sorted(df.loc[mask_rart, "lt"].unique()):
                for frtc in sorted(df.loc[mask_rart & (df["lt"] == lt), "frtc"].unique()):
                    frt = frt_vals[frtc]
                    mask = mask_rart & (df["lt"] == lt) & (df["frtc"] == frtc)
                    yield from _emit_agg(df[mask], ra, rt, f"|{lt}|{frt}")

        del df


# ---------------------------------------------------------------------------
# Legacy dict-accumulator path (fallback when numpy/pandas unavailable)
# ---------------------------------------------------------------------------

def _iter_grid_data_legacy(
    per_bins_by_time,
    model: str,
    metadata: Dict[str, Any],
    lat_band_mode: bool,
    grid_type_fn,
):
    """Dict-based accumulator path — used when per_bins_by_time is a plain
    list or stream (not a :class:`_NpzBinStore`)."""
    all_metrics = list(metadata["metrics"])
    n_metrics = len(all_metrics)
    metric_idx = {m: i for i, m in enumerate(all_metrics)}

    def _make_row() -> list:
        return [0.0] * (n_metrics * 2)

    def _get_cell_key(b: Dict[str, Any], has_depth: bool) -> tuple:
        if lat_band_mode:
            lat_l, lat_r = _parse_bin_value(b["lat_bin"])
            if has_depth and "depth_bin" in b:
                db = b["depth_bin"]
                dk = (db["left"], db["right"]) if isinstance(db, dict) else _parse_bin_value(db)
                return (lat_l, lat_r, dk[0], dk[1])
            return (lat_l, lat_r)
        else:
            lat_l, lat_r, lon_l, lon_r = _extract_lat_lon_bounds(b)
            if has_depth and "depth_bin" in b:
                db = b["depth_bin"]
                dk = (db["left"], db["right"]) if isinstance(db, dict) else _parse_bin_value(db)
                return (lat_l, lat_r, lon_l, lon_r, dk[0], dk[1])
            return (lat_l, lat_r, lon_l, lon_r)

    def _accum_entry(entry, cells_by_var):
        for var_name, bins in entry["per_bins"].items():
            if not bins:
                continue
            has_depth = var_name in metadata["depth_bins"]
            cells = cells_by_var[var_name]
            for b in bins:
                cell_key = _get_cell_key(b, has_depth)
                row = cells[cell_key]
                for metric, mi in metric_idx.items():
                    v = b.get(metric)
                    if v is not None and not (isinstance(v, float) and math.isnan(v)):
                        i2 = mi * 2
                        row[i2] += v
                        row[i2 + 1] += 1

    def _emit_cells(cells_by_var, ra, rt, ref_prefix, key_suffix):
        eff_gt = grid_type_fn(rt)
        for var_name in sorted(cells_by_var):
            has_depth = var_name in metadata["depth_bins"]
            cell_accums = cells_by_var[var_name]
            for metric in all_metrics:
                mi = metric_idx[metric]
                i2 = mi * 2
                if lat_band_mode:
                    if has_depth:
                        depth_bands: Dict[tuple, list] = defaultdict(list)
                        all_depth: Dict[tuple, list] = defaultdict(list)
                        for cell_key, row in cell_accums.items():
                            cnt = row[i2 + 1]
                            if cnt == 0:
                                continue
                            val = round(row[i2] / cnt, 6)
                            lat_l, lat_r, dl, dr = cell_key
                            depth_bands[(dl, dr)].append([lat_l, lat_r, val])
                            all_depth[(lat_l, lat_r)].append(val)
                        for (dl, dr), bdata in depth_bands.items():
                            depth_label = f"{dl:.1f}-{dr:.1f}"
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|{depth_label}"
                            vals = [r[2] for r in bdata]
                            yield key, {"grid_type": eff_gt, "data": sorted(bdata, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                        avg = [[s, n, round(_mean(vs), 6)] for (s, n), vs in all_depth.items() if not math.isnan(_mean(vs))]
                        if avg:
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|all_depths"
                            vals = [r[2] for r in avg]
                            yield key, {"grid_type": eff_gt, "data": sorted(avg, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                    else:
                        band = []
                        for cell_key, row in cell_accums.items():
                            cnt = row[i2 + 1]
                            if cnt == 0:
                                continue
                            s, n = cell_key
                            band.append([s, n, round(row[i2] / cnt, 6)])
                        if band:
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}"
                            vals = [r[2] for r in band]
                            yield key, {"grid_type": eff_gt, "data": sorted(band, key=lambda r: r[0]), "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                else:
                    if has_depth:
                        depth_grids: Dict[tuple, list] = defaultdict(list)
                        all_depth2: Dict[tuple, list] = defaultdict(list)
                        for cell_key, row in cell_accums.items():
                            cnt = row[i2 + 1]
                            if cnt == 0:
                                continue
                            val = round(row[i2] / cnt, 6)
                            lat_l, lat_r, lon_l, lon_r, dl, dr = cell_key
                            depth_grids[(dl, dr)].append([lat_l, lat_r, lon_l, lon_r, val])
                            all_depth2[(lat_l, lat_r, lon_l, lon_r)].append(val)
                        for (dl, dr), raw in depth_grids.items():
                            depth_label = f"{dl:.1f}-{dr:.1f}"
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|{depth_label}"
                            vals = [r[4] for r in raw]
                            out = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in raw] if eff_gt == "points" else raw
                            yield key, {"grid_type": eff_gt, "data": out, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                        avg2 = [[ll, lr, xl, xr, round(_mean(vs), 6)] for (ll, lr, xl, xr), vs in all_depth2.items() if not math.isnan(_mean(vs))]
                        if avg2:
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}|all_depths"
                            vals = [r[4] for r in avg2]
                            out2 = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in avg2] if eff_gt == "points" else avg2
                            yield key, {"grid_type": eff_gt, "data": out2, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}
                    else:
                        grid = []
                        for cell_key, row in cell_accums.items():
                            cnt = row[i2 + 1]
                            if cnt == 0:
                                continue
                            lat_l, lat_r, lon_l, lon_r = cell_key
                            grid.append([lat_l, lat_r, lon_l, lon_r, round(row[i2] / cnt, 6)])
                        if grid:
                            key = f"{model}|{ref_prefix}{var_name}|{metric}{key_suffix}"
                            vals = [r[4] for r in grid]
                            out3 = [[(r[0]+r[1])/2, (r[2]+r[3])/2, r[4]] for r in grid] if eff_gt == "points" else grid
                            yield key, {"grid_type": eff_gt, "data": out3, "vmin": round(min(vals), 6), "vmax": round(max(vals), 6)}

    # Discover groups in one pass
    groups: set = set()
    ra_rt_pairs_set: set = set()
    for entry in per_bins_by_time:
        rt = entry.get("ref_type") or "gridded"
        ra = entry.get("ref_alias") or rt
        lt = entry["lead_time"]
        groups.add((ra, rt, lt))
        ra_rt_pairs_set.add((ra, rt))

    # Pass 1: per-lt
    for (ra, rt, lt) in sorted(groups):
        cells_by_var: Dict[str, Dict[tuple, list]] = defaultdict(lambda: defaultdict(_make_row))
        for entry in per_bins_by_time:
            e_rt = entry.get("ref_type") or "gridded"
            e_ra = entry.get("ref_alias") or e_rt
            if e_ra != ra or e_rt != rt or entry["lead_time"] != lt:
                continue
            _accum_entry(entry, cells_by_var)
        yield from _emit_cells(cells_by_var, ra, rt, f"{ra}|", f"|{lt}")
        del cells_by_var

    # Pass 2: all-lt
    for (ra, rt) in sorted(ra_rt_pairs_set):
        cells_by_var2: Dict[str, Dict[tuple, list]] = defaultdict(lambda: defaultdict(_make_row))
        for entry in per_bins_by_time:
            e_rt = entry.get("ref_type") or "gridded"
            e_ra = entry.get("ref_alias") or e_rt
            if e_ra != ra or e_rt != rt:
                continue
            _accum_entry(entry, cells_by_var2)
        yield from _emit_cells(cells_by_var2, ra, rt, f"{ra}|", "|all")
        del cells_by_var2

    # Pass 3: per-FRT
    for entry in per_bins_by_time:
        rt = entry.get("ref_type") or "gridded"
        ra = entry.get("ref_alias") or rt
        lt = entry["lead_time"]
        frt_raw = entry.get("forecast_reference_time", "")
        frt_date = frt_raw[:10] if frt_raw else "unknown"
        cells_frt: Dict[str, Dict[tuple, list]] = defaultdict(lambda: defaultdict(_make_row))
        _accum_entry(entry, cells_frt)
        yield from _emit_cells(cells_frt, ra, rt, f"{ra}|", f"|{lt}|{frt_date}")
        del cells_frt


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
        depth_label = _extract_depth_from_key(key)
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
        if len(parts) < 5:
            continue
        depth_label = _extract_depth_from_key(key)
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
        depth_label = _extract_depth_from_key(key)
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
    _total_grids = len(grids) if isinstance(grids, dict) else None
    _iter = (
        _rich_progress.track(
            items,
            description="  Writing map files …",
            total=_total_grids,
            transient=True,
        )
        if _HAS_RICH else items
    )
    for key, grid_info in _iter:
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

    logger.opt(colors=True).info(
        "  <cyan>✓</cyan>  Wrote <b>{}</b> grid file(s) + manifest  <dim>→</dim>  <cyan>{}</cyan>",
        len(manifest), map_dir
    )


def preprocess_per_bins(results_dir: Path, output_dir: Path, config: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Main entry point: load per-bins files, aggregate, write output.

    Returns metadata dict if per-bins files were found, None otherwise.

    Uses a **single pass** through :func:`_iter_grid_data` instead of the
    previous two-pass approach (pass 1 = colour scales, pass 2 = write).
    Grid files are written immediately during iteration; a fast fixup
    pass then rewrites only the files whose local vmin/vmax differ from
    the global colour-scale values.  This eliminates the expensive second
    DataFrame construction + groupby aggregation.
    """
    datasets = load_per_bins_files(results_dir)
    if not datasets:
        logger.info("No *_per_bins.json/.jsonl files found, skipping map page.")
        return None

    logger.debug("Discovering metadata...")
    metadata = discover_metadata(datasets, results_dir=results_dir, config=config)
    logger.debug("  Models: {}", metadata['models'])
    logger.debug("  Variables: {}", metadata['variables'])
    logger.debug("  Metrics: {}", metadata['metrics'])
    logger.debug("  Lead times: {}", metadata['lead_times'])
    if metadata["depth_bins"]:
        for var, dbs in metadata["depth_bins"].items():
            logger.debug("  Depth bins for {}: {} levels", var, len(dbs))
    if metadata["ref_variables"]:
        logger.debug("  Reference datasets: {}", list(metadata['ref_variables'].keys()))

    logger.opt(colors=True).info(
        "  <dim>◎</dim>  Writing grid files + computing colour scales (single pass) …"
    )

    # ── Single pass: write grid JS files and track colour-scale stats ──
    map_dir = output_dir / "map_data"
    map_dir.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, str] = {}
    group_min: Dict[tuple, float] = {}
    group_max: Dict[tuple, float] = {}
    # file_group tracks which files belong to which colour-scale group
    # and what their local (per-grid) vmin/vmax were at write time.
    file_group: Dict[str, tuple] = {}  # filename → (group, local_vmin, local_vmax)

    _iter = _iter_grid_data(datasets, metadata)
    if _HAS_RICH:
        _iter = _rich_progress.track(
            _iter,
            description="  Writing grid files …",
            transient=True,
        )

    for key, grid_info in _iter:
        # Track colour-scale stats
        parts = key.split("|")
        grp = None
        if len(parts) >= 5:
            depth_label = _extract_depth_from_key(key)
            grp = (parts[1], parts[2], parts[3], depth_label)
            vmin = grid_info.get("vmin")
            vmax = grid_info.get("vmax")
            if vmin is not None and vmax is not None:
                if grp not in group_min:
                    group_min[grp] = vmin
                    group_max[grp] = vmax
                else:
                    group_min[grp] = min(group_min[grp], vmin)
                    group_max[grp] = max(group_max[grp], vmax)

        # Write JSONP grid file
        safe_name = key.replace("|", "_").replace(" ", "_")
        filename = f"{safe_name}.js"
        filepath = map_dir / filename
        payload = json.dumps(grid_info, separators=(",", ":"))
        with open(filepath, "w") as f:
            f.write(f"_mapDataCallback({payload});\n")
        manifest[key] = filename

        if grp is not None and grid_info.get("vmin") is not None:
            file_group[filename] = (grp, grid_info["vmin"], grid_info["vmax"])

    # ── Fast fixup pass: apply global colour scales ────────────────────
    # Only rewrites files whose local vmin/vmax differ from the group
    # global values.  Each file is a small JSONP snippet (a few KB to a
    # few hundred KB), so this is orders of magnitude cheaper than
    # re-iterating _iter_grid_data.
    _n_fixed = 0
    for filename, (grp, local_vmin, local_vmax) in file_group.items():
        gmin = group_min.get(grp)
        gmax = group_max.get(grp)
        if gmin is None or (local_vmin == gmin and local_vmax == gmax):
            continue
        filepath = map_dir / filename
        with open(filepath) as f:
            content = f.read()
        content = re.sub(r'"vmin":[^,}]+', f'"vmin":{gmin}', content)
        content = re.sub(r'"vmax":[^,}]+', f'"vmax":{gmax}', content)
        with open(filepath, "w") as f:
            f.write(content)
        _n_fixed += 1

    if _n_fixed:
        logger.debug("  Colour-scale fixup: {} file(s) updated", _n_fixed)

    logger.debug("  Found {} colour-scale groups", len(group_min))

    # ── Write manifest + metadata ──────────────────────────────────────
    meta_file = map_dir / "manifest.json"
    with open(meta_file, "w") as f:
        json.dump(
            {"metadata": metadata, "files": manifest},
            f,
            indent=2,
            default=str,
        )

    logger.opt(colors=True).info(
        "  <cyan>✓</cyan>  Wrote <b>{}</b> grid file(s) + manifest"
        "  <dim>→</dim>  <cyan>{}</cyan>",
        len(manifest), map_dir,
    )

    return metadata
