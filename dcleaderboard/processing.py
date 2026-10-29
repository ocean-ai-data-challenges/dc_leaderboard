"""
Data processing and report generation module for the leaderboard.

This module handles data loading, statistical calculations (percent differences),
HTML generation via Pandas Styler, and Matplotlib-based visualizations.
"""

import fnmatch
import json
import re
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import cm
from matplotlib.colors import Normalize, Colormap

METRICS_NAMES = {
    "rmse": "Root Mean Squared Error",
    "rmsd": "Root Mean Squared Deviation",
    "rmsd_geostrophic_currents": "RMSD of Geostrophic Currents",
    "rmsd_mld": "RMSD of Mixed Layer Depth",
    "lagrangian": "Lagrangian analysis",
}


# How a metric is read, for the colours: "lower" (an error: lower is better), a target value
# (closer is better: a bias, a ratio), or None (no better or worse: a count, a spread). Patterns
# (fnmatch) are allowed; ``metrics_targets`` in the config adds to / overrides these.
DEFAULT_METRIC_TARGETS: Dict[str, Any] = {
    "bias": 0.0,
    "me": 0.0,
    "ssr": 1.0,
    "ssr_c": 1.0,
    "energy_ratio_mean_*": None,   # the ensemble mean is smoother than the truth by design
    "energy_ratio_*": 1.0,
    "count": None,
    "spread": None,
    "n_members": None,
}


def metric_target(metric: str, config: Optional[Dict[str, Any]] = None) -> Any:
    """"lower", a target value, or None (neutral) for ``metric``; first matching pattern wins,
    the config's ``metrics_targets`` before the defaults."""
    targets = dict((config or {}).get("metrics_targets") or {})
    for table in (targets, DEFAULT_METRIC_TARGETS):
        if metric in table:
            return table[metric]
        for pattern, value in table.items():
            if any(c in pattern for c in "*?[") and fnmatch.fnmatch(metric, pattern):
                return value
    return "lower"


def lead_day_label(lead_time: Any, zero_based: bool = False) -> str:
    """"Lead day N": N is the lead time in days (``zero_based``) or the lead time + 1."""
    if lead_time is None:
        return "unknown"
    return f"Lead day {int(lead_time) if zero_based else int(lead_time) + 1}"


def restrict_to_common_period(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Keep the forecasts every model has a period for: initialisations between the latest first
    one and the earliest last one of the models (their periods' overlap).

    A model's means over another period than the others' are not comparable (a 2024 baseline
    against a 2023-2024 challenger). Returns the restricted frame and, per model, its
    ``first``/``last`` initialisation and the number of forecasts kept / in total, plus the
    ``common`` period (None when the periods do not overlap: nothing is then removed).
    """
    info: Dict[str, Any] = {"models": {}, "common": None}
    if df.empty or "frt" not in df.columns or df["frt"].isna().all():
        return df, info
    frt = pd.to_datetime(df["frt"], errors="coerce")
    by_model = frt.groupby(df["model"])
    first, last = by_model.min(), by_model.max()
    lo, hi = first.max(), last.min()
    keep = (frt >= lo) & (frt <= hi) if lo <= hi else pd.Series(True, index=df.index)
    for model in first.index:
        sel = df["model"] == model
        info["models"][model] = {
            "first": first[model], "last": last[model],
            "n_total": int(frt[sel].nunique()), "n_kept": int(frt[sel & keep].nunique()),
        }
    if lo <= hi:
        info["common"] = (lo, hi)
        df = df[keep]
    return df, info


def restrict_to_period(df: pd.DataFrame, period: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Keep the forecasts initialised in a declared period, of the models that cover it.

    ``period``: ``start``, ``end`` (dates) and ``coverage_tolerance_days`` (default 21): a model
    covers the period when its first forecast in it is at most that many days after ``start`` and
    its last one at most that many days before ``end`` (the last forecasts end before ``end`` by
    their horizon). A model that does not (a 2024 baseline in a 2023-2024 view) is left out of the
    view, not scored over part of it. ``exclude_models`` lists models left out whatever their
    coverage (a model whose training saw part of the period: another track). Returns the frame and
    {"excluded": {model: (first, last)}, "other_track": {model: (first, last)}}.
    """
    info: Dict[str, Any] = {"excluded": {}, "other_track": {}}
    if df.empty or "frt" not in df.columns:
        return df, info
    other_track = set(period.get("exclude_models") or [])
    start, end = pd.Timestamp(period["start"]), pd.Timestamp(period["end"])
    tol = pd.Timedelta(days=int(period.get("coverage_tolerance_days", 21)))
    frt = pd.to_datetime(df["frt"], errors="coerce")
    df = df[(frt >= start) & (frt <= end)]
    frt = frt[df.index]
    keep = pd.Series(True, index=df.index)
    for model, f in frt.groupby(df["model"]):
        first, last = f.min(), f.max()
        if model in other_track:
            info["other_track"][model] = (first, last)
            keep &= df["model"] != model
        elif first - start > tol or end - last > tol:
            info["excluded"][model] = (first, last)
            keep &= df["model"] != model
    return df[keep], info


def display_variable(label: str, variables_map: Dict[str, str]) -> str:
    """The display name of a variable label: the label's own entry, else its depth prefix
    ("Surface", "50m") kept and the rest translated ("50m u_current" with u_current mapped)."""
    if label in variables_map:
        return variables_map[label]
    m = re.match(r"^(surface|\d+(?:\.\d+)?m)\s+(.+)$", label, flags=re.IGNORECASE)
    if m and m.group(2) in variables_map:
        return f"{m.group(1)} {variables_map[m.group(2)]}"
    return label


def get_depth_order(variable_name: str) -> int:
    """Extract depth from variable name for sorting."""
    variable_lower = variable_name.lower()
    if "surface" in variable_lower:
        return 0
    # Extract depth numbers (50m, 200m, etc.)
    depth_match = re.search(r"(\d+)m", variable_lower)
    if depth_match:
        return int(depth_match.group(1))
    return 999  # Put variables without depth at the end


def get_variable_type(variable_name: str) -> str:
    """Extract variable type (without depth) from variable name."""
    variable_lower = variable_name.lower()

    # Remove depth indications
    # Remove "surface", depths in meters, etc.
    cleaned_var = re.sub(r"\bsurface\b", "", variable_lower)
    cleaned_var = re.sub(r"\d+m\b", "", cleaned_var)
    cleaned_var = re.sub(r"\d+_m\b", "", cleaned_var)
    cleaned_var = re.sub(r"_surface\b", "", cleaned_var)

    # Clean underscores and multiple spaces
    cleaned_var = re.sub(r"_+", "_", cleaned_var)
    cleaned_var = cleaned_var.strip("_").strip()

    # If cleaned variable is empty, use original variable
    if not cleaned_var:
        return variable_name.lower()

    return cleaned_var


def sort_variables_by_type_and_depth(variables: List[str]) -> List[str]:
    """Sort variables by type then depth."""

    def sort_key(var: str) -> Tuple[str, int, str]:
        var_type = get_variable_type(var)
        depth = get_depth_order(var)
        return var_type, depth, var

    return sorted(variables, key=sort_key)


def get_lead_days_for_display(all_lead_days: List[str], max_count: int = 5,
                              mode: str = "odd") -> List[str]:
    """
    Select lead days for display.

    ``mode`` "spread": ``max_count`` lead days evenly spread from the first to the last (the
    horizon may be any length). ``mode`` "odd" (the former behaviour):

    - Always include Lead day 1 if it exists
    - Take odd lead days
    - Limit total count and remove last if necessary
    """
    if mode == "spread":
        ordered = sorted(
            (ld for ld in all_lead_days if re.search(r"(\d+)", ld)),
            key=lambda ld: int(re.search(r"(\d+)", ld).group(1)),  # type: ignore[union-attr]
        )
        if len(ordered) <= max_count:
            return ordered
        idx = np.unique(np.round(np.linspace(0, len(ordered) - 1, max_count)).astype(int))
        return [ordered[i] for i in idx]
    # Extract and sort all lead days numerically
    lead_days_with_nums = []
    for ld in all_lead_days:
        m = re.search(r"(\d+)", ld)
        if m:
            lead_days_with_nums.append((int(m.group(1)), ld))

    lead_days_with_nums.sort()

    # Select lead day 1 first, then odds
    selected = []

    # Add Lead day 1 if exists
    for num, ld in lead_days_with_nums:
        if num == 1:
            selected.append(ld)
            break

    # Add other odd lead days (except 1 already added)
    for num, ld in lead_days_with_nums:
        if num % 2 == 1 and num != 1 and len(selected) < max_count:
            selected.append(ld)

    # If too many lead days, remove the last one (the largest)
    if len(selected) > max_count:
        selected = selected[:-1]

    return selected


def load_data(results_dir: Path) -> pd.DataFrame:
    """Load all JSON results into a DataFrame."""
    files = list(Path(results_dir).glob("*.json"))
    data = []

    for file in files:
        with open(file) as f:
            content = json.load(f)

            # Check format
            if "dataset" in content and "results" in content:
                dataset_name = content["dataset"]

                # Iterate over results for this dataset
                for model_key, entries in content["results"].items():
                    if isinstance(entries, list):
                        for entry in entries:
                            model = entry.get("model", model_key)
                            ref_alias = entry.get("ref_alias", "unknown")
                            lead_time = entry.get("lead_time", None)
                            lead_day = lead_day_label(lead_time)
                            frt = entry.get("forecast_reference_time")
                            result = entry.get("result", [])

                            # Process each item in result
                            if isinstance(result, list):
                                for item in result:
                                    metric = item.get("Metric", "unknown")
                                    variable = item.get("Variable", "unknown")
                                    value = item.get("Value", 0)

                                    if not isinstance(value, (int, float)):
                                        continue

                                    data.append(
                                        {
                                            "model": model,
                                            "metric": metric,
                                            "lead_day": lead_day,
                                            "lead_time": lead_time,
                                            "frt": frt,
                                            "variable": variable,
                                            "score": value,
                                            "ref_alias": ref_alias,
                                            "dataset": dataset_name,
                                        }
                                    )
                            elif isinstance(result, dict):
                                # Process dict format (like glorys)
                                for metric_name, variables in result.items():
                                    for variable, score in variables.items():
                                        data.append(
                                            {
                                                "model": model,
                                                "metric": metric_name,
                                                "lead_day": lead_day,
                                                "lead_time": lead_time,
                                                "frt": frt,
                                                "variable": variable,
                                                "score": score,
                                                "ref_alias": ref_alias,
                                                "dataset": dataset_name,
                                            }
                                        )

    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def bold_reference_index(val: str, reference_model: str) -> str:
    """Return styling to bold the reference model index."""
    return "font-weight: bold;" if val == reference_model else ""


def _color_cells(val: float, percent: float, real_cmap: Colormap, norm: Normalize) -> str:
    """Helper to color cells based on percentage difference."""
    if pd.isna(percent):
        return ""
    color = real_cmap(norm(percent))
    return (
        f"background-color: rgba({int(color[0] * 255)},"
        f"{int(color[1] * 255)},{int(color[2] * 255)},{color[3]:.2f})"
    )


def generate_report_items(
    df: pd.DataFrame, cmap_code: str = "coolwarm", config: Optional[Dict[str, Any]] = None
) -> Generator[Tuple[str, Any], None, None]:
    """
    Yield (type, content) items for the report.

    type: 'markdown' or 'styler'

    config: optional dict with keys:
      - metrics_names: dict mapping metric keys to display names
      - variables_names: dict mapping variable keys to display names
      - texts: dict with templates for headers:
          'reference_header': "## Reference dataset: {ref_alias}"
          'metric_header': "### Metric: {metric_name}"
          'variable_group_header': "#### {var_type} Variables"
    """
    if config is None:
        config = {}

    metrics_map = config.get("metrics_names", METRICS_NAMES)
    variables_map = config.get("variables_names", {})
    models_map = config.get("models_names", {})
    texts = config.get("texts", {})

    # Apply model aliasing if provided
    if models_map and not df.empty:
        df["dataset"] = df["dataset"].map(lambda x: models_map.get(x, x))
        df["model"] = df["model"].map(lambda x: models_map.get(x, x))

    yield ("markdown", '<div style="height: 90px;"></div>')

    if len(df) == 0:
        yield ("markdown", "## Aucune donnée trouvée dans les fichiers JSON")
        return

    lead_cfg = config.get("lead_days") or {}
    if lead_cfg.get("zero_based") and "lead_time" in df.columns:
        df = df.copy()
        df["lead_day"] = [lead_day_label(t, zero_based=True) if pd.notna(t) else "unknown"
                          for t in df["lead_time"]]

    # A declared period (config ``period``, one view of ``periods``): its forecasts, its models
    excluded: Dict[str, Any] = {}
    if config.get("period"):
        _period = dict(config["period"])
        # models named by their key or their display name (models_names)
        _period["exclude_models"] = [models_map.get(m, m) for m in _period.get("exclude_models") or []]
        df, _cov = restrict_to_period(df, _period)
        excluded = {m: (*v, "coverage") for m, v in _cov["excluded"].items()}
        excluded.update({m: (*v, "track") for m, v in _cov["other_track"].items()})
        if df.empty:
            yield ("markdown", "*No model covers this period.*")
            return

    # Models compared over their common period only (config ``common_period``)
    if config.get("common_period"):
        df, period = restrict_to_common_period(df)
        period["excluded"] = excluded
        if period.get("models"):
            yield ("markdown", "## " + texts.get("period_header", "Compared period"))
            yield ("markdown", _period_summary(period, texts))
            yield ("markdown", '<div style="height: 50px;"></div>')
        if df.empty:
            yield ("markdown", "*No forecast in the common period of the models.*")
            return

    # Check for lead days
    if "lead_day" in df.columns:
        all_unique_lead_days = df["lead_day"].unique()
        lead_days = get_lead_days_for_display(
            list(all_unique_lead_days), max_count=int(lead_cfg.get("max_count", 5)),
            mode=lead_cfg.get("mode", "odd"))
    else:
        lead_days = []

    reference_model = config.get("reference_model", "glonet")  # Default

    for ref_alias in sorted(df["ref_alias"].unique()):
        header = texts.get("reference_header", "## Reference dataset: {ref_alias}")
        yield ("markdown", header.format(ref_alias=ref_alias.upper()))

        ref_df = df[df.ref_alias == ref_alias]

        # Determine reference model for this dataset
        datasets_in_ref = ref_df["dataset"].unique()
        ref_key = config.get("reference_model", "glonet")
        ref_alias_name = models_map.get(ref_key, ref_key)
        if ref_alias_name in datasets_in_ref:
            reference_model = ref_alias_name
        else:
            # Take first dataset as reference
            reference_model = datasets_in_ref[0]

        # Sort all variables by type and depth
        all_variables = sort_variables_by_type_and_depth(
            list(ref_df["variable"].unique())
        )

        # Group variables by type
        variables_by_type: Dict[str, List[str]] = {}
        for var in all_variables:
            var_type = get_variable_type(var)
            if var_type not in variables_by_type:
                variables_by_type[var_type] = []
            variables_by_type[var_type].append(var)

        # Loop: variable group first, then metric
        for var_type, var_group in variables_by_type.items():
            header = texts.get("variable_group_header", "#### {var_type} Variables")
            yield ("markdown", header.format(
                var_type=variables_map.get(var_type, var_type.title())))

            # When the pipeline provides an explicit allowed_metrics list
            # (derived from the sources' metrics in the project YAML), use it
            # as a whitelist.  This hides sub-statistics (me, mse, …) that
            # metric classes may emit automatically.
            # metrics_names is intentionally NOT used as a filter here: it is
            # only a renaming map for display labels.
            _available = sorted(ref_df.metric.unique())
            _allowed = (config or {}).get("allowed_metrics")
            if _allowed:
                # in the order of the list (the scores to read first come first)
                _filtered = [m for m in _allowed if m in _available]
                _available = _filtered if _filtered else _available

            for metric in _available:
                metric_complete_name = metrics_map.get(metric, metric)
                header = texts.get("metric_header", "### Metric: {metric_name}")
                yield ("markdown", header.format(metric_name=metric_complete_name))

                # Filter on ref_alias, metric AND variable group
                ref_metric_df = ref_df[
                    (ref_df.metric == metric) & ref_df.variable.isin(var_group)
                ]

                # Filter on odd lead days
                sub = ref_metric_df[
                    ref_metric_df.lead_day.isin(lead_days)
                ]

                if len(sub) == 0:
                    yield ("markdown", "*No data available for these variables and lead days.*")
                    continue

                # Ensure lead day 1 is included if exists
                available_leads = sorted(
                    sub["lead_day"].unique(),
                    key=lambda x: int(re.search(r"\d+", x).group(0)),  # type: ignore
                )

                # Use pre-selected lead days that are available in data
                common_leads = [ld for ld in lead_days if ld in available_leads]

                # If no common lead days, take first available ones
                if not common_leads:
                    common_leads = available_leads[:4]

                sub = sub[sub.lead_day.isin(common_leads)]

                if len(sub) == 0:
                    yield ("markdown", "*No data available for selected lead days.*")
                    continue

                # Multi-index pivot with correct variable order
                # First sort variables in desired order
                ordered_variables = [var for var in var_group if var in sub["variable"].unique()]

                pivot = sub.pivot_table(
                    index="model", columns=["variable", "lead_day"], values="score", aggfunc="mean"
                )

                # Reorganize columns for correct order
                if isinstance(pivot.columns, pd.MultiIndex):
                    # Create new columns order
                    new_columns = []
                    for var in ordered_variables:
                        for lead in common_leads:
                            if (var, lead) in pivot.columns:
                                new_columns.append((var, lead))

                    # Reindex with new order
                    pivot = pivot.reindex(columns=new_columns)

                    # APPLY VARIABLE MAPPING HERE (depth prefix kept: "50m" + mapped name)
                    if variables_map:
                        pivot = pivot.rename(
                            columns=lambda v, vm=variables_map: display_variable(v, vm), level=0)

                if len(pivot) == 0:
                    yield ("markdown", "*No data to display.*")
                    continue

                # Reorder to put reference model first
                target = metric_target(metric, config)
                if reference_model in pivot.index:
                    new_order = [reference_model] + [m for m in pivot.index if m != reference_model]
                    pivot = pivot.reindex(new_order)
                    ref_values = pivot.loc[reference_model]
                    # colours: how much worse (red) or better (blue) than the reference, in %
                    # of the reference's error: the value itself for an error, the distance to
                    # the target for a bias or a ratio, nothing for a neutral quantity
                    if target is None:
                        percent_diff = pd.DataFrame(np.nan, index=pivot.index, columns=pivot.columns)
                    else:
                        bad = pivot if target == "lower" else (pivot - float(target)).abs()
                        bad_ref = bad.loc[reference_model]
                        with np.errstate(divide="ignore", invalid="ignore"):
                            percent_diff = (bad - bad_ref) / bad_ref.where(bad_ref.abs() > 1e-12) * 100
                else:
                    # If no reference model, no coloring
                    ref_values = None
                    percent_diff = pd.DataFrame(0, index=pivot.index, columns=pivot.columns)

                # Dynamic color scale calculation
                if ref_values is not None:
                    non_ref_values = percent_diff.values[1:] if len(percent_diff) > 1 else []
                    if len(non_ref_values) > 0 and not np.all(np.isnan(non_ref_values)):
                        absmax = np.nanmax(np.abs(non_ref_values))
                        # Minimum % for stronger contrast
                        absmax = max(absmax, 2)
                    else:
                        absmax = 2  # default value
                else:
                    absmax = 2

                norm = plt.Normalize(-absmax, absmax)
                real_cmap = plt.get_cmap(cmap_code)

                def style_func(
                    df_style: pd.DataFrame,
                    p_diff: pd.DataFrame = percent_diff,
                    r_cmap: Colormap = real_cmap,
                    n_orm: Normalize = norm,
                ) -> pd.DataFrame:
                    styled_df = pd.DataFrame("", index=df_style.index, columns=df_style.columns)
                    for i in df_style.index:
                        for j in df_style.columns:
                            # Using correct type access
                            percent = p_diff.loc[i, j]
                            styled_df.loc[i, j] = _color_cells(
                                df_style.loc[i, j], percent, r_cmap, n_orm
                            )
                    return styled_df

                # Determine where each variable starts for borders
                styles = []
                if isinstance(pivot.columns, pd.MultiIndex):
                    prev_var = None
                    col_idx = 0
                    for var, _ in pivot.columns:
                        if var != prev_var and prev_var is not None:
                            # Add border at start of each new variable group
                            # Use col_heading and data col classes that Pandas generates
                            styles.append(
                                {
                                    "selector": f"th.col_heading.level0.col{col_idx}",
                                    "props": [("border-left", "3px solid #dadce0")],
                                }
                            )
                            styles.append(
                                {
                                    "selector": f"th.col_heading.level1.col{col_idx}",
                                    "props": [("border-left", "3px solid #dadce0")],
                                }
                            )
                            styles.append(
                                {
                                    "selector": f"td.col{col_idx}",
                                    "props": [("border-left", "3px solid #dadce0")],
                                }
                            )
                        prev_var = var
                        col_idx += 1

                # Add specific formatting to match the expected output
                styles.extend([
                    # Center align data cells (skipping the first column which is the index)
                    {"selector": "td:not(:first-child)", "props": [("text-align", "center")]},
                    # Center align all headers
                    {"selector": "th.col_heading", "props": [("text-align", "center")]},
                    # Variable headers (Level 0): Bold
                    {
                        "selector": "th.col_heading.level0",
                        "props": [("text-align", "center"), ("font-weight", "bold")],
                    },
                    # Lead day headers (Level 1): Smaller font
                    {
                        "selector": "th.col_heading.level1",
                        "props": [("text-align", "center"), ("font-size", "0.9em")],
                    },
                ])

                styled = pivot.style.apply(style_func, axis=None).format("{:.3f}")

                # Apply the variable-separator border styles
                if styles:
                    styled = styled.set_table_styles(styles)

                pivot.columns.names = ["Variable", "Lead Day"]
                pivot.index.name = None

                # Set table attributes to match expected styles
                styled = styled.set_table_attributes('class="dataframe table"')
                
                # Use default value in lambda for loop variable reference_model
                styled = styled.map_index(
                    lambda val, ref=reference_model: bold_reference_index(val, ref), axis=0
                )

                yield ("styler", styled)
                yield ("markdown", '<div style="height: 50px;"></div>')


def _period_summary(period: Dict[str, Any], texts: Dict[str, Any]) -> str:
    """HTML table of the period of each model and the forecasts kept."""
    models = period.get("models") or {}
    common = period.get("common")
    if common is not None:
        head = texts.get("common_period_text",
                         "Every score is a mean over the forecasts initialised in the models' "
                         "common period, {first} to {last}.")
        head = head.format(first=f"{common[0]:%Y-%m-%d}", last=f"{common[1]:%Y-%m-%d}")
    else:
        head = "The models' periods do not overlap: each one is scored over its own."
    rows = "".join(
        f"<tr><th>{model}</th><td>{m['first']:%Y-%m-%d}</td><td>{m['last']:%Y-%m-%d}</td>"
        f"<td>{m['n_kept']} / {m['n_total']}</td></tr>"
        for model, m in models.items()
    )
    why = {
        "coverage": texts.get("excluded_text", "not shown: does not cover the period"),
        "track": texts.get("other_track_text", "not shown: ranked in another track"),
    }
    rows += "".join(
        f"<tr><th>{model}</th><td>{first:%Y-%m-%d}</td><td>{last:%Y-%m-%d}</td>"
        f"<td>{why[reason]}</td></tr>"
        for model, (first, last, reason) in (period.get("excluded") or {}).items()
    )
    return (f"<p>{head}</p><div class=\"table-responsive\"><table class=\"dataframe table\">"
            "<thead><tr><th>Model</th><th>First forecast</th><th>Last forecast</th>"
            f"<th>Forecasts kept</th></tr></thead><tbody>{rows}</tbody></table></div>")


def create_legend_plot(cmap_code: str = "coolwarm",
                       label: str = "Error relative to the reference (%): blue better, red worse",
                       ) -> plt.Figure:
    """Create a matplotlib figure for the legend."""
    cmap = plt.get_cmap(cmap_code)
    norm = plt.Normalize(-100, 100)

    # Colorbar
    fig, ax = plt.subplots(figsize=(6.5, 0.4))

    # Colorbar
    cb = plt.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), cax=ax, orientation="horizontal")
    
    cb.set_label(label, fontsize=9.5, labelpad=5)
    cb.ax.tick_params(labelsize=8.5)

    return fig
