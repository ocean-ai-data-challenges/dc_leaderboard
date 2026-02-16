"""
Data processing and report generation module for the leaderboard.

This module handles data loading, statistical calculations (percent differences),
HTML generation via Pandas Styler, and Matplotlib-based visualizations.
"""

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


def get_lead_days_for_display(all_lead_days: List[str], max_count: int = 4) -> List[str]:
    """
    Select lead days for display.

    - Always include Lead day 1 if it exists
    - Take odd lead days
    - Limit total count and remove last if necessary
    """
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
                            lead_day = (
                                f"Lead day {lead_time + 1}" if lead_time is not None else "unknown"
                            )
                            result = entry.get("result", [])

                            # Process each item in result
                            if isinstance(result, list):
                                for item in result:
                                    metric = item.get("Metric", "unknown")
                                    variable = item.get("Variable", "unknown")
                                    value = item.get("Value", 0)

                                    data.append(
                                        {
                                            "model": model,
                                            "metric": metric,
                                            "lead_day": lead_day,
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
    texts = config.get("texts", {})

    yield ("markdown", '<div style="height: 90px;"></div>')

    if len(df) == 0:
        yield ("markdown", "## Aucune donnée trouvée dans les fichiers JSON")
        return

    # Check for lead days
    if "lead_day" in df.columns:
        all_unique_lead_days = df["lead_day"].unique()
        lead_days = get_lead_days_for_display(list(all_unique_lead_days), max_count=4)
    else:
        lead_days = []

    reference_model = "glonet"  # Default

    for ref_alias in sorted(df["ref_alias"].unique()):
        header = texts.get("reference_header", "## Reference dataset: {ref_alias}")
        yield ("markdown", header.format(ref_alias=ref_alias.upper()))

        ref_df = df[df.ref_alias == ref_alias]

        # Determine reference model for this dataset
        datasets_in_ref = ref_df["dataset"].unique()
        if "glonet" in datasets_in_ref:
            reference_model = "glonet"
        else:
            # Take first dataset as reference
            reference_model = datasets_in_ref[0]

        # Loop over metrics for this reference dataset
        for metric in sorted(ref_df.metric.unique()):
            metric_complete_name = metrics_map.get(metric, metric)
            header = texts.get("metric_header", "### Metric: {metric_name}")
            yield ("markdown", header.format(metric_name=metric_complete_name))

            # Filter on ref_alias AND metric
            ref_metric_df = ref_df[ref_df.metric == metric]

            # Sort variables by type and depth
            metric_variables = sort_variables_by_type_and_depth(
                list(ref_metric_df["variable"].unique())
            )

            # Group variables by type
            variables_by_type: Dict[str, List[str]] = {}
            for var in metric_variables:
                var_type = get_variable_type(var)
                if var_type not in variables_by_type:
                    variables_by_type[var_type] = []
                variables_by_type[var_type].append(var)

            # Process each variable group
            for var_type, var_group in variables_by_type.items():
                header = texts.get("variable_group_header", "#### {var_type} Variables")
                yield ("markdown", header.format(var_type=var_type.title()))

                # Filter on group variables and odd lead days
                sub = ref_metric_df[
                    ref_metric_df.variable.isin(var_group) & ref_metric_df.lead_day.isin(lead_days)
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

                    # APPLY VARIABLE MAPPING HERE
                    if variables_map:
                        # Rename levels of columns using dict
                        pivot = pivot.rename(columns=variables_map, level=0)

                if len(pivot) == 0:
                    yield ("markdown", "*No data to display.*")
                    continue

                # Reorder to put reference model first
                if reference_model in pivot.index:
                    new_order = [reference_model] + [m for m in pivot.index if m != reference_model]
                    pivot = pivot.reindex(new_order)
                    ref_values = pivot.loc[reference_model]
                    percent_diff = (pivot - ref_values) / ref_values * 100
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
                            # Add border at start of each new variable
                            styles.append(
                                {
                                    "selector": f"th.col{col_idx}, td.col{col_idx}",
                                    "props": [("border-left", "3px solid #333")],
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
                # Add class="dataframe" and Quarto generic classes for correct styling
                # Matches: class="caption-top table table-sm table-striped small"
                styled = styled.set_table_attributes(
                    'class="caption-top table table-sm table-striped small" data-quarto-postprocess="true"'
                )
                pivot.columns.names = ["Variable", "Lead Day"]
                pivot.index.name = None

                # Set table attributes to match expected styles
                # Removed table-striped to avoid conflict with custom row styling
                styled = styled.set_table_attributes('class="dataframe table"')
                
                # Use default value in lambda for loop variable reference_model
                styled = styled.map_index(
                    lambda val, ref=reference_model: bold_reference_index(val, ref), axis=0
                )

                yield ("styler", styled)
                yield ("markdown", '<div style="height: 50px;"></div>')


def create_legend_plot(cmap_code: str = "coolwarm") -> plt.Figure:
    """Create a matplotlib figure for the legend."""
    cmap = plt.get_cmap(cmap_code)
    norm = plt.Normalize(-100, 100)

    # Significantly reduced height for a thinner, more standard colorbar look
    # Was (8, 1.2), now (6, 0.35)
    fig, ax = plt.subplots(figsize=(6, 0.35))

    # Colorbar
    cb = plt.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), cax=ax, orientation="horizontal")
    
    # Adjust font sizes for better proportions with the smaller bar
    cb.set_label("Deviation from Reference (%)", fontsize=10, labelpad=5)
    cb.ax.tick_params(labelsize=8)

    return fig
