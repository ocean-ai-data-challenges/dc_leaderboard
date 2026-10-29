"""Leaderboard tables: common period, colours by metric target, lead days, names."""

import json

import numpy as np
import pandas as pd
import pytest

from dcleaderboard.processing import (
    display_variable,
    generate_report_items,
    get_lead_days_for_display,
    load_data,
    metric_target,
    restrict_to_common_period,
)


def _write(tmp_path, model, frts, values, ref="glorys", leads=range(10)):
    entries = []
    for frt in frts:
        for lead in leads:
            entries.append({
                "ref_alias": ref, "lead_time": lead, "forecast_reference_time": frt,
                "result": [{"Metric": m, "Variable": "Surface ssh", "Value": v}
                           for m, v in values.items()],
            })
    (tmp_path / f"results_{model}.json").write_text(
        json.dumps({"dataset": model, "results": {model: entries}}))


def _tables(df, config):
    return [c for kind, c in generate_report_items(df, config=config) if kind == "styler"]


def test_models_are_compared_over_their_common_period(tmp_path):
    _write(tmp_path, "glonet", ["2024-01-03", "2024-06-05"], {"rmse": 1.0})
    _write(tmp_path, "ens", ["2023-01-05", "2023-06-01", "2024-01-04", "2024-06-06"],
           {"rmse": 2.0})
    df = load_data(tmp_path)
    kept, info = restrict_to_common_period(df)
    assert info["common"] == (pd.Timestamp("2024-01-03"), pd.Timestamp("2024-06-05"))
    assert info["models"]["ens"]["n_total"] == 4 and info["models"]["ens"]["n_kept"] == 1
    assert info["models"]["glonet"]["n_kept"] == 2
    items = list(generate_report_items(df, config={"common_period": True}))
    assert any(k == "markdown" and "Compared period" in c for k, c in items)


def test_colours_follow_each_metrics_target():
    assert metric_target("rmse") == "lower"
    assert metric_target("bias") == 0.0 and metric_target("ssr") == 1.0
    assert metric_target("energy_ratio_lt100km") == 1.0
    assert metric_target("energy_ratio_mean_lt100km") is None and metric_target("count") is None
    assert metric_target("bias", {"metrics_targets": {"bias": None}}) is None


def test_a_bias_of_the_other_sign_is_not_better(tmp_path):
    _write(tmp_path, "glonet", ["2024-01-03"], {"bias": -0.3, "ssr": 0.5})
    _write(tmp_path, "other", ["2024-01-03"], {"bias": 0.3, "ssr": 1.5})
    df = load_data(tmp_path)
    import matplotlib.pyplot as plt
    c = plt.get_cmap("coolwarm")(0.5)
    zero = f"rgba({int(c[0] * 255)},{int(c[1] * 255)},{int(c[2] * 255)}"
    for styler in _tables(df, {"allowed_metrics": ["bias", "ssr"]}):
        styles = styler._compute().ctx
        other_row = [v for (i, _), v in styles.items() if i == 1]
        # same distance to the target as the reference: 0 % -> the colour of 0, not blue/red
        assert other_row and all(zero in str(v) for v in other_row)


def test_lead_days_spread_over_the_horizon():
    leads = [f"Lead day {d}" for d in range(28)]
    shown = get_lead_days_for_display(leads, 5, mode="spread")
    assert shown[0] == "Lead day 0" and shown[-1] == "Lead day 27" and len(shown) == 5
    assert get_lead_days_for_display([f"Lead day {d}" for d in range(1, 11)], 5) == [
        "Lead day 1", "Lead day 3", "Lead day 5", "Lead day 7", "Lead day 9"]   # former default


def test_zero_based_lead_labels_and_metric_order(tmp_path):
    _write(tmp_path, "glonet", ["2024-01-03"], {"rmse": 1.0, "crmse": 0.5, "mse": 1.0},
           leads=[0, 1])
    df = load_data(tmp_path)
    items = list(generate_report_items(df, config={
        "lead_days": {"zero_based": True, "mode": "spread"},
        "allowed_metrics": ["crmse", "rmse"]}))
    headers = [c for k, c in items if k == "markdown" and c.startswith("### Metric")]
    assert headers == ["### Metric: crmse", "### Metric: Root Mean Squared Error"]   # list order
    tables = [c for k, c in items if k == "styler"]
    assert "Lead day 0" in tables[0].to_html()


def test_variable_names_keep_the_depth():
    names = {"u_current": "Eastward velocity (U)", "Surface ssh": "Sea surface height"}
    assert display_variable("50m u_current", names) == "50m Eastward velocity (U)"
    assert display_variable("Surface ssh", names) == "Sea surface height"
    assert display_variable("200m salinity", names) == "200m salinity"


def test_ratio_colours_are_relative_to_the_target():
    # an ssr of 1.5 is as far from 1 as 0.5: same badness as the reference
    df = pd.DataFrame({
        "model": ["glonet", "other"], "metric": ["ssr", "ssr"], "lead_day": ["Lead day 1"] * 2,
        "variable": ["Surface ssh"] * 2, "score": [0.5, 1.5], "ref_alias": ["x", "x"],
        "dataset": ["glonet", "other"],
    })
    styler = _tables(df, {})[0]
    assert np.isfinite(styler.data.values).all()


def test_a_period_keeps_the_models_that_cover_it(tmp_path):
    from dcleaderboard.processing import restrict_to_period
    _write(tmp_path, "glonet", ["2024-01-03", "2024-06-05", "2024-12-18"], {"rmse": 1.0})
    _write(tmp_path, "ens", ["2023-01-05", "2023-06-01", "2024-01-04", "2024-12-19"],
           {"rmse": 2.0})
    df = load_data(tmp_path)
    full, info = restrict_to_period(df, {"start": "2023-01-01", "end": "2024-12-31"})
    assert set(full["model"]) == {"ens"} and "glonet" in info["excluded"]   # 2024 only
    y2024, info = restrict_to_period(df, {"start": "2024-01-01", "end": "2024-12-31"})
    assert set(y2024["model"]) == {"ens", "glonet"} and not info["excluded"]
    assert pd.to_datetime(y2024["frt"]).min() >= pd.Timestamp("2024-01-01")


def test_the_page_has_one_view_per_period(tmp_path):
    from dcleaderboard.html_builder import generate_leaderboard_content
    _write(tmp_path, "glonet", ["2024-01-03", "2024-12-18"], {"rmse": 1.0})
    _write(tmp_path, "ens", ["2023-01-05", "2024-01-04", "2024-12-19"], {"rmse": 2.0})
    html = generate_leaderboard_content(tmp_path, {
        "common_period": True,
        "periods": [
            {"key": "2024", "label": "2024", "start": "2024-01-01", "end": "2024-12-31"},
            {"key": "all", "label": "2023-2024", "start": "2023-01-01", "end": "2024-12-31",
             "config": {"reference_model": "ens"}},
        ]})
    assert html.count('class="period-view"') == 2 and "dcShowPeriod('all')" in html
    v2024, vall = html.split('data-period="all" style=')[0], html.split('data-period="all" style=')[1]
    assert "glonet" in v2024 and "does not cover the period" in vall
    assert 'id="2024-' in html and 'id="all-' in html                       # anchors kept apart
