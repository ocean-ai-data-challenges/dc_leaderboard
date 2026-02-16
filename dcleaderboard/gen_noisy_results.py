#!/usr/bin/env python3
"""
Utility script to generate noisy results for testing/demonstration purposes.

This script takes an existing JSON result file, adds Gaussian noise to the metric values,
changes the model name, and saves it as a new file.
"""

import json
import sys
from typing import Any, Dict, List

import numpy as np


def add_noise(obj: Any, std_rel: float) -> Any:
    """Add Gaussian noise to numerical values in a JSON structure."""
    if isinstance(obj, dict):
        return {k: add_noise(v, std_rel) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [add_noise(v, std_rel) for v in obj]
    elif isinstance(obj, (int, float)):
        noise = np.random.normal(0, abs(obj) * std_rel)
        return float(round(obj + noise, 8))
    else:
        return obj


def process_file(src_path: str, dst_path: str, new_model: str, std_rel: float = 0.05) -> None:
    """Read a JSON file, add noise to results, and save to a new file."""
    with open(src_path) as f:
        data = json.load(f)

    # Replace dataset name
    data["dataset"] = new_model

    # Replace model name in all entries
    if "results" in data and isinstance(data["results"], dict):
        # Retrieve all entries regardless of the old model name
        all_entries: List[Dict[str, Any]] = []
        for entries in data["results"].values():
            all_entries.extend(entries)

        # Modify the "model" field in each entry
        for entry in all_entries:
            entry["model"] = new_model
            if "result" in entry:
                entry["result"] = add_noise(entry["result"], std_rel)

        # Replace the key in "results" with the new name
        data["results"] = {new_model: all_entries}

    with open(dst_path, "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python gen_noisy_results.py <src.json> <dst.json> <new_model> [std_rel]")
        sys.exit(1)

    src, dst, new_model_arg = sys.argv[1:4]
    std_rel_arg = float(sys.argv[4]) if len(sys.argv) > 4 else 0.05

    np.random.seed(42)
    process_file(src, dst, new_model_arg, std_rel_arg)

    # Example usage:
    # python gen_noisy_results.py results/results_glonet.json \
    #      results/results_challenger_model_1.json challenger_model_1 0.08
