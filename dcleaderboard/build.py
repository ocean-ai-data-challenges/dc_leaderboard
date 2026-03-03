from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import yaml
from loguru import logger

from dcleaderboard.html_builder import build_site


def _load_config_file(config_path: Path) -> dict:
    """Load a YAML or JSON leaderboard config file and return it as a dict."""
    try:
        if config_path.suffix.lower() in {".yaml", ".yml"}:
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        else:
            return json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to load config file {}: {}", config_path, exc)
        return {}


def _auto_detect_config(results_dir: Path) -> dict:
    """Look for leaderboard_config.yaml / .json in results_dir and its parent.

    Returns the merged config dict (empty dict if nothing found).
    File names checked (in order of preference):
      leaderboard_config.yaml, leaderboard_config.yml, leaderboard_config.json
    Search locations: results_dir itself, then results_dir.parent.
    """
    candidate_names = [
        "leaderboard_config.yaml",
        "leaderboard_config.yml",
        "leaderboard_config.json",
    ]
    for search_dir in (results_dir, results_dir.parent):
        for name in candidate_names:
            candidate = search_dir / name
            if candidate.is_file():
                logger.info("Auto-detected leaderboard config: {}", candidate)
                return _load_config_file(candidate)
    return {}


class BuildError(RuntimeError):
    pass


def clean_output_dir(output_dir: Path) -> None:
    """Remove all generated files from a previous build.

    This ensures every run starts from a clean state so that stale
    artefacts (map JS files, HTML pages, figures, etc.) from earlier
    runs never leak into the new build.
    """
    if not output_dir.exists():
        return

    removed: list[str] = []
    for child in list(output_dir.iterdir()):
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
        removed.append(child.name)

    if removed:
        logger.info("Cleaned {} items from {}", len(removed), output_dir)


@dataclass(frozen=True)
class RenderedSite:
    site_dir: Path
    leaderboard_html: Path
    about_html: Path | None


def render_site_from_results(
    *,
    results_files: Sequence[str | Path],
    output_site_dir: str | Path,
    template_dir: str | Path | None = None,
    include_benchmarks: bool = False,
    custom_config: dict | None = None,
    site_base_url: str = "",
) -> RenderedSite:
    """Render the website using pure Python.

    This uses dcleaderboard.html_builder to generate the site.
    """
    import tempfile

    output_site_dir = Path(output_site_dir).expanduser().resolve()

    # Always start from a clean output directory
    clean_output_dir(output_site_dir)
    
    # Locate styles.css
    # Priority: 1. template_dir/styles.css 2. package_dir/styles.css
    styles_css = None
    if template_dir:
        template_base = Path(template_dir).expanduser().resolve()
        if (template_base / "styles.css").exists():
            styles_css = template_base / "styles.css"
    
    if styles_css is None:
        # Fallback to package directory
        styles_css = Path(__file__).parent / "styles.css"

    if not styles_css.exists():
        logger.warning("styles.css not found at {}", styles_css)

    results_paths = [Path(p).expanduser().resolve() for p in results_files]
    
    if include_benchmarks:
        package_results_dir = Path(__file__).parent / "results"
        if package_results_dir.exists():
            benchmark_files = list(package_results_dir.glob("results_*.json"))
            # Avoid duplicates if the user already pointed to some of these
            existing_names = {p.name for p in results_paths}
            for bf in benchmark_files:
                if bf.name not in existing_names:
                    results_paths.append(bf)
        else:
            logger.warning("include_benchmarks=True but {} not found", package_results_dir)

    if not results_paths:
        raise BuildError("No results files provided")
    for p in results_paths:
        if not p.exists():
            raise BuildError(f"Results file not found: {p}")

    # Use a temp dir to aggregate results because build_site expects a directory
    with tempfile.TemporaryDirectory() as tmp:
        tmp_results_dir = Path(tmp) / "results"
        tmp_results_dir.mkdir()
        
        for src in results_paths:
            shutil.copy2(src, tmp_results_dir / src.name)

        # Also copy per-bins files (.jsonl and legacy .json) from the same
        # source directories so that map_processing.py can find them.
        source_dirs = {src.parent for src in results_paths}
        for src_dir in source_dirs:
            for pb_file in (
                list(src_dir.glob("*_per_bins.jsonl"))
                + list(src_dir.glob("*_per_bins.json"))
            ):
                dst = tmp_results_dir / pb_file.name
                if not dst.exists():
                    shutil.copy2(pb_file, dst)
            
        build_site(output_site_dir, tmp_results_dir, styles_css, custom_config, site_base_url=site_base_url)

    leaderboard_html = output_site_dir / "leaderboard.html"
    about_html = output_site_dir / "about.html"
    return RenderedSite(
        site_dir=output_site_dir,
        leaderboard_html=leaderboard_html,
        about_html=about_html if about_html.exists() else None,
    )


def render_site_from_results_dir(
    *,
    results_dir: str | Path,
    output_site_dir: str | Path,
    template_dir: str | Path | None = None,
    include_benchmarks: bool = False,
    custom_config: dict | None = None,
    config_file: str | Path | None = None,
    site_base_url: str = "",
) -> RenderedSite:
    """Render the website from a directory of JSON results.

    Configuration priority (highest wins):
    1. ``custom_config`` dict passed directly.
    2. ``config_file`` path (YAML or JSON).
    3. Auto-detected ``leaderboard_config.yaml`` / ``.json`` in *results_dir*
       or its parent directory.
    """
    results_dir = Path(results_dir).expanduser().resolve()
    if not results_dir.exists():
        raise BuildError(f"Results directory not found: {results_dir}")

    # --- Build the effective config ---
    # Start from file: explicit config_file > auto-detected file
    file_config: dict = {}
    if config_file is not None:
        file_config = _load_config_file(Path(config_file).expanduser().resolve())
    else:
        file_config = _auto_detect_config(results_dir)

    # Merge: file_config as base, custom_config overrides
    if file_config or custom_config:
        from dcleaderboard.html_builder import _merge_configs
        effective_config: dict | None = file_config
        if custom_config:
            effective_config = _merge_configs(file_config, custom_config)
    else:
        effective_config = None

    # Pass all json files in the directory that look like results
    result_files = list(results_dir.glob("results_*.json"))
    if not result_files:
        # Fallback to all json if strict naming isn't found
        result_files = [f for f in results_dir.glob("*.json")
                        if f.name != "leaderboard_config.json"]

    # If include_benchmarks is True, acceptable to have no files in users dir,
    # as long as benchmarks exist. But normally user wants to compare SOMETHING.
    # We defer the empty check to render_site_from_results which handles the merge.

    return render_site_from_results(
        results_files=result_files,
        output_site_dir=output_site_dir,
        template_dir=template_dir,
        include_benchmarks=include_benchmarks,
        custom_config=effective_config,
        site_base_url=site_base_url,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build DC Leaderboard HTML site")
    parser.add_argument("--results-dir", required=True, help="Directory containing results JSON files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the website")
    parser.add_argument("--template-dir", help="Directory containing styles.css (optional)")
    parser.add_argument(
        "--config",
        metavar="FILE",
        help="Path to a YAML or JSON leaderboard config file (overrides auto-detection)",
    )
    parser.add_argument(
        "--site-base-url",
        default="",
        help="Base URL for the generated site (default: empty = relative paths)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        outputs = render_site_from_results_dir(
            results_dir=args.results_dir,
            output_site_dir=args.output_dir,
            template_dir=args.template_dir,
            config_file=args.config,
            site_base_url=args.site_base_url,
        )
        logger.success("Site generated successfully.")
        logger.info("Leaderboard: {}", outputs.leaderboard_html)
        if outputs.about_html:
            logger.info("About: {}", outputs.about_html)
    except Exception as e:
        logger.error("Build failed: {}", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
