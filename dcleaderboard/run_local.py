"""
Script to run the leaderboard generation locally using the current environment.
It uses the pure-Python builder and skips Poetry/Quarto checks.

Usage:
    python run_local.py [--config PATH/TO/leaderboard_config.yaml]

If --config is not provided, the script uses the bundled default config:
    dcleaderboard/config/leaderboard_texts.yaml
"""
import argparse
import sys
from pathlib import Path

# Add the parent directory to sys.path to ensure dcleaderboard is importable
current_file = Path(__file__).resolve()
repo_root = current_file.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from dcleaderboard.build import render_site_from_results_dir
from loguru import logger

# Default config bundled with the package (can be overridden via --config).
_DEFAULT_CONFIG = current_file.parent / "config" / "leaderboard_texts.yaml"


def main():
    parser = argparse.ArgumentParser(
        description="Build the DC Leaderboard HTML site locally."
    )
    parser.add_argument(
        "--config",
        metavar="FILE",
        default=None,
        help=(
            "Path to a YAML or JSON leaderboard config file. "
            f"Defaults to {_DEFAULT_CONFIG}."
        ),
    )
    args = parser.parse_args()

    base_dir = current_file.parent
    results_dir = base_dir / "results"
    site_dir = base_dir / "_site"

    # Resolve which config file to use.
    config_file: Path | None
    if args.config:
        config_file = Path(args.config)
    elif _DEFAULT_CONFIG.is_file():
        config_file = _DEFAULT_CONFIG
    else:
        config_file = None

    # Use empty base URL so map pages load grid data via relative paths.
    # XMLHttpRequest supports file:// with relative paths (unlike fetch()),
    # so no absolute file:// URL is needed.
    site_base_url = ""

    logger.info("Starting local build...")
    logger.info("Input:  {}", results_dir)
    logger.info("Output: {}", site_dir)
    if config_file:
        logger.info("Config: {}", config_file)

    try:
        render_site_from_results_dir(
            results_dir=results_dir,
            output_site_dir=site_dir,
            template_dir=base_dir,
            config_file=config_file,
            site_base_url=site_base_url,
        )
        logger.success("Build successful! Open {}/leaderboard.html to view.", site_dir)
    except Exception as e:
        logger.error("Build failed: {}", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
