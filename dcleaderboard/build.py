from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from dcleaderboard.html_builder import build_site


class BuildError(RuntimeError):
    pass


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
) -> RenderedSite:
    """Render the website using pure Python.

    This uses dcleaderboard.html_builder to generate the site.
    """
    import shutil
    import tempfile

    output_site_dir = Path(output_site_dir).expanduser().resolve()
    
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
        # warning or error? Let's warn but continue if possible (though html_builder needs it)
        print(f"Warning: styles.css not found at {styles_css}", file=sys.stderr)

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
            print(f"Warning: include_benchmarks=True but {package_results_dir} not found", file=sys.stderr)

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
            
        build_site(output_site_dir, tmp_results_dir, styles_css)

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
) -> RenderedSite:
    """Render the website from a directory of JSON results."""
    results_dir = Path(results_dir).expanduser().resolve()
    if not results_dir.exists():
        raise BuildError(f"Results directory not found: {results_dir}")

    # Pass all json files in the directory that look like results
    result_files = list(results_dir.glob("results_*.json"))
    if not result_files:
         # Fallback to all json if strict naming isn't found
         result_files = list(results_dir.glob("*.json"))
    
    # If include_benchmarks is True, acceptable to have no files in users dir, 
    # as long as benchmarks exist. But normally user wants to compare SOMETHING.
    # We defer the empty check to render_site_from_results which handles the merge.

    return render_site_from_results(
        results_files=result_files,
        output_site_dir=output_site_dir,
        template_dir=template_dir,
        include_benchmarks=include_benchmarks,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build DC Leaderboard HTML site")
    parser.add_argument("--results-dir", required=True, help="Directory containing results JSON files")
    parser.add_argument("--output-dir", required=True, help="Output directory for the website")
    parser.add_argument("--template-dir", help="Directory containing styles.css (optional)")

    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        outputs = render_site_from_results_dir(
            results_dir=args.results_dir,
            output_site_dir=args.output_dir,
            template_dir=args.template_dir,
        )
        print("Site generated successfully.")
        print(f"  Leaderboard: {outputs.leaderboard_html}")
        if outputs.about_html:
            print(f"  About: {outputs.about_html}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
