"""
Script to run the leaderboard generation locally using the current environment.
It uses the pure-Python builder and skips Poetry/Quarto checks.
"""
import sys
from pathlib import Path

# Add the parent directory to sys.path to ensure dcleaderboard is importable
current_file = Path(__file__).resolve()
repo_root = current_file.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from dcleaderboard.build import render_site_from_results_dir

def main():
    base_dir = current_file.parent
    results_dir = base_dir / "results"
    site_dir = base_dir / "_site"
    
    print(f"Starting local build...")
    print(f"Input: {results_dir}")
    print(f"Output: {site_dir}")
    
    try:
        render_site_from_results_dir(
            results_dir=results_dir, 
            output_site_dir=site_dir,
            template_dir=base_dir
        )
        print(f"Build successful! Open {site_dir}/leaderboard.html to view.")
    except Exception as e:
        print(f"Build failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
