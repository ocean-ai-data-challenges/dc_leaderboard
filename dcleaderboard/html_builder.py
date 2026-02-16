"""
Module to generate the leaderboard HTML pages using pure Python.
Replaces the Quarto dependency while maintaining similar layout and styling.
"""

import base64
import glob
import io
import shutil
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
from dcleaderboard.processing import (
    create_legend_plot,
    generate_report_items,
    load_data,
)
from typing import Dict

# Constants matching Quarto 'flatly' theme
BOOTSTRAP_CSS = "https://cdn.jsdelivr.net/npm/bootswatch@5/dist/flatly/bootstrap.min.css"
BOOTSTRAP_JS = "https://cdn.jsdelivr.net/npm/bootstrap@5/dist/js/bootstrap.bundle.min.js"
BOOTSTRAP_ICONS = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"

# Custom Configuration from test_customization.py
CUSTOM_CONFIG: Dict[str, Dict[str, str]] = {
    # A. Metric Renaming
    "metrics_names": {
        "rmse": "Root Mean Squared Error (RMSE)",
        "rmsd": "Standard Deviation (RMSD)",
        "rmsd_geostrophic_currents": "RMSD Geostrophic Currents",
        # Metrics not listed here will keep their original name
    },
    # B. Variable Renaming (Alias for display in columns)
    "variables_names": {
        "Surface ssh": "Sea Surface Height (SSH)",
        "ssh": "Sea Surface Height (SSH)",  # Just in case
        "sst": "Sea Surface Temperature (SST)",
        "u": "Velocity U",
        "v": "Velocity V",
        "u_geostrophic": "Velocity U (Geo)",
        "v_geostrophic": "Velocity V (Geo)",
    },
    # C. Section Title Customization (Templates)
    "texts": {
        "reference_header": "## Reference Dataset: {ref_alias}",
        "metric_header": "### Metric: {metric_name}",
        "variable_group_header": "#### Variable Type: {var_type}",
    },
}

NAVBAR_HTML = """
<nav class="navbar navbar-expand-lg navbar-dark bg-primary">
  <div class="container-fluid">
    <span class="navbar-brand mb-0 h1">Ocean & Climate Data Challenge</span>
    <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarColor01" aria-controls="navbarColor01" aria-expanded="false" aria-label="Toggle navigation">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarColor01">
      <ul class="navbar-nav me-auto">
        <li class="nav-item">
          <a class="nav-link {active_home}" href="leaderboard.html">Home</a>
        </li>
        <li class="nav-item">
          <a class="nav-link {active_about}" href="about.html">About</a>
        </li>
      </ul>
      <ul class="navbar-nav ms-auto">
          <li class="nav-item compact">
            <a class="nav-link" href="https://github.com/ppr-ocean-ia/dc-tools">
              <i class="bi bi-github" role="img">
              </i> 
            </a>
          </li>
      </ul>
    </div>
  </div>
</nav>
"""


def markdown_to_html(md_text: str) -> str:
    """Basic Markdown -> HTML conversion for titles and formatting."""
    if md_text.startswith("#### "):
        return f"<h4>{md_text[5:]}</h4>"
    elif md_text.startswith("### "):
        return f"<h3>{md_text[4:]}</h3>"
    elif md_text.startswith("## "):
        return f"<h2>{md_text[3:]}</h2>"
    elif md_text.startswith("*") and md_text.endswith("*"):
        return f"<i>{md_text[1:-1]}</i>"
    # Basic paragraph fallback if not a header
    # Revert to raw text if it's not a header to match legacy behavior if needed, 
    # but <p> is generally safer. However, preserving <p> wrap for non-layout elements.
    # Check if we should avoid <p>? Layout divs start with <div...
    return f"<p>{md_text}</p>" if not md_text.strip().startswith("<") else md_text


def build_head(title: str) -> str:
    """Build HTML head section."""
    return f"""<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="stylesheet" href="{BOOTSTRAP_CSS}">
    <link rel="stylesheet" href="{BOOTSTRAP_ICONS}">
    <link rel="stylesheet" href="styles.css">
    <style>
        body {{ padding-top: 0; }}
        /* Table overrides for cleaner look */
        .dataframe {{ 
            width: 100% !important;
            margin-bottom: 2rem;
        }}
    </style>
</head>"""


def build_page(title: str, content: str, active_page: str = "") -> str:
    """Assemble the full HTML page."""
    active_home = "active" if active_page == "leaderboard" else ""
    active_about = "active" if active_page == "about" else ""
    
    navbar = NAVBAR_HTML.format(active_home=active_home, active_about=active_about)
    
    return f"""<!DOCTYPE html>
<html lang="en">
{build_head(title)}
<body>
{navbar}
<div class="container" style="padding-top: 40px; padding-bottom: 60px;">
    {content}
</div>
<script src="{BOOTSTRAP_JS}"></script>
</body>
</html>
"""


def generate_leaderboard_content(results_dir: Path) -> str:
    """Generate the HTML content for the leaderboard page."""
    print("Loading data for leaderboard...")
    df = load_data(results_dir)
    html_parts = []
    
    # Title matching leaderboard.qmd
    html_parts.append('<h1 class="title">Data Challenge 2 Leaderboard (Probabilistic short-term forecasting of global ocean dynamics)</h1>')
    
    # Buffer for card content
    card_buffer = []

    for item_type, content in generate_report_items(df, config=CUSTOM_CONFIG):
        if item_type == "markdown":
            # Check for spacer div which indicates end of section
            if '<div style="height: 50px;"></div>' in content:
                if card_buffer:
                    html_parts.append('<div class="leaderboard-card">')
                    html_parts.extend(card_buffer)
                    html_parts.append('</div>')
                    card_buffer = []
            else:
                card_buffer.append(markdown_to_html(content))
        elif item_type == "styler":
            card_buffer.append('<div class="table-responsive">')
            card_buffer.append(content.to_html())
            card_buffer.append('</div>')

    # Flush any remaining content
    if card_buffer:
        html_parts.append('<div class="leaderboard-card">')
        html_parts.extend(card_buffer)
        html_parts.append('</div>')
            
    # Legend Plot
    print("Generating legend plot...")
    fig = create_legend_plot()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    
    html_parts.append(
        f'<div style="display: flex; justify-content: center; margin-bottom: 50px;">\n'
        f'<div class="legend-container">\n'
        f'<img src="data:image/png;base64,{img_str}" style="max-width: 100%; height: auto;" />\n'
        f'</div></div>'
    )
    return "\n".join(html_parts)


def generate_about_content() -> str:
    """Generate the HTML content for the about page."""
    # Content from about.qmd
    return """
<h1 class="title">About</h1>
<p>Leaderboard des Data Challenges du PPR Océan et Climat</p>
"""


def build_site(output_dir: Path, results_dir: Path, styles_css: Path) -> None:
    """Build the complete static site."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Building site in {output_dir}")
    
    # Copy CSS
    if styles_css.exists():
        shutil.copy(styles_css, output_dir / "styles.css")
        print("checkmark CSS copied")
    else:
        print(f"Warning: {styles_css} not found")

    # Build Leaderboard
    print("Generating leaderboard.html")
    leaderboard_content = generate_leaderboard_content(results_dir)
    leaderboard_html = build_page(
        "Data Challenge 2 Leaderboard", 
        leaderboard_content, 
        active_page="leaderboard"
    )
    with open(output_dir / "leaderboard.html", "w", encoding="utf-8") as f:
        f.write(leaderboard_html)

    # Build About
    print("Generating about.html")
    about_content = generate_about_content()
    about_html = build_page("About", about_content, active_page="about")
    with open(output_dir / "about.html", "w", encoding="utf-8") as f:
        f.write(about_html)
        
    print("Site build complete!")
