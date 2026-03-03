"""
Module to generate the leaderboard HTML pages using pure Python.
Replaces the Quarto dependency while maintaining similar layout and styling.
Inspired by Google Research's WeatherBench design.
"""

import base64
import glob
import io
import re
import shutil
from pathlib import Path
from typing import List, Tuple, Any, Optional
import matplotlib.pyplot as plt
from dcleaderboard.processing import (
    create_legend_plot,
    generate_report_items,
    load_data,
)
from typing import Dict
from loguru import logger

def _merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            # Recurse for nested dicts
            result[key] = _merge_configs(result[key], value)
        else:
            # Overwrite for other types
            result[key] = value
    return result

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

# SVG icons used in the page
GITHUB_SVG = '<svg viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.65 7.65 0 0 1 2-.27c.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8z"/></svg>'
OCEAN_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 12c1.5-2 3.5-3 5-3s3.5 1 5 3c1.5 2 3.5 3 5 3s3.5-1 5-3"/><path d="M2 7c1.5-2 3.5-3 5-3s3.5 1 5 3c1.5 2 3.5 3 5 3s3.5-1 5-3"/><path d="M2 17c1.5-2 3.5-3 5-3s3.5 1 5 3c1.5 2 3.5 3 5 3s3.5-1 5-3"/></svg>'


def _slugify(text: str) -> str:
    """Convert text to a URL-friendly slug."""
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')


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
    return f"<p>{md_text}</p>" if not md_text.strip().startswith("<") else md_text


def build_navbar(active_page: str = "", config: Dict[str, Any] = None) -> str:
    """Build the modern Google Research-style navigation bar."""
    if config is None:
        config = {}
    
    brand_name = config.get("texts", {}).get("brand_name", "Ocean & Climate")
    brand_sub = config.get("texts", {}).get("brand_sub", "Data Challenge")
    github_url = config.get("texts", {}).get("github_url", "https://github.com/ppr-ocean-ia/dc-tools")
    
    active_home = "active" if active_page == "leaderboard" else ""
    active_maps = "active" if active_page == "maps" else ""
    active_about = "active" if active_page == "about" else ""
    
    return f"""
<nav class="navbar-main">
  <div class="navbar-inner">
    <a class="navbar-brand-area" href="leaderboard.html">
      <span class="brand-icon">DC</span>
      <span class="brand-text">{brand_name}</span>
      <span class="brand-sub">{brand_sub}</span>
    </a>
    <ul class="navbar-links">
      <li><a class="{active_home}" href="leaderboard.html">Leaderboard</a></li>
      <li><a class="{active_maps}" href="maps.html">Maps</a></li>
      <li><a class="{active_about}" href="about.html">About</a></li>
      <li>
        <a class="nav-icon" href="{github_url}" target="_blank" rel="noopener" title="GitHub">
          {GITHUB_SVG}
        </a>
      </li>
    </ul>
  </div>
</nav>
"""


def build_hero(config: Dict[str, Any]) -> str:
    """Build the hero section, inspired by WeatherBench."""
    texts = config.get("texts", {})
    page_title = texts.get(
        "page_title",
        "Data Challenge 2 Leaderboard"
    )
    hero_subtitle = texts.get(
        "hero_subtitle",
        "Probabilistic short-term forecasting of global ocean dynamics. "
        "An open benchmark for evaluating ML and physics-based ocean forecasting models."
    )
    github_url = texts.get("github_url", "https://github.com/ppr-ocean-ia/dc-tools")
    paper_url = texts.get("paper_url", "")
    docs_url = texts.get("docs_url", "")
    
    quick_links_html = []
    if github_url:
        quick_links_html.append(f'<a class="ql-primary" href="{github_url}" target="_blank">{GITHUB_SVG} Code</a>')
    if paper_url:
        quick_links_html.append(f'<a class="ql-outline" href="{paper_url}" target="_blank">Paper</a>')
    if docs_url:
        quick_links_html.append(f'<a class="ql-outline" href="{docs_url}" target="_blank">Documentation</a>')
    # Always add a link to the about page
    quick_links_html.append('<a class="ql-outline" href="about.html">About</a>')
    
    return f"""
<section class="hero">
  <div class="hero-inner">
    <h1>{page_title}</h1>
    <p class="hero-subtitle">{hero_subtitle}</p>
    <div class="quick-links">
      {''.join(quick_links_html)}
    </div>
  </div>
</section>
"""


def build_section_nav(sections: List[Tuple[str, str]]) -> str:
    """Build the section navigation pill area."""
    if not sections:
        return ""
    links = []
    for slug, label in sections:
        links.append(f'<a href="#{slug}">{label}</a>')
    return f"""
<div class="section-nav">
  <div class="section-nav-title">Jump to section</div>
  <div class="section-nav-links">
    {''.join(links)}
  </div>
</div>
"""


def build_head(title: str) -> str:
    """Build HTML head section."""
    return f"""<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link rel="stylesheet" href="styles.css">
</head>"""


def build_footer(config: Dict[str, Any] = None) -> str:
    """Build the footer section."""
    if config is None:
        config = {}
    texts = config.get("texts", {})
    github_url = texts.get("github_url", "https://github.com/ppr-ocean-ia/dc-tools")
    
    return f"""
<footer class="footer">
  <div class="footer-inner">
    <div class="footer-brand">
      <h3>Ocean &amp; Climate Data Challenge</h3>
      <p>An open benchmark for evaluating ocean forecasting models, maintained by the PPR Oc&eacute;an et Climat community.</p>
    </div>
    <div class="footer-links">
      <div class="footer-links-col">
        <h4>Resources</h4>
        <a href="leaderboard.html">Leaderboard</a>
        <a href="maps.html">Spatial Maps</a>
        <a href="about.html">About</a>
      </div>
      <div class="footer-links-col">
        <h4>Community</h4>
        <a href="{github_url}" target="_blank">GitHub</a>
        <a href="{github_url}/issues" target="_blank">Issues</a>
      </div>
    </div>
  </div>
  <div class="footer-bottom">
    PPR Oc&eacute;an et Climat &middot; Data Challenge Leaderboard
  </div>
</footer>
"""


def build_page(title: str, content: str, active_page: str = "", config: Dict[str, Any] = None, include_hero: bool = False) -> str:
    """Assemble the full HTML page."""
    if config is None:
        config = {}
    
    navbar = build_navbar(active_page, config)
    hero = build_hero(config) if include_hero else ""
    footer = build_footer(config)
    
    return f"""<!DOCTYPE html>
<html lang="en">
{build_head(title)}
<body>
{navbar}
{hero}
<div class="container content-area">
    {content}
</div>
{footer}
</body>
</html>
"""


def generate_leaderboard_content(results_dir: Path, config: Dict[str, Any]) -> str:
    """Generate the HTML content for the leaderboard page."""
    logger.info("Loading data for leaderboard...")
    df = load_data(results_dir)
    html_parts = []
    
    # Collect section anchors for nav
    sections: List[Tuple[str, str]] = []
    card_buffer: List[str] = []
    current_section_id = ""

    # First pass: collect section names
    for item_type, content in generate_report_items(df, config=config):
        if item_type == "markdown" and isinstance(content, str) and content.startswith("## "):
            label = content[3:].strip()
            slug = _slugify(label)
            sections.append((slug, label))

    # Section nav
    html_parts.append(build_section_nav(sections))

    # Second pass: build actual content
    for item_type, content in generate_report_items(df, config=config):
        if item_type == "markdown":
            if '<div style="height: 50px;"></div>' in content:
                if card_buffer:
                    html_parts.append(f'<div class="leaderboard-card" id="{current_section_id}">')
                    html_parts.extend(card_buffer)
                    html_parts.append('</div>')
                    card_buffer = []
            elif '<div style="height: 90px;"></div>' in content:
                # Skip the old spacer
                continue
            else:
                rendered = markdown_to_html(content)
                # Inject anchor ids for h2 section headers
                if content.startswith("## "):
                    label = content[3:].strip()
                    current_section_id = _slugify(label)
                card_buffer.append(rendered)
        elif item_type == "styler":
            card_buffer.append('<div class="table-responsive">')
            card_buffer.append(content.to_html())
            card_buffer.append('</div>')

    # Flush any remaining content
    if card_buffer:
        html_parts.append(f'<div class="leaderboard-card" id="{current_section_id}">')
        html_parts.extend(card_buffer)
        html_parts.append('</div>')
            
    # Legend Plot
    logger.info("Generating legend plot...")
    fig = create_legend_plot()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    
    html_parts.append(
        f'<div class="legend-section">\n'
        f'<div class="legend-container">\n'
        f'<span class="legend-title">Color Scale</span>\n'
        f'<img src="data:image/png;base64,{img_str}" style="max-width: 520px; height: auto;" />\n'
        f'</div></div>'
    )
    return "\n".join(html_parts)


def generate_about_content(config: Dict[str, Any] = None) -> str:
    """Generate the HTML content for the about page."""
    if config is None:
        config = {}
    texts = config.get("texts", {})
    github_url = texts.get("github_url", "https://github.com/ppr-ocean-ia/dc-tools")
    
    return f"""
<div class="leaderboard-card">
<h2>About this Leaderboard</h2>
<p>This leaderboard tracks the performance of various ocean forecasting models as part of the 
<strong>PPR Oc&eacute;an et Climat</strong> Data Challenge initiative.</p>

<h3>How it works</h3>
<p>Models are evaluated against reference satellite datasets using standard metrics. 
The tables show metric values for each model, with color coding indicating performance 
relative to the reference model: <strong style="color:#1a73e8;">blue = better</strong>, 
<strong style="color:#c53929;">red = worse</strong>.</p>

<h3>Participating</h3>
<p>Want to add your model? Check out the 
<a href="{github_url}" target="_blank">GitHub repository</a> for submission guidelines and evaluation code.</p>
</div>
"""


def build_site(
    output_dir: Path, 
    results_dir: Path, 
    styles_css: Path,
    custom_config: Optional[Dict[str, Any]] = None,
    site_base_url: str = "",
) -> None:
    """Build the complete static site."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Merge custom config with defaults
    if custom_config:
        config = _merge_configs(CUSTOM_CONFIG, custom_config)
    else:
        config = CUSTOM_CONFIG

    logger.info("Building site in {}", output_dir)
    
    # Copy CSS
    if styles_css.exists():
        shutil.copy(styles_css, output_dir / "styles.css")
        logger.debug("styles.css copied")
    else:
        logger.warning("{} not found", styles_css)

    # Build Leaderboard
    logger.info("Generating leaderboard.html")
    leaderboard_content = generate_leaderboard_content(results_dir, config)
    page_title = config.get("texts", {}).get("page_title", "Data Challenge 2 Leaderboard")
    leaderboard_html = build_page(
        page_title, 
        leaderboard_content, 
        active_page="leaderboard",
        config=config,
        include_hero=True,
    )
    with open(output_dir / "leaderboard.html", "w", encoding="utf-8") as f:
        f.write(leaderboard_html)

    # Build Maps page (if per_bins data exists)
    logger.info("Generating maps.html")
    try:
        from dcleaderboard.map_processing import preprocess_per_bins
        from dcleaderboard.map_builder import build_map_page

        map_metadata = preprocess_per_bins(results_dir, output_dir)
        if map_metadata:
            maps_html = build_map_page(
                metadata=map_metadata,
                config=config,
                build_head_fn=build_head,
                build_navbar_fn=build_navbar,
                build_footer_fn=build_footer,
                site_base_url=site_base_url,
            )
            with open(output_dir / "maps.html", "w", encoding="utf-8") as f:
                f.write(maps_html)
        else:
            logger.info("Skipping maps.html (no per-bins data)")
    except Exception as e:
        import traceback
        logger.warning("Could not generate maps page: {}", e)
        traceback.print_exc()

    # Build About
    logger.info("Generating about.html")
    about_content = generate_about_content(config)
    about_html = build_page(
        "About", 
        about_content, 
        active_page="about",
        config=config,
        include_hero=False,
    )
    with open(output_dir / "about.html", "w", encoding="utf-8") as f:
        f.write(about_html)
        
    logger.success("Site build complete!")
