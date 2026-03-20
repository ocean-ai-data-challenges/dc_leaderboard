"""
Module to generate the interactive map HTML page.

Uses Leaflet.js for the base map and canvas overlays for grid data.
Loads pre-aggregated JSON files produced by map_processing.py.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def generate_map_page_content(metadata: Dict[str, Any], site_base_url: str = "") -> str:
    """Generate the HTML content (inside the container) for the map page.

    Parameters
    ----------
    metadata : dict
        Map metadata produced by map_processing.
    site_base_url : str
        Base URL (or local path) to the _site directory.  Used to build the
        fetch URL for grid JSON files.  Should end with ``/`` when non-empty.
        An empty string means relative paths (works when served by HTTP).
    """

    models = metadata["models"]
    variables = metadata["variables"]
    metrics = metadata["metrics"]
    lead_times = metadata["lead_times"]
    depth_bins = metadata.get("depth_bins", {})
    ref_variables = metadata.get("ref_variables", {})

    # Build option lists
    model_options = "\n".join(
        f'<option value="{m}">{m}</option>' for m in models
    )
    # Reference dataset options – sorted ref names
    ref_options = "\n".join(
        f'<option value="{r}">{r.replace("_", " ").title()}</option>'
        for r in sorted(ref_variables.keys())
    )
    variable_options = "\n".join(
        f'<option value="{v}">{v.replace("_", " ").title()}</option>' for v in variables
    )
    metric_options = "\n".join(
        f'<option value="{m}">{m.upper()}</option>' for m in metrics
    )
    lead_option_parts = []
    for lt in lead_times:
        if lt == "all":
            lead_option_parts.append('<option value="all">All days (composite)</option>')
        else:
            lead_option_parts.append(f'<option value="{lt}">Day {lt + 1}</option>')
    lead_options = "\n".join(lead_option_parts)

    # Depth bins as JS object
    depth_bins_js = "{"
    for var, dbs in depth_bins.items():
        labels = [f'"{d[0]:.1f}-{d[1]:.1f}"' for d in dbs]
        depth_bins_js += f'"{var}": [{", ".join(labels)}], '
    depth_bins_js += "}"

    # Ref variables as JS object: { ref_alias: [var1, var2, ...], ... }
    import json as _json
    ref_variables_js = _json.dumps(ref_variables, sort_keys=True)
    # Ref type map: { ref_alias: "gridded"|"observation" }
    ref_type_map = metadata.get("ref_type_map", {})
    ref_type_map_js = _json.dumps(ref_type_map, sort_keys=True)

    # Colormap — sampled from matplotlib at build time.
    # Change CMAP_NAME to switch palettes (e.g. "cmocean.cm.balance",
    # "RdBu_r", "coolwarm", "viridis", …).
    import matplotlib.cm as _mcm
    _CMAP_NAME = "RdYlBu_r"  # standard in physical oceanography
    _cmap_obj = _mcm.get_cmap(_CMAP_NAME)
    _N_STOPS = 16
    _cmap_stops = [
        f"[{int(r*255)}, {int(g*255)}, {int(b*255)}]"
        for r, g, b, _ in (_cmap_obj(i / (_N_STOPS - 1)) for i in range(_N_STOPS))
    ]
    cmap_js_array = ",\n    ".join(_cmap_stops)
    cmap_name_label = _CMAP_NAME

    return f"""
<div class="map-page">
  <div class="map-controls">
    <div class="control-group">
      <label for="select-model">Model</label>
      <select id="select-model">{model_options}</select>
    </div>
    <div class="control-group">
      <label for="select-ref">Reference Dataset</label>
      <select id="select-ref">{ref_options}</select>
    </div>
    <div class="control-group">
      <label for="select-variable">Variable</label>
      <select id="select-variable">{variable_options}</select>
    </div>
    <div class="control-group">
      <label for="select-metric">Metric</label>
      <select id="select-metric">{metric_options}</select>
    </div>
    <div class="control-group">
      <label for="select-lead">Lead Day</label>
      <select id="select-lead">{lead_options}</select>
    </div>
    <div class="control-group" id="depth-group" style="display:none;">
      <label for="select-depth">Depth</label>
      <select id="select-depth">
        <option value="all_depths">All depths (avg)</option>
      </select>
    </div>
  </div>

  <div class="map-status" id="map-status">Loading…</div>

  <div id="map-container">
    <div id="map"></div>
    <div id="map-colorbar">
      <div id="colorbar-gradient"></div>
      <div id="colorbar-labels">
        <span id="cb-min"></span>
        <span id="cb-max"></span>
      </div>
      <div id="cb-title"></div>
    </div>
  </div>
</div>

<!-- Leaflet CSS & JS -->
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<!-- TopoJSON client for land mask -->
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>

<script>
(function() {{
  'use strict';

  // --- Config ---
  const DEPTH_BINS = {depth_bins_js};
  const REF_VARIABLES = {ref_variables_js};
  const REF_TYPE_MAP = {ref_type_map_js};
  let map, gridLayer, currentData = null;

  // --- Color scale ({cmap_name_label}) ---
  // Generated at build time from matplotlib.  Low values → blue, high → red,
  // which is the standard convention in physical-oceanography publications.
  const CMAP = [
    {cmap_js_array}
  ];

  function interpolateColor(t) {{
    t = Math.max(0, Math.min(1, t));
    const idx = t * (CMAP.length - 1);
    const i = Math.floor(idx);
    const f = idx - i;
    if (i >= CMAP.length - 1) return CMAP[CMAP.length - 1];
    const c0 = CMAP[i], c1 = CMAP[i + 1];
    return [
      Math.round(c0[0] + (c1[0] - c0[0]) * f),
      Math.round(c0[1] + (c1[1] - c0[1]) * f),
      Math.round(c0[2] + (c1[2] - c0[2]) * f)
    ];
  }}

  function colorToCSS(rgb, alpha) {{
    return `rgba(${{rgb[0]}},${{rgb[1]}},${{rgb[2]}},${{alpha || 0.75}})`;
  }}

  // --- Map init ---
  function initMap() {{
    // 85.05° is the Web-Mercator limit; keep a small margin.
    var BOUNDS = L.latLngBounds([[-85, -180], [85, 180]]);

    map = L.map('map', {{
      center: [20, 0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 10,
      worldCopyJump: false,
      preferCanvas: true,
      maxBounds: BOUNDS,
      maxBoundsViscosity: 1.0
    }});

    // Fit the map so the whole world is visible within the container.
    map.fitBounds(BOUNDS, {{ animate: false }});

    // Pane z-ordering:
    //   tilePane   (z=200, built-in) – CartoDB dark_matter base
    //   gridPane   (z=250)           – semi-transparent data colour cells
    //   labelsPane (z=400)           – coast/border lines + labels on top
    map.createPane('gridPane');
    map.getPane('gridPane').style.zIndex = 250;

    map.createPane('labelsPane');
    map.getPane('labelsPane').style.zIndex = 400;
    map.getPane('labelsPane').style.pointerEvents = 'none';

    // Layer 1 – base: CartoDB dark_matter_no_labels.
    // Land ≈ dark grey (#262626), ocean ≈ very dark navy (#0d1420).
    // These two shades show clearly through the semi-transparent grid below.
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_matter_no_labels/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 19,
      noWrap: true,
      bounds: BOUNDS
    }}).addTo(map);

    // Layer 2 – data grid (rendered at ~0.75 opacity so the base-map
    // land/ocean shades remain visible underneath).
    gridLayer = L.layerGroup({{ pane: 'gridPane' }}).addTo(map);

    // Pane for opaque land-mask polygons drawn ABOVE the data grid.
    map.createPane('landPane');
    map.getPane('landPane').style.zIndex = 300;
    map.getPane('landPane').style.pointerEvents = 'none';

    // Layer 3 – vector land mask (z=300, above data grid z=250).
    // Opaque dark polygons hide the data on land areas so continents
    // appear clearly even when a full-coverage grid is rendered.
    loadLandMask();

    // Layer 4 – CartoDB dark_only_labels drawn ON TOP of everything.
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_only_labels/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      attribution: '',
      subdomains: 'abcd',
      maxZoom: 19,
      pane: 'labelsPane',
      noWrap: true,
      bounds: BOUNDS
    }}).addTo(map);
  }}

  // ---------------------------------------------------------------------------
  // Land-mask helpers
  // ---------------------------------------------------------------------------

  // Returns true if any two consecutive ring vertices differ by >180° in lon
  // (= the ring crosses the antimeridian in its raw GeoJSON coordinates).
  function _ringCrossesAntimeridian(ring) {{
    for (var i = 1; i < ring.length; i++) {{
      if (Math.abs(ring[i][0] - ring[i - 1][0]) > 180) return true;
    }}
    return false;
  }}

  function _polyHasAntimeridian(rings) {{
    for (var i = 0; i < rings.length; i++) {{
      if (_ringCrossesAntimeridian(rings[i])) return true;
    }}
    return false;
  }}

  // Clamp latitude to Web-Mercator limits so Leaflet doesn't distort poles.
  function _clampLat(ring) {{
    return ring.map(function(v) {{
      return [v[0], Math.max(-84.5, Math.min(84.5, v[1]))];
    }});
  }}

  // Unwrap a ring so that consecutive vertices never differ by more than 180°
  // in longitude.  The resulting coordinates may go outside [-180, 180] but
  // form a continuous, non-jumping polygon that Leaflet can render without the
  // full-width horizontal artifact.
  function _unwrapRing(ring) {{
    if (ring.length === 0) return ring;
    var result = [[ring[0][0], ring[0][1]]];
    for (var i = 1; i < ring.length; i++) {{
      var prev = result[i - 1][0];
      var lon = ring[i][0];
      while (lon - prev > 180) lon -= 360;
      while (prev - lon > 180) lon += 360;
      result.push([lon, ring[i][1]]);
    }}
    return result;
  }}

  // For polygons that cross the antimeridian (e.g. Russia, Antarctica):
  // unwrap all rings to make them continuous, then shift the whole polygon so
  // that its centroid longitude sits inside [-180, 180].  The part that still
  // extends beyond ±180° simply falls outside the map bounds and is invisible,
  // while the main visible bulk of the polygon is rendered correctly.
  function _normalizeAntimeridianPoly(rings) {{
    var unwrapped = rings.map(_unwrapRing);
    // Compute centroid longitude from the outer ring
    var sum = 0;
    unwrapped[0].forEach(function(v) {{ sum += v[0]; }});
    var centerLon = sum / unwrapped[0].length;
    // Shift so centroid is in [-180, 180]
    var shift = 0;
    if (centerLon > 180) shift = -360;
    else if (centerLon < -180) shift = 360;
    if (shift !== 0) {{
      unwrapped = unwrapped.map(function(ring) {{
        return ring.map(function(v) {{ return [v[0] + shift, v[1]]; }});
      }});
    }}
    return unwrapped;
  }}

  // Return a FeatureCollection of individual Polygons ready for Leaflet.
  // Antimeridian-crossing polygons are unwrapped (not skipped) so that
  // countries like Russia appear correctly – only the small part of their
  // ring that extends beyond the visible ±180° bound is lost.
  function _safePolygons(geojson) {{
    var features = [];
    var src = geojson.type === 'FeatureCollection' ? geojson.features : [geojson];
    src.forEach(function(feat) {{
      var geom = feat.geometry || feat;
      if (geom.type === 'Polygon') {{
        var coords = _polyHasAntimeridian(geom.coordinates)
          ? _normalizeAntimeridianPoly(geom.coordinates)
          : geom.coordinates;
        features.push({{ type: 'Feature', geometry: {{
          type: 'Polygon',
          coordinates: coords.map(_clampLat)
        }}, properties: {{}} }});
      }} else if (geom.type === 'MultiPolygon') {{
        geom.coordinates.forEach(function(polyRings) {{
          var coords = _polyHasAntimeridian(polyRings)
            ? _normalizeAntimeridianPoly(polyRings)
            : polyRings;
          features.push({{ type: 'Feature', geometry: {{
            type: 'Polygon',
            coordinates: coords.map(_clampLat)
          }}, properties: {{}} }});
        }});
      }}
    }});
    return {{ type: 'FeatureCollection', features: features }};
  }}

  function loadLandMask() {{
    fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json')
      .then(function(r) {{ return r.json(); }})
      .then(function(world) {{
        var countries = topojson.feature(world, world.objects.countries);
        var safe = _safePolygons(countries);
        L.geoJSON(safe, {{
          pane: 'landPane',
          style: {{ fillColor: '#1e1e2e', fillOpacity: 1, color: '#3a3a5c', weight: 0.6 }},
          interactive: false
        }}).addTo(map);
      }})
      .catch(function(e) {{ console.warn('Land mask failed to load:', e); }});
  }}

  // --- Variable selector update (filtered by reference dataset) ---
  function updateVariableSelector() {{
    const ref = document.getElementById('select-ref').value;
    const varSelect = document.getElementById('select-variable');
    const previousValue = varSelect.value;

    // Determine which variables to show
    const allowed = REF_VARIABLES[ref] || [];

    varSelect.innerHTML = '';
    allowed.forEach(function(v) {{
      const opt = document.createElement('option');
      opt.value = v;
      opt.textContent = v.replace(/_/g, ' ').replace(/\\b\\w/g, function(c) {{ return c.toUpperCase(); }});
      varSelect.appendChild(opt);
    }});

    // Try to keep previous selection if still available
    if (allowed.indexOf(previousValue) >= 0) {{
      varSelect.value = previousValue;
    }}

    // Also update depth selector for the (possibly changed) variable
    updateDepthSelector();
  }}

  // --- Depth selector update ---
  function updateDepthSelector() {{
    const variable = document.getElementById('select-variable').value;
    const depthGroup = document.getElementById('depth-group');
    const depthSelect = document.getElementById('select-depth');

    if (DEPTH_BINS[variable] && DEPTH_BINS[variable].length > 0) {{
      depthGroup.style.display = '';
      depthSelect.innerHTML = '<option value="all_depths">All depths (avg)</option>';
      DEPTH_BINS[variable].forEach(function(d) {{
        const opt = document.createElement('option');
        opt.value = d;
        opt.textContent = d + ' m';
        depthSelect.appendChild(opt);
      }});
    }} else {{
      depthGroup.style.display = 'none';
    }}
  }}

  // --- Build data key ---
  // The lookup tries three key formats in order:
  //   1. model|ref_alias|variable|metric|lead   (current format, one file per ref)
  //   2. model|ref_type|variable|metric|lead    (legacy format, shared gridded/observation)
  //   3. model|variable|metric|lead             (oldest format, no ref at all)
  function _buildKey(refSegment, depth) {{
    const model    = document.getElementById('select-model').value;
    const variable = document.getElementById('select-variable').value;
    const metric   = document.getElementById('select-metric').value;
    const lead     = document.getElementById('select-lead').value;
    let key = refSegment ? model + '|' + refSegment + '|' + variable + '|' + metric + '|' + lead
                         : model + '|' + variable + '|' + metric + '|' + lead;
    if (depth) key += '|' + depth;
    return key;
  }}

  function _currentDepth() {{
    const variable = document.getElementById('select-variable').value;
    const hasDepth = DEPTH_BINS[variable] && DEPTH_BINS[variable].length > 0;
    return hasDepth ? document.getElementById('select-depth').value : null;
  }}

  function getDataKey() {{
    const ref = document.getElementById('select-ref').value;
    return _buildKey(ref, _currentDepth());
  }}

  // Fallback 1: use ref_type (gridded / observation) – matches map_data files
  // generated before per-ref splitting was introduced.
  function getDataKeyRefType() {{
    const ref     = document.getElementById('select-ref').value;
    const refType = REF_TYPE_MAP[ref] || 'gridded';
    return _buildKey(refType, _currentDepth());
  }}

  // Fallback 2: no ref segment at all (oldest data format).
  function getDataKeyFallback() {{
    return _buildKey(null, _currentDepth());
  }}

  const SITE_BASE_URL = '{site_base_url}';

  function getFilename(key) {{
    return SITE_BASE_URL + 'map_data/' + key.replace(/\\|/g, '_').replace(/ /g, '_') + '.js';
  }}

  // --- Render latitude-band data ---
  // Each record is [lat_south, lat_north, value].
  // We draw a full-width rectangle (-180 → 180) for each band.
  // The dark base-map tiles show through for land, so bands
  // appear to cover only ocean areas.
  function renderLatBands(data, vmin, vmax) {{
    gridLayer.clearLayers();

    if (!data || data.length === 0) {{
      document.getElementById('map-status').textContent = 'No data for this combination.';
      return;
    }}

    const range = vmax - vmin || 1;

    data.forEach(function(band) {{
      const south = band[0], north = band[1], val = band[2];
      const t = (val - vmin) / range;
      const color = colorToCSS(interpolateColor(t), 0.75);

      const bounds = [[south, -180], [north, 180]];
      const rect = L.rectangle(bounds, {{
        pane: 'gridPane',
        color: 'rgba(255,255,255,0.15)',
        weight: 0.5,
        fillColor: color,
        fillOpacity: 0.75,
        interactive: true
      }});
      rect.bindTooltip(
        `<b>${{val.toFixed(4)}}</b><br>${{south.toFixed(0)}}&deg; – ${{north.toFixed(0)}}&deg;`,
        {{ sticky: true, className: 'grid-tooltip' }}
      );
      gridLayer.addLayer(rect);
    }});

    updateColorbar(vmin, vmax);
    document.getElementById('map-status').textContent =
      data.length + ' latitude bands loaded.';
  }}

  // --- Render spatial (lat/lon) grid ---
  // Each record is [lat_south, lat_north, lon_west, lon_east, value].
  // We draw properly-sized L.rectangle for each cell using the
  // canvas renderer for performance (can be >60 000 cells at 1° res).
  function renderSpatialGrid(data, vmin, vmax) {{
    gridLayer.clearLayers();

    if (!data || data.length === 0) {{
      document.getElementById('map-status').textContent = 'No data for this combination.';
      return;
    }}

    const range = vmax - vmin || 1;
    const canvasRenderer = L.canvas({{ pane: 'gridPane', padding: 0.5 }});

    data.forEach(function(cell) {{
      const latS = cell[0], latN = cell[1], lonW = cell[2], lonE = cell[3], val = cell[4];
      const t = (val - vmin) / range;
      const color = colorToCSS(interpolateColor(t), 0.75);

      const bounds = [[latS, lonW], [latN, lonE]];
      const rect = L.rectangle(bounds, {{
        pane: 'gridPane',
        renderer: canvasRenderer,
        color: 'none',
        fillColor: color,
        fillOpacity: 0.75,
        weight: 0,
        interactive: true
      }});
      rect.bindTooltip(
        `<b>${{val.toFixed(4)}}</b><br>Lat: ${{latS.toFixed(1)}}&deg;\u2013${{latN.toFixed(1)}}&deg;<br>Lon: ${{lonW.toFixed(1)}}&deg;\u2013${{lonE.toFixed(1)}}&deg;`,
        {{ sticky: true, className: 'grid-tooltip' }}
      );
      gridLayer.addLayer(rect);
    }});

    updateColorbar(vmin, vmax);
    document.getElementById('map-status').textContent =
      data.length + ' grid cells loaded.';
  }}

  // --- Render observation points ---
  // Each record is [lat_center, lon_center, value].
  // Used for in-situ (Argo) and satellite along-track (SARAL, Jason-3, SWOT)
  // data where the sparse, track-like coverage must stay visually distinct
  // from dense model grids.  Points are drawn as SVG circle markers that
  // change radius with zoom level so they feel like real measurement locations.
  function renderPoints(data, vmin, vmax) {{
    gridLayer.clearLayers();

    if (!data || data.length === 0) {{
      document.getElementById('map-status').textContent = 'No data for this combination.';
      return;
    }}

    const range = vmax - vmin || 1;
    const currentZoom = map.getZoom();
    // Radius grows with zoom so points stay legible but don't swamp the view.
    const radius = Math.max(2, Math.min(10, currentZoom - 1));

    data.forEach(function(pt) {{
      const lat = pt[0], lon = pt[1], val = pt[2];
      const t = (val - vmin) / range;
      const color = colorToCSS(interpolateColor(t), 0.85);

      const marker = L.circleMarker([lat, lon], {{
        pane: 'gridPane',
        radius: radius,
        color: 'rgba(255,255,255,0.4)',
        weight: 0.8,
        fillColor: color,
        fillOpacity: 0.85,
        interactive: true
      }});
      marker.bindTooltip(
        `<b>${{val.toFixed(4)}}</b><br>Lat: ${{lat.toFixed(2)}}&deg;<br>Lon: ${{lon.toFixed(2)}}&deg;`,
        {{ sticky: true, className: 'grid-tooltip' }}
      );
      gridLayer.addLayer(marker);
    }});

    // Resize circles whenever the user zooms.
    map.off('zoomend', renderPoints._zoomHandler);
    renderPoints._zoomHandler = function() {{
      const z = map.getZoom();
      const r = Math.max(2, Math.min(10, z - 1));
      gridLayer.eachLayer(function(layer) {{
        if (layer.setRadius) layer.setRadius(r);
      }});
    }};
    map.on('zoomend', renderPoints._zoomHandler);

    updateColorbar(vmin, vmax);
    document.getElementById('map-status').textContent =
      data.length + ' observation points loaded.';
  }}

  // --- Render observation latitude bands ---
  // Same data format as renderLatBands ([south, north, value]) but with a
  // visually distinct style: thinner bands (typically 1° wide from altimetry
  // or in-situ data) rendered with a white edge so individual stripes are
  // readable.  Status text labels them as "observation" data.
  function renderObsLatBands(data, vmin, vmax) {{
    gridLayer.clearLayers();

    if (!data || data.length === 0) {{
      document.getElementById('map-status').textContent = 'No data for this combination.';
      return;
    }}

    const range = vmax - vmin || 1;

    data.forEach(function(band) {{
      const south = band[0], north = band[1], val = band[2];
      const t = (val - vmin) / range;
      const color = colorToCSS(interpolateColor(t), 0.80);

      const bounds = [[south, -180], [north, 180]];
      const rect = L.rectangle(bounds, {{
        pane: 'gridPane',
        color: 'rgba(255,255,255,0.55)',
        weight: 0.8,
        fillColor: color,
        fillOpacity: 0.80,
        interactive: true
      }});
      rect.bindTooltip(
        `<b>${{val.toFixed(4)}}</b><br>${{south.toFixed(1)}}&deg; \u2013 ${{north.toFixed(1)}}&deg; <i>(obs.)</i>`,
        {{ sticky: true, className: 'grid-tooltip' }}
      );
      gridLayer.addLayer(rect);
    }});

    updateColorbar(vmin, vmax);
    document.getElementById('map-status').textContent =
      data.length + ' observation latitude bands loaded.';
  }}

  // --- Render: pick strategy based on grid_type ---
  function renderGrid(json) {{
    const data = json.data, vmin = json.vmin, vmax = json.vmax;
    if (json.grid_type === 'lat_band') {{
      renderLatBands(data, vmin, vmax);
    }} else if (json.grid_type === 'lat_band_obs') {{
      renderObsLatBands(data, vmin, vmax);
    }} else if (json.grid_type === 'points') {{
      renderPoints(data, vmin, vmax);
    }} else {{
      renderSpatialGrid(data, vmin, vmax);
    }}
  }}

  // --- Colorbar ---
  function updateColorbar(vmin, vmax) {{
    const gradient = document.getElementById('colorbar-gradient');
    const stops = [];
    for (let i = 0; i <= 10; i++) {{
      const t = i / 10;
      const c = interpolateColor(t);
      stops.push(`rgb(${{c[0]}},${{c[1]}},${{c[2]}}) ${{t * 100}}%`);
    }}
    gradient.style.background = `linear-gradient(to right, ${{stops.join(', ')}})`;

    const metric = document.getElementById('select-metric').value;
    document.getElementById('cb-min').textContent = vmin.toFixed(4);
    document.getElementById('cb-max').textContent = vmax.toFixed(4);
    document.getElementById('cb-title').textContent = metric.toUpperCase();
    document.getElementById('map-colorbar').style.display = 'flex';
  }}

  // --- JSONP data loader ---
  // We use dynamic <script> tags instead of fetch/XHR because
  // those are blocked by CORS for file:// URLs in all modern
  // browsers.  <script src="..."> is the only method that works
  // reliably with local files.
  //
  // Race-condition guard: a removed-from-DOM script may still execute if
  // its HTTP response was already received (browser behaviour).  We track
  // the *expected* script element so that any stale callback from a
  // previously-loaded or in-flight script is silently discarded.
  var _pendingLoadResolve = null;
  var _pendingScriptRef   = null;   // element we are currently waiting for
  window._mapDataCallback = function(json) {{
    // document.currentScript is the <script> element being evaluated right
    // now.  If it differs from the element we registered most recently, the
    // call is stale (from a removed / superseded script) and must be ignored.
    var caller = document.currentScript;
    if (caller && caller !== _pendingScriptRef) {{
      return;  // stale callback – discard
    }}
    if (_pendingLoadResolve) {{
      _pendingLoadResolve(json);
      _pendingLoadResolve = null;
    }}
    _pendingScriptRef = null;
  }};

  function loadData() {{
    // Build the ordered list of keys to try:
    //  1. model|ref_alias     → current format, one file per reference
    //  2. model|ref_type      → legacy format (gridded / observation)
    //  3. model|model         → when per_bins ref_alias equals the model name
    //  4. model               → oldest format, no ref segment at all
    const model   = document.getElementById('select-model').value;
    const k1 = getDataKey();
    const k2 = getDataKeyRefType();
    const k3 = _buildKey(model, _currentDepth());   // ref_alias == model name
    const k4 = getDataKeyFallback();
    // Deduplicate: skip a fallback if it happens to equal a previous key.
    const seen = {{}};
    const chain = [];
    [k1, k2, k3, k4].forEach(function(k) {{
      if (!seen[k]) {{ seen[k] = true; chain.push(k); }}
    }});
    _loadKeyChain(chain);
  }}

  function _loadKeyChain(keys) {{
    if (!keys || keys.length === 0) {{
      const status = document.getElementById('map-status');
      status.textContent = 'No data available for this combination.';
      gridLayer.clearLayers();
      document.getElementById('map-colorbar').style.display = 'none';
      return;
    }}
    _loadKey(keys[0], keys.slice(1));
  }}

  function _loadKey(key, remainingKeys) {{
    const filename = getFilename(key);
    const status = document.getElementById('map-status');

    status.textContent = 'Loading data...';

    const old = document.getElementById('map-data-script');
    if (old) old.parentNode.removeChild(old);

    // Create promise BEFORE registering _pendingScriptRef so that
    // _mapDataCallback can safely resolve it.
    var promise = new Promise(function(resolve) {{ _pendingLoadResolve = resolve; }});

    var script = document.createElement('script');
    script.id = 'map-data-script';
    script.src = filename;
    // Register the new script as the expected recipient BEFORE appending.
    _pendingScriptRef = script;

    // Render is driven by the Promise (resolved inside _mapDataCallback).
    // The onload handler merely acts as a fallback signal: if the script
    // loaded but _mapDataCallback was somehow not invoked (e.g. empty file),
    // the promise will be pending and we silently show no-data.
    promise.then(function(json) {{
      if (json && json.data) {{
        currentData = json;
        renderGrid(json);
      }} else {{
        status.textContent = 'No data available for this combination.';
        gridLayer.clearLayers();
        document.getElementById('map-colorbar').style.display = 'none';
      }}
    }});

    script.onerror = function() {{
      _pendingLoadResolve = null;
      _pendingScriptRef   = null;
      if (remainingKeys && remainingKeys.length > 0) {{
        // Current key not found – try next fallback in chain.
        _loadKey(remainingKeys[0], remainingKeys.slice(1));
      }} else {{
        status.textContent = 'No data available for this combination.';
        gridLayer.clearLayers();
        document.getElementById('map-colorbar').style.display = 'none';
        console.warn('No data file found for: ' + key);
      }}
    }};
    document.head.appendChild(script);
  }}

  // --- Init ---
  document.addEventListener('DOMContentLoaded', function() {{
    initMap();
    updateVariableSelector();

    document.getElementById('select-ref').addEventListener('change', function() {{
      updateVariableSelector();
      loadData();
    }});
    document.getElementById('select-variable').addEventListener('change', function() {{
      updateDepthSelector();
      loadData();
    }});
    // Auto-load on any selector change
    ['select-model', 'select-metric', 'select-lead', 'select-depth'].forEach(function(id) {{
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', loadData);
    }});

    // Initial load
    loadData();
  }});
}})();
</script>
"""


def build_map_page(
    metadata: Dict[str, Any],
    config: Dict[str, Any],
    build_head_fn,
    build_navbar_fn,
    build_footer_fn,
    site_base_url: str = "",
) -> str:
    """Build the complete map HTML page."""
    content = generate_map_page_content(metadata, site_base_url=site_base_url)

    navbar = build_navbar_fn("maps", config)
    footer = build_footer_fn(config)

    return f"""<!DOCTYPE html>
<html lang="en">
{build_head_fn("Spatial Maps")}
<body>
{navbar}
<div class="container content-area">
    <h1 class="map-title">Spatial Performance Maps</h1>
    <p class="map-description">Interactive visualization of model performance metrics across the global ocean.
    Select a model, variable, metric, and lead day to display the spatial distribution.</p>
    {content}
</div>
{footer}
</body>
</html>
"""
