#!/usr/bin/env python3
"""
sandag - CLI for the San Diego Regional Data Warehouse (SANDAG/SanGIS)

Access 358+ GIS datasets covering San Diego County including parcels, bikeways,
zoning, census data, transportation, ecology, and much more.

Data source: https://geo.sandag.org
"""

import json
import sys
import os
import csv
import io
import re
import time
import sqlite3
import difflib
from pathlib import Path
from urllib.parse import urlencode

import click
import requests
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

console = Console()
err_console = Console(stderr=True)

# ── Constants ──────────────────────────────────────────────────────────────────

BASE_URL = "https://geo.sandag.org/server/rest/services/Hosted"
PORTAL_URL = "https://geo.sandag.org/portal"
DOWNLOAD_BASE = "https://geo.sandag.org/server/rest/directories/downloads"

CATEGORIES = [
    "Agriculture", "Business", "Census", "Community", "District",
    "Ecology & Parks", "Elevation", "Fire", "Health and Public Safety",
    "Hydrology & Geology", "Jurisdiction", "Landbase", "Land Use",
    "Miscellaneous", "Place", "Transportation", "Utilities", "Zoning",
]

# Keyword overrides for categories whose portal taxonomy terms don't appear
# in dataset tags/descriptions. Each entry is a list of OR-matched phrases.
CATEGORY_KEYWORDS = {
    "ecology & parks":       ["ecology", "park", "open space", "trail", "preserve", "mscp", "wildlife"],
    "health and public safety": ["health", "hospital", "clinic", "ambulance", "naloxone",
                                "evacuation", "public safety", "healthcare", "pharmacy"],
    "hydrology & geology":   ["flood", "floodplain", "nhd", "fault", "watershed",
                               "geology", "isopluvial", "flowline", "waterbody", "soils"],
    "landbase":              ["parcel", "address", "cadastral", "property", "assessor",
                               "lot", "apn", "situs"],
}

EXPORT_FORMATS = {
    "geojson": ".geojson",
    "csv": ".csv",
    "shapefile": "_shapefile.zip",
    "filegdb": "_filegdb.zip",
    "json": ".html",  # their JSON viewer is .html
    "metadata": ".pdf",
}

CACHE_DIR = Path.home() / ".cache" / "sdgis-cli"
CACHE_FILE = CACHE_DIR / "datasets.json"
INDEX_FILE = CACHE_DIR / "index.db"
CACHE_TTL = 86400  # 24 hours
EMBED_MODEL = "all-MiniLM-L6-v2"


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "sdgis-cli/1.0"})
    s.timeout = 30
    return s


def handle_request_error(e, dataset=None):
    """Provide helpful error messages for common failures."""
    msg = str(e)
    if "ProxyError" in msg or "ConnectionError" in msg:
        raise click.ClickException(
            f"Cannot connect to geo.sandag.org. Check your internet connection."
        )
    elif "Timeout" in msg:
        raise click.ClickException(
            f"Request timed out. The server may be slow — try again."
        )
    elif "404" in msg:
        hint = f" Check the dataset name: '{dataset}'" if dataset else ""
        raise click.ClickException(f"Dataset not found.{hint}")
    else:
        raise click.ClickException(f"Request failed: {e}")


def feature_server_url(dataset_name, layer=0):
    return f"{BASE_URL}/{dataset_name}/FeatureServer/{layer}"


def service_url(dataset_name):
    return f"{BASE_URL}/{dataset_name}/FeatureServer"


RDW_LIST_URL = f"{BASE_URL}/RDW_List/FeatureServer/0/query"

def discover_datasets(session, force=False):
    """Discover datasets from the RDW_List authoritative catalog."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if not force and CACHE_FILE.exists():
        age = time.time() - CACHE_FILE.stat().st_mtime
        if age < CACHE_TTL:
            with open(CACHE_FILE) as f:
                return json.load(f)

    datasets = []

    with err_console.status("Discovering datasets..."):
        try:
            r = session.get(RDW_LIST_URL, params={
                "where": "1=1",
                "outFields": "dataset_name,category1,category2,tags,details",
                "orderByFields": "dataset_name ASC",
                "resultRecordCount": 1000,
                "f": "json",
            }, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise ValueError(data["error"].get("message", "API error"))

            for item in data.get("features", []):
                a = item.get("attributes", {})
                name = a.get("dataset_name", "")
                if not name:
                    continue
                cats = [c for c in [a.get("category1"), a.get("category2")] if c]
                tags = [t.strip() for t in (a.get("tags") or "").split(",") if t.strip()]
                datasets.append({
                    "name": name,
                    "title": name,
                    "description": (a.get("details") or "")[:200],
                    "tags": tags,
                    "categories": cats,
                    "url": f"{BASE_URL}/{name}/FeatureServer",
                })

        except Exception as e:
            err_console.print(f"[red]Could not reach RDW_List: {e}")
            err_console.print("[yellow]Falling back to portal search...")
            return _discover_via_portal(session)

    if not datasets:
        err_console.print("[yellow]RDW_List returned no datasets. Using portal search.")
        return _discover_via_portal(session)

    with open(CACHE_FILE, "w") as f:
        json.dump(datasets, f, indent=2)

    return datasets


def _discover_via_portal(session):
    """Fallback: discover datasets via ArcGIS Portal search."""
    datasets = []
    start = 1
    for _ in range(20):  # max 2000 results
        params = {"q": 'type:"Feature Service" AND owner:SanGIS', "start": start, "num": 100, "f": "json"}
        try:
            r = session.get(f"{PORTAL_URL}/sharing/rest/search", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            err_console.print(f"[red]Portal search failed: {e}")
            return get_builtin_datasets()

        for item in data.get("results", []):
            url = item.get("url", "")
            m = re.search(r"/Hosted/([^/]+)/", url)
            name = m.group(1) if m else item.get("name", "").replace(" ", "_")
            datasets.append({
                "name": name,
                "title": item.get("title", name),
                "description": (item.get("snippet") or "")[:200],
                "tags": item.get("tags", []),
                "categories": [],
                "url": url,
            })

        next_start = data.get("nextStart", -1)
        if next_start == -1 or next_start <= start:
            break
        start = next_start

    seen, unique = set(), []
    for ds in sorted(datasets, key=lambda x: x["name"].lower()):
        if ds["name"] not in seen:
            seen.add(ds["name"])
            unique.append(ds)

    if unique:
        with open(CACHE_FILE, "w") as f:
            json.dump(unique, f, indent=2)
    return unique or get_builtin_datasets()


def get_builtin_datasets():
    """Fallback list extracted from the Data Warehouse HTML."""
    names = [
        "ABC_Licenses", "Active_Faults_CN", "Address_Points", "Address_Points_NG911",
        "Adult_Residential_Facilities", "Affordable_Housing_Inventory",
        "Agricultural_Commodity_2020", "Agricultural_Preserve",
        "Agricultural_Preserve_Contracts", "Airport_Influence_Area",
        "Airport_Noise_Contours", "Airport_Overflight_Extents", "Airport_Runways",
        "Airport_Safety_Zones", "Airspace", "Ambulance_Operating_Areas",
        "Assembly_Bill_130", "Assembly_Bill_2011", "Assembly_Bill_803",
        "Assessor_Book", "AWM_Certified_Producers", "AWM_Commodity",
        "AWM_Organic_Producers", "Ballot_Drop_Boxes_2025_11_04", "Bays",
        "Bike_Master_Plan_SD", "Bike_Plan_CN", "Bikeways", "BMP_CN",
        "Broadband_Business_CPUC", "Broadband_Consumer_CPUC", "Broadband_Mobile_CPUC",
        "Building_Outlines", "Business_Improvement_Districts_SD", "Business_Sites",
        "CA_Bridge_Hospitals", "Call_Box", "CALTRANS_Urban_Area", "Casinos",
    ]
    return [{"name": n, "title": n.replace("_", " "), "description": "", "tags": [], "id": "", "url": service_url(n)} for n in names]


def suggest_dataset(name):
    """Return close dataset name matches for 'did you mean?' hints."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            datasets = json.load(f)
    else:
        datasets = get_builtin_datasets()
    names = [ds["name"] for ds in datasets]
    matches = difflib.get_close_matches(name, names, n=3, cutoff=0.5)
    if not matches:
        name_lower = name.lower()
        matches = [n for n in names if name_lower in n.lower() or n.lower() in name_lower][:3]
    return matches


def fuzzy_match(query, datasets):
    """Simple fuzzy search across name, title, tags, and description."""
    q = query.lower()
    scored = []
    for ds in datasets:
        score = 0
        name_lower = ds["name"].lower()
        title_lower = ds.get("title", "").lower()
        tags_lower = " ".join(ds.get("tags", [])).lower()
        desc_lower = ds.get("description", "").lower()

        if q == name_lower:
            score = 100
        elif q in name_lower:
            score = 80
        elif q in title_lower:
            score = 70
        elif q in tags_lower:
            score = 50
        elif q in desc_lower:
            score = 30

        if score > 0:
            scored.append((score, ds))

    scored.sort(key=lambda x: (-x[0], x[1]["name"].lower()))
    return [ds for _, ds in scored]


def query_features(session, dataset, where="1=1", out_fields="*", geometry=None,
                   geometry_type=None, spatial_rel=None, return_geometry=True,
                   result_offset=0, result_record_count=2000, out_sr=4326,
                   order_by=None, return_count_only=False, return_ids_only=False,
                   layer=0):
    """Query an ArcGIS Feature Service layer."""
    url = f"{feature_server_url(dataset, layer)}/query"
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": str(return_geometry).lower(),
        "outSR": out_sr,
        "f": "json",
        "resultOffset": result_offset,
        "resultRecordCount": result_record_count,
    }

    if geometry:
        params["geometry"] = geometry
    if geometry_type:
        params["geometryType"] = geometry_type
    if spatial_rel:
        params["spatialRel"] = spatial_rel
    if order_by:
        params["orderByFields"] = order_by
    if return_count_only:
        params["returnCountOnly"] = "true"
    if return_ids_only:
        params["returnIdsOnly"] = "true"

    try:
        r = session.get(url, params=params, timeout=60)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        handle_request_error(e, dataset)
    data = r.json()

    if "error" in data:
        msg = data['error'].get('message', 'Unknown error')
        code = data['error'].get('code', '?')
        if "does not exist" in msg and "Field name" in msg:
            raise click.ClickException(
                f"{msg}\n\n  Run 'sdgis fields {dataset}' to see all valid field names."
            )
        if "service" in msg.lower() and "not found" in msg.lower():
            suggestions = suggest_dataset(dataset)
            hint = f"\n\n  Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            hint += f"\n  Run 'sdgis search {dataset}' to find datasets."
            raise click.ClickException(f"Dataset not found: '{dataset}'.{hint}")
        raise click.ClickException(f"API Error {code}: {msg}")
    return data


def get_layer_info(session, dataset, layer=0):
    """Get field definitions and metadata for a layer."""
    url = feature_server_url(dataset, layer)
    r = session.get(url, params={"f": "json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        msg = data['error'].get('message', 'Unknown')
        if "service" in msg.lower() and "not found" in msg.lower():
            suggestions = suggest_dataset(dataset)
            hint = f"\n\n  Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            hint += f"\n  Run 'sdgis search {dataset}' to find datasets."
            raise click.ClickException(f"Dataset not found: '{dataset}'.{hint}")
        raise click.ClickException(f"API Error: {msg}")
    return data


def get_service_info(session, dataset):
    """Get service-level metadata."""
    url = service_url(dataset)
    r = session.get(url, params={"f": "json"}, timeout=30)
    r.raise_for_status()
    return r.json()


def features_to_geojson(features, geometry_type=None):
    """Convert ArcGIS JSON features to GeoJSON FeatureCollection."""
    geo_features = []
    for feat in features:
        geom = feat.get("geometry")
        props = feat.get("attributes", {})
        geo_geom = None

        if geom:
            if "x" in geom and "y" in geom:
                geo_geom = {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
            elif "rings" in geom:
                if len(geom["rings"]) == 1:
                    geo_geom = {"type": "Polygon", "coordinates": geom["rings"]}
                else:
                    geo_geom = {"type": "MultiPolygon", "coordinates": [[r] for r in geom["rings"]]}
            elif "paths" in geom:
                if len(geom["paths"]) == 1:
                    geo_geom = {"type": "LineString", "coordinates": geom["paths"][0]}
                else:
                    geo_geom = {"type": "MultiLineString", "coordinates": geom["paths"]}
            elif "points" in geom:
                geo_geom = {"type": "MultiPoint", "coordinates": geom["points"]}

        geo_features.append({
            "type": "Feature",
            "geometry": geo_geom,
            "properties": props,
        })

    return {
        "type": "FeatureCollection",
        "features": geo_features,
    }


def features_to_csv_str(features):
    """Convert features to CSV string."""
    if not features:
        return ""
    rows = [f.get("attributes", {}) for f in features]
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ── SQLite index ───────────────────────────────────────────────────────────────

def _open_index():
    """Open (and initialize schema if needed) the SQLite index."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(INDEX_FILE)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS datasets (
            name        TEXT PRIMARY KEY,
            title       TEXT,
            description TEXT,
            tags        TEXT,
            url         TEXT,
            id          TEXT,
            modified    INTEGER
        );
        CREATE TABLE IF NOT EXISTS embeddings (
            name   TEXT PRIMARY KEY,
            vector BLOB
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS datasets_fts USING fts5(
            name, title, description, tags,
            content='datasets', content_rowid='rowid'
        );
    """)
    conn.commit()
    return conn


def build_index(session, force=False):
    """Populate index.db with dataset metadata and embeddings."""
    try:
        import numpy as np
        from sentence_transformers import SentenceTransformer
        has_embeddings = True
    except ImportError:
        has_embeddings = False
        err_console.print("[yellow]sentence-transformers not installed — building FTS-only index (no semantic search).")
        err_console.print("[dim]  For semantic search, run:")
        err_console.print("[dim]    pipx inject sdgis-cli sentence-transformers numpy  (if using pipx)")
        err_console.print("[dim]    pip install sdgis-cli[embed]                       (if using pip)")

    datasets = discover_datasets(session, force=force)
    conn = _open_index()

    # Upsert metadata + rebuild FTS
    conn.execute("DELETE FROM datasets_fts")
    for ds in datasets:
        conn.execute(
            "INSERT OR REPLACE INTO datasets (name, title, description, tags, url, id, modified) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ds["name"], ds.get("title", ""), ds.get("description", ""),
             json.dumps(ds.get("tags", [])), ds.get("url", ""),
             ds.get("id", ""), ds.get("modified", 0))
        )
    conn.execute(
        "INSERT INTO datasets_fts(datasets_fts) VALUES ('rebuild')"
    )
    conn.commit()

    if not has_embeddings:
        conn.close()
        return len(datasets), False

    # Determine which datasets still need embeddings
    if force:
        conn.execute("DELETE FROM embeddings")
        conn.commit()
        to_embed = datasets
    else:
        existing = {row[0] for row in conn.execute("SELECT name FROM embeddings")}
        to_embed = [ds for ds in datasets if ds["name"] not in existing]

    if not to_embed:
        conn.close()
        return len(datasets), True

    err_console.print(f"[dim]Loading embedding model '{EMBED_MODEL}'...")
    model = SentenceTransformer(EMBED_MODEL)

    texts = [
        f"{ds['name']} {ds.get('title', '')} {ds.get('description', '')} "
        f"{' '.join(ds.get('tags', []))}"
        for ds in to_embed
    ]

    with Progress(SpinnerColumn(), TextColumn("[bold blue]{task.description}"),
                  console=err_console) as progress:
        task = progress.add_task(f"Embedding {len(to_embed)} datasets...", total=None)
        vecs = model.encode(texts, batch_size=64, show_progress_bar=False)
        progress.update(task, description=f"[bold blue]Embedded {len(to_embed)} datasets")

    for ds, vec in zip(to_embed, vecs):
        blob = vec.astype("float32").tobytes()
        conn.execute("INSERT OR REPLACE INTO embeddings (name, vector) VALUES (?, ?)",
                     (ds["name"], blob))
    conn.commit()
    conn.close()
    return len(datasets), True


def _load_dataset_row(row):
    return {
        "name": row["name"],
        "title": row["title"] or "",
        "description": row["description"] or "",
        "tags": json.loads(row["tags"] or "[]"),
        "url": row["url"] or "",
        "id": row["id"] or "",
    }


def semantic_search(query, top_k=25):
    """Vector cosine-similarity search against the embedding index."""
    import numpy as np
    from sentence_transformers import SentenceTransformer

    conn = _open_index()
    rows = conn.execute("SELECT e.name, e.vector, d.title, d.description, d.tags, d.url, d.id "
                        "FROM embeddings e JOIN datasets d ON e.name = d.name").fetchall()
    conn.close()
    if not rows:
        return None  # index empty

    model = SentenceTransformer(EMBED_MODEL)
    q_vec = model.encode([query])[0].astype("float32")
    q_norm = q_vec / (np.linalg.norm(q_vec) + 1e-9)

    scored = []
    for row in rows:
        vec = np.frombuffer(row["vector"], dtype="float32")
        norm = vec / (np.linalg.norm(vec) + 1e-9)
        score = float(q_norm @ norm)
        scored.append((score, row))

    scored.sort(key=lambda x: -x[0])
    return [
        {
            "name": row["name"], "title": row["title"] or "",
            "description": row["description"] or "",
            "tags": json.loads(row["tags"] or "[]"),
            "url": row["url"] or "", "id": row["id"] or "",
            "_score": round(score, 4),
        }
        for score, row in scored[:top_k]
    ]


def fts_search(query, top_k=25):
    """FTS5 full-text search against the SQLite index."""
    conn = _open_index()
    # Escape FTS5 special chars
    safe_q = re.sub(r'["\*\(\)]', ' ', query).strip()
    try:
        rows = conn.execute(
            "SELECT d.name, d.title, d.description, d.tags, d.url, d.id "
            "FROM datasets_fts f JOIN datasets d ON d.name = f.name "
            "WHERE datasets_fts MATCH ? ORDER BY rank LIMIT ?",
            (safe_q, top_k)
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()
    return [_load_dataset_row(r) for r in rows]


# ── CLI ────────────────────────────────────────────────────────────────────────

@click.group()
@click.version_option("1.0.9", prog_name="sdgis")
@click.pass_context
def cli(ctx):
    """
    🌊 sdgis - San Diego Regional Data Warehouse CLI

    Access 358+ GIS datasets from SANDAG/SanGIS covering San Diego County.

    \b
    Datasets include: parcels, bikeways, zoning, census tracts, fire stations,
    transit routes, hydrology, affordable housing, business sites, and more.

    \b
    Quick start:
      sdgis categories                # Browse the 18 dataset categories
      sdgis list                      # See all 360+ available datasets
      sdgis info Bikeways             # Explore a dataset's fields & metadata
      sdgis query Bikeways --limit 5  # Fetch features (use exact name from list)
      sdgis count Bikeways            # Count total features
      sdgis download Bikeways         # Download pre-built exports
    """
    ctx.ensure_object(dict)
    ctx.obj["session"] = get_session()


# ── list ───────────────────────────────────────────────────────────────────────

@cli.command("list")
@click.option("--refresh", is_flag=True, help="Force refresh the dataset cache")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.option("--category", "-c", default=None, help="Filter by category (e.g. Transportation, Fire)")
@click.pass_context
def list_datasets(ctx, refresh, fmt, as_json, category):
    as_json = as_json or fmt == "json"
    """List all available datasets.

    \b
    Examples:
      sdgis list
      sdgis list --category Transportation
      sdgis list -c Fire
    """
    session = ctx.obj["session"]
    datasets = discover_datasets(session, force=refresh)

    if category:
        cat_lower = category.lower()
        # Use authoritative category fields from RDW_List if available,
        # otherwise fall back to keyword matching against tags/description
        has_category_data = any(ds.get("categories") for ds in datasets)
        if has_category_data:
            datasets = [
                ds for ds in datasets
                if any(cat_lower == c.lower() for c in ds.get("categories", []))
            ]
        else:
            phrases = CATEGORY_KEYWORDS.get(
                cat_lower,
                [p.strip() for p in re.split(r"\s*&\s*", cat_lower) if p.strip()]
            )
            def _matches(ds):
                text = " ".join([
                    ds.get("name", ""),
                    ds.get("description", ""),
                    " ".join(ds.get("tags", [])),
                ]).lower()
                return any(phrase in text for phrase in phrases)
            datasets = [ds for ds in datasets if _matches(ds)]
        if not datasets:
            err_console.print(f"[yellow]No datasets found in category '{category}'")
            err_console.print("[dim]  Run [bold]sdgis categories[/] to see valid category names")
            return

    if as_json:
        click.echo(json.dumps(datasets, indent=2))
        return

    title = f"SANDAG Data Warehouse — {len(datasets)} Datasets"
    if category:
        title += f" (category: {category})"

    table = Table(title=title, box=box.ROUNDED, show_lines=False)
    table.add_column("#", style="dim", width=4)
    table.add_column("Dataset Name", style="cyan bold", no_wrap=True)
    table.add_column("Tags", style="dim", max_width=40)

    for i, ds in enumerate(datasets, 1):
        tags = ", ".join(ds.get("tags", [])[:3])
        table.add_row(str(i), ds["name"], tags)

    console.print(table)


# ── index ──────────────────────────────────────────────────────────────────────

@cli.command("index")
@click.option("--force", is_flag=True, help="Rebuild index and re-embed all datasets")
@click.pass_context
def build_index_cmd(ctx, force):
    """Build local SQLite search index with embeddings.

    \b
    Downloads dataset catalog and computes semantic embeddings using
    a local sentence-transformers model (all-MiniLM-L6-v2, ~22MB).
    Run once before using 'search' for semantic results.

    \b
    Examples:
      sdgis index           # build or update index
      sdgis index --force   # force full rebuild
    """
    session = ctx.obj["session"]
    n, has_embeddings = build_index(session, force=force)
    mode = "semantic + FTS" if has_embeddings else "FTS-only"
    console.print(f"[green]✓[/] Index built — [bold]{n}[/] datasets indexed ({mode}) at [dim]{INDEX_FILE}[/]")
    if has_embeddings:
        console.print("[dim]  Use [bold]sdgis search <query>[/bold] for semantic search, e.g. sdgis search 'bike infrastructure'[/]")
    else:
        console.print("[dim]  Keyword search enabled. For semantic search: [bold]pipx inject sdgis-cli sentence-transformers numpy[/]")


# ── search ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("query")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.option("--fts", "force_fts", is_flag=True, help="Force FTS keyword search (skip semantic)")
@click.option("--fuzzy", "force_fuzzy", is_flag=True, help="Force fuzzy string match (skip index)")
@click.pass_context
def search(ctx, query, fmt, as_json, force_fts, force_fuzzy):
    as_json = as_json or fmt == "json"
    """Search datasets by name, tags, or description.

    \b
    Search priority (auto-detected):
      1. Semantic vector search  (if 'sdgis index' has been run)
      2. FTS5 keyword search     (if index exists without embeddings)
      3. Fuzzy string match      (fallback, no index required)
    """
    session = ctx.obj["session"]
    matches = None
    mode = "fuzzy"

    if not force_fuzzy and INDEX_FILE.exists():
        if not force_fts:
            # Try semantic search
            try:
                import numpy  # noqa: F401
                from sentence_transformers import SentenceTransformer  # noqa: F401
                with err_console.status("Searching (semantic)..."):
                    matches = semantic_search(query)
                if matches is not None:
                    mode = "semantic"
            except ImportError:
                pass

        if matches is None:
            # Fall back to FTS
            with err_console.status("Searching (FTS)..."):
                matches = fts_search(query) or None
            if matches:
                mode = "fts"

    if matches is None:
        # Final fallback: fuzzy match against live/cached catalog
        datasets = discover_datasets(session)
        matches = fuzzy_match(query, datasets)
        mode = "fuzzy"

    if not matches:
        err_console.print(f"[yellow]No datasets matching '{query}'")
        err_console.print("[dim]  Try: [bold]sdgis list[/] to browse all datasets, or [bold]sdgis categories[/] to explore by topic")
        return

    if as_json:
        click.echo(json.dumps(matches, indent=2))
        return

    mode_label = {"semantic": "semantic", "fts": "keyword", "fuzzy": "fuzzy"}.get(mode, mode)
    table = Table(
        title=f"Search: '{query}' — {len(matches)} results [{mode_label}]",
        box=box.ROUNDED,
    )
    table.add_column("Dataset", style="cyan bold", max_width=40)
    table.add_column("Title", max_width=45)
    table.add_column("Description", max_width=50, style="dim")

    for ds in matches[:25]:
        desc = ds.get("description", "")[:80]
        table.add_row(ds["name"], ds.get("title", ""), desc)

    console.print(table)
    if len(matches) > 25:
        err_console.print(f"[dim]  ...and {len(matches) - 25} more results")
    if mode == "fuzzy" and not force_fuzzy:
        err_console.print("[dim]  Tip: run [bold]sdgis index[/] for semantic search")


# ── info ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--layer", default=0, help="Layer index (default: 0)")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.pass_context
def info(ctx, dataset, layer, fmt, as_json):
    as_json = as_json or fmt == "json"
    """Show detailed info and fields for a dataset.

    \b
    Use this to explore a dataset before querying it — shows all field names,
    types, geometry type, and feature count. Get dataset names from 'sdgis list'.

    \b
    Examples:
      sdgis info Bikeways
      sdgis info ABC_Licenses --json-output
    """
    session = ctx.obj["session"]

    with err_console.status(f"Fetching info for {dataset}..."):
        try:
            svc = get_service_info(session, dataset)
            lyr = get_layer_info(session, dataset, layer)
        except Exception as e:
            raise click.ClickException(f"Failed to fetch info: {e}")

    if as_json:
        click.echo(json.dumps({"service": svc, "layer": lyr}, indent=2, default=str))
        return

    desc = svc.get("serviceDescription") or lyr.get("description") or "No description"
    name = lyr.get("name", dataset)
    geom_type = lyr.get("geometryType", "Unknown")
    feature_count_est = lyr.get("maxRecordCount", "?")

    panel_text = Text()
    panel_text.append(f"Name: ", style="bold")
    panel_text.append(f"{name}\n")
    panel_text.append(f"Geometry: ", style="bold")
    panel_text.append(f"{geom_type}\n")
    panel_text.append(f"Max Records/Query: ", style="bold")
    panel_text.append(f"{svc.get('maxRecordCount', '?')}\n")
    panel_text.append(f"Spatial Ref: ", style="bold")
    sr = lyr.get("extent", {}).get("spatialReference", {})
    panel_text.append(f"WKID {sr.get('latestWkid', sr.get('wkid', '?'))}\n\n")
    panel_text.append(f"{desc[:500]}", style="dim")

    console.print(Panel(panel_text, title=f"[bold cyan]{dataset}", border_style="blue"))

    # Fields table
    fields = lyr.get("fields", [])
    if fields:
        ftable = Table(title="Fields", box=box.SIMPLE, show_lines=False)
        ftable.add_column("Name", style="green")
        ftable.add_column("Alias", style="dim")
        ftable.add_column("Type", style="yellow")
        ftable.add_column("Length", style="dim", justify="right")

        for f in fields:
            ftype = f.get("type", "").replace("esriFieldType", "")
            length = str(f.get("length", "")) if f.get("length") else ""
            ftable.add_row(f["name"], f.get("alias", ""), ftype, length)

        console.print(ftable)

    # Links
    console.print()
    console.print(f"[dim]REST URL:[/]  {service_url(dataset)}")
    console.print(f"[dim]Portal:[/]    {PORTAL_URL}/home/item.html?id={svc.get('serviceItemId', '')}")
    console.print(f"[dim]Map:[/]       {PORTAL_URL}/apps/mapviewer/index.html?layers={svc.get('serviceItemId', '')}")


# ── count ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--where", default="1=1", help="SQL WHERE clause")
@click.option("--layer", default=0, help="Layer index")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.pass_context
def count(ctx, dataset, where, layer, fmt, as_json):
    as_json = as_json or fmt == "json"
    """Count features in a dataset.

    \b
    Examples:
      sdgis count Bikeways
      sdgis count ABC_Licenses --where "LICENSE_TYPE='21'"
    """
    session = ctx.obj["session"]

    with err_console.status(f"Counting features in {dataset}..."):
        data = query_features(session, dataset, where=where, return_count_only=True, layer=layer)

    n = data.get("count", "?")

    if as_json:
        click.echo(json.dumps({"dataset": dataset, "where": where, "count": n}))
        return

    console.print(f"[bold cyan]{dataset}[/]: [bold green]{n:,}[/] features" if isinstance(n, int) else f"{dataset}: {n} features")
    if where != "1=1":
        console.print(f"[dim]  WHERE: {where}")


# ── query ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--where", default="1=1", help="SQL WHERE clause (e.g. \"NAME='Oceanside'\")")
@click.option("--fields", default="*", help="Comma-separated field names (default: all)")
@click.option("--limit", default=10, type=int, help="Max features to return (default: 10)")
@click.option("--offset", default=0, type=int, help="Result offset for pagination")
@click.option("--order-by", default=None, help="ORDER BY clause (e.g. 'NAME ASC')")
@click.option("--geometry/--no-geometry", default=False, help="Include geometry in output")
@click.option("--srid", default=4326, type=int, help="Output spatial reference (default: 4326/WGS84)")
@click.option("--bbox", default=None, help="Bounding box filter: xmin,ymin,xmax,ymax (WGS84)")
@click.option("--layer", default=0, type=int, help="Layer index")
@click.option("-f", "--format", "fmt", default="table",
              type=click.Choice(["table", "json", "geojson", "csv"]),
              help="Output format")
@click.pass_context
def query(ctx, dataset, where, fields, limit, offset, order_by, geometry,
          srid, bbox, layer, fmt):
    """Query features from a dataset.

    \b
    Examples:
      sdgis query Bikeways --limit 5
      sdgis query Bikeways --where "RD_NAME='Coast Blvd'" --fields "RD_NAME,CLASS"
      sdgis query Affordable_Housing_Inventory -f geojson --geometry > housing.geojson
      sdgis query ABC_Licenses --bbox "-117.2,32.7,-117.1,32.8" --limit 50
    """
    session = ctx.obj["session"]

    geom_param = None
    geom_type = None
    spatial_rel = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            geom_param = json.dumps({
                "xmin": parts[0], "ymin": parts[1],
                "xmax": parts[2], "ymax": parts[3],
                "spatialReference": {"wkid": 4326}
            })
            geom_type = "esriGeometryEnvelope"
            spatial_rel = "esriSpatialRelIntersects"
        except (ValueError, IndexError):
            raise click.ClickException("Invalid bbox format. Use: xmin,ymin,xmax,ymax")

    with err_console.status(f"Querying {dataset}..."):
        data = query_features(
            session, dataset, where=where, out_fields=fields,
            return_geometry=(geometry or fmt == "geojson"),
            result_record_count=limit, result_offset=offset,
            out_sr=srid, order_by=order_by, layer=layer,
            geometry=geom_param, geometry_type=geom_type, spatial_rel=spatial_rel,
        )

    features = data.get("features", [])
    if not features:
        err_console.print("[yellow]No features returned.")
        if where and where != "1=1":
            err_console.print(f"[dim]  Hint: use [bold]sdgis values {dataset} <field>[/bold] to see valid values for a field.")
        return

    if fmt == "json":
        click.echo(json.dumps(data, indent=2))
    elif fmt == "geojson":
        click.echo(json.dumps(features_to_geojson(features), indent=2))
    elif fmt == "csv":
        click.echo(features_to_csv_str(features))
    else:
        # Table output
        all_attrs = features[0].get("attributes", {})
        field_names = list(all_attrs.keys())

        term_width = console.size.width
        col_width = 20
        max_cols = max(2, min(len(field_names), (term_width - 4) // (col_width + 3)))

        table = Table(
            title=f"{dataset} — {len(features)} features",
            box=box.ROUNDED,
            show_lines=False,
        )
        for fname in field_names[:max_cols]:
            table.add_column(fname, max_width=col_width, overflow="ellipsis")

        for feat in features:
            attrs = feat.get("attributes", {})
            row = [str(attrs.get(fn, "")) if attrs.get(fn) is not None else "" for fn in field_names[:max_cols]]
            table.add_row(*row)

        console.print(table)
        if len(field_names) > max_cols:
            err_console.print(f"[dim]  ({len(field_names) - max_cols} additional fields hidden — use --fields to select columns or -f json to see all)")
        if data.get("exceededTransferLimit"):
            err_console.print(f"[yellow]  ⚠ More features available. Use --offset {offset + limit} to paginate.")


# ── bbox ────────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--where", default="1=1", help="SQL WHERE clause to filter features")
@click.option("--layer", default=0, type=int, help="Layer index")
@click.pass_context
def bbox(ctx, dataset, where, layer):
    """Output the bounding box of a dataset (or filtered subset) as xmin,ymin,xmax,ymax.

    \b
    Useful for piping into --bbox of another query:
      BBOX=$(sdgis bbox Dam_Inundation_DSOD --where "downstreamhazard='Extremely High'")
      sdgis query Community_Points --bbox "$BBOX" -f json

    Output is plain text: xmin,ymin,xmax,ymax (WGS84)
    """
    session = ctx.obj["session"]
    url = f"{feature_server_url(dataset, layer)}/query"
    params = {
        "where": where,
        "returnGeometry": "true",
        "returnExtentOnly": "true",
        "outSR": 4326,
        "f": "json",
    }
    with err_console.status(f"Fetching extent for {dataset}..."):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            handle_request_error(e, dataset)
        data = r.json()

    if "error" in data:
        msg = data["error"].get("message", "Unknown")
        raise click.ClickException(f"API Error: {msg}")

    ext = data.get("extent")
    if ext and all(k in ext for k in ("xmin", "ymin", "xmax", "ymax")):
        result = f"{ext['xmin']},{ext['ymin']},{ext['xmax']},{ext['ymax']}"
        click.echo(result)
    else:
        raise click.ClickException("No extent returned — dataset may not support returnExtentOnly.")


# ── query-all ──────────────────────────────────────────────────────────────────

@cli.command("query-all")
@click.argument("dataset")
@click.option("--where", default="1=1", help="SQL WHERE clause")
@click.option("--fields", default="*", help="Comma-separated field names")
@click.option("--geometry/--no-geometry", default=False, help="Include geometry")
@click.option("--srid", default=4326, type=int, help="Output SRID")
@click.option("--layer", default=0, type=int, help="Layer index")
@click.option("-f", "--format", "fmt", default="geojson",
              type=click.Choice(["json", "geojson", "csv"]),
              help="Output format (default: geojson)")
@click.option("--limit", default=None, type=int, help="Stop after N features (default: all)")
@click.option("--max-features", "limit", default=None, type=int, hidden=True)
@click.pass_context
def query_all(ctx, dataset, where, fields, geometry, srid, layer, fmt, limit):
    """Fetch ALL features with automatic pagination.

    \b
    Automatically pages through the entire dataset:
      sdgis query-all Bikeways -f geojson > bikeways.geojson
      sdgis query-all ABC_Licenses -f csv > licenses.csv
    """
    session = ctx.obj["session"]
    page_size = 2000
    offset = 0
    all_features = []

    with err_console.status(f"Fetching {dataset}...") as status:
        while True:
            remaining = None
            if limit:
                remaining = limit - len(all_features)
                if remaining <= 0:
                    break
                page_size = min(2000, remaining)

            data = query_features(
                session, dataset, where=where, out_fields=fields,
                return_geometry=(geometry or fmt == "geojson"),
                result_record_count=page_size, result_offset=offset,
                out_sr=srid, layer=layer,
            )

            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            offset += len(features)
            status.update(f"Fetching {dataset}... ({len(all_features)} features)")

            if not data.get("exceededTransferLimit", False):
                break

    err_console.print(f"[green]Total: {len(all_features)} features")

    if fmt == "geojson":
        click.echo(json.dumps(features_to_geojson(all_features), indent=2))
    elif fmt == "csv":
        click.echo(features_to_csv_str(all_features))
    else:
        click.echo(json.dumps({"features": all_features}, indent=2))


# ── fields ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--layer", default=0, help="Layer index")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.pass_context
def fields(ctx, dataset, layer, fmt, as_json):
    as_json = as_json or fmt == "json"
    """List fields/columns for a dataset."""
    session = ctx.obj["session"]

    with err_console.status(f"Fetching fields for {dataset}..."):
        lyr = get_layer_info(session, dataset, layer)

    field_list = lyr.get("fields", [])

    if as_json:
        click.echo(json.dumps(field_list, indent=2))
        return

    table = Table(title=f"{dataset} — Fields", box=box.ROUNDED)
    table.add_column("Name", style="green bold")
    table.add_column("Alias")
    table.add_column("Type", style="yellow")
    table.add_column("Nullable", style="dim")
    table.add_column("Domain", style="dim")

    for f in field_list:
        ftype = f.get("type", "").replace("esriFieldType", "")
        nullable = "✓" if f.get("nullable", True) else "✗"
        domain = f.get("domain", {})
        domain_str = domain.get("name", "") if domain else ""
        table.add_row(f["name"], f.get("alias", ""), ftype, nullable, domain_str)

    console.print(table)


# ── values ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.argument("field")
@click.option("--where", default="1=1", help="SQL WHERE clause to filter before counting")
@click.option("--limit", default=200, type=int, help="Max distinct values to return (default: 200)")
@click.option("--layer", default=0, help="Layer index")
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
@click.pass_context
def values(ctx, dataset, field, where, limit, layer, fmt, as_json):
    as_json = as_json or fmt == "json"
    """List distinct values for a field — essential for building WHERE filters.

    \b
    Use this before querying to discover valid filter values for a field.
    Without this, you're guessing at exact string values (and failing).

    \b
    Examples:
      sdgis values Bikeways jurisdiction
      sdgis values ABC_Licenses LICENSE_TYPE --json-output
      sdgis values Roads_All funclass
    """
    session = ctx.obj["session"]
    url = f"{feature_server_url(dataset, layer)}/query"
    params = {
        "where": where,
        "outFields": field,
        "returnGeometry": "false",
        "returnDistinctValues": "true",
        "orderByFields": field,
        "resultRecordCount": limit,
        "f": "json",
    }

    with err_console.status(f"Fetching distinct values for {dataset}.{field}..."):
        try:
            r = session.get(url, params=params, timeout=60)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            handle_request_error(e, dataset)
        data = r.json()

    if "error" in data:
        msg = data['error'].get('message', 'Unknown error')
        if "does not exist" in msg and "Field name" in msg:
            raise click.ClickException(
                f"{msg}\n\n  Run 'sdgis fields {dataset}' to see all valid field names."
            )
        raise click.ClickException(f"API Error: {msg}")

    features = data.get("features", [])
    vals = [f.get("attributes", {}).get(field) for f in features]
    vals = [v for v in vals if v is not None]

    if as_json:
        click.echo(json.dumps(vals, indent=2))
        return

    if not vals:
        err_console.print(f"[yellow]No values found for {dataset}.{field}")
        return

    table = Table(
        title=f"{dataset}.{field} — {len(vals)} distinct value{'s' if len(vals) != 1 else ''}",
        box=box.ROUNDED,
    )
    table.add_column("Value", style="cyan")
    for v in vals:
        table.add_row(str(v))

    console.print(table)
    if len(vals) >= limit:
        err_console.print(f"[dim]  Showing first {limit} values. Use --limit to increase.")


# ── describe ───────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--layer", default=0, help="Layer index")
@click.option("-n", "--sample-count", default=3, type=int, help="Number of sample records")
@click.pass_context
def describe(ctx, dataset, layer, sample_count):
    """Full schema + count + sample records in one JSON call.

    \b
    Designed for AI agents that need to understand a dataset before querying.
    Returns fields, geometry type, feature count, and sample rows as JSON.

    \b
    Examples:
      sdgis describe Bikeways
      sdgis describe ABC_Licenses -n 5
      sdgis describe Bikeways | less        # page through large output
      sdgis describe Bikeways | jq '.fields[].name'
    """
    session = ctx.obj["session"]

    with err_console.status(f"Describing {dataset}..."):
        try:
            lyr = get_layer_info(session, dataset, layer)
            count_data = query_features(session, dataset, where="1=1", return_count_only=True, layer=layer)
            sample_data = query_features(session, dataset, result_record_count=sample_count, layer=layer)
        except Exception as e:
            raise click.ClickException(f"Failed to describe {dataset}: {e}")

    fields_info = [
        {
            "name": f["name"],
            "alias": f.get("alias", ""),
            "type": f.get("type", "").replace("esriFieldType", ""),
            "nullable": f.get("nullable", True),
            "domain": f.get("domain", {}).get("name") if f.get("domain") else None,
        }
        for f in lyr.get("fields", [])
    ]

    click.echo(json.dumps({
        "dataset": dataset,
        "name": lyr.get("name", dataset),
        "geometry_type": lyr.get("geometryType", ""),
        "feature_count": count_data.get("count"),
        "max_record_count": lyr.get("maxRecordCount"),
        "description": lyr.get("description", ""),
        "fields": fields_info,
        "sample": [f.get("attributes", {}) for f in sample_data.get("features", [])],
    }, indent=2))


# ── sample ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.argument("n_arg", default=None, required=False, type=int, metavar="N")
@click.option("-n", "--count", "n_opt", default=5, help="Number of sample records")
@click.option("--layer", default=0, help="Layer index")
@click.option("-f", "--format", "fmt", default="table",
              type=click.Choice(["table", "json", "geojson", "csv"]),
              help="Output format")
@click.pass_context
def sample(ctx, dataset, n_arg, n_opt, layer, fmt):
    """Show N sample records from a dataset (default: 5).

    \b
    Like 'sdgis head' but configurable count and no field type summary.
    Use 'sdgis head' for a first look, 'sdgis sample' when you need more rows.

    \b
    Examples:
      sdgis sample Bikeways
      sdgis sample Bikeways 10
      sdgis sample ABC_Licenses -n 10 -f json
    """
    n = n_arg if n_arg is not None else n_opt
    ctx.invoke(query, dataset=dataset, limit=n, layer=layer, fmt=fmt)


# ── download ───────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("-f", "--format", "fmt", default="geojson",
              type=click.Choice(["geojson", "csv", "shapefile", "filegdb", "metadata"]),
              help="Download format")
@click.option("-o", "--output", default=None, help="Output file path")
@click.pass_context
def download(ctx, dataset, fmt, output):
    """Download pre-built dataset exports from SANDAG.

    \b
    Available formats (-f):
      geojson    — GeoJSON file (.geojson)
      csv        — CSV attributes only (.csv)
      shapefile  — Zipped Shapefile (.zip)
      filegdb    — File Geodatabase (.gdb.zip)
      metadata   — Metadata XML (.xml)

    \b
    Examples:
      sdgis download Bikeways
      sdgis download Bikeways -f shapefile -o bikeways.zip
      sdgis download Bikeways -f csv

    \b
    Note: Not all datasets have all formats available.
    Use 'sdgis query-all' for live data export instead.
    """
    session = ctx.obj["session"]
    suffix = EXPORT_FORMATS[fmt]
    url = f"{DOWNLOAD_BASE}/{dataset}{suffix}"

    if output is None:
        output = f"{dataset}{suffix}"

    err_console.print(f"[dim]Downloading {url}")

    with err_console.status(f"Downloading {dataset} ({fmt})..."):
        r = session.get(url, timeout=120, stream=True)
        if r.status_code == 404 or "Download_not_available" in r.url:
            raise click.ClickException(
                f"Format '{fmt}' is not available for {dataset}. Try a different format."
            )
        r.raise_for_status()

        with open(output, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    size = os.path.getsize(output)
    size_str = f"{size / 1024:.1f} KB" if size < 1048576 else f"{size / 1048576:.1f} MB"
    console.print(f"[green]✓[/] Saved to [bold]{output}[/] ({size_str})")


# ── categories ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("-f", "--format", "fmt", default="table", type=click.Choice(["table", "json"]), help="Output format (table or json)")
@click.option("--json-output", "as_json", is_flag=True, hidden=True)
def categories(fmt, as_json):
    as_json = as_json or fmt == "json"
    """List dataset categories used in the Data Warehouse."""
    if as_json:
        click.echo(json.dumps(CATEGORIES))
        return

    table = Table(title="Dataset Categories", box=box.ROUNDED)
    table.add_column("#", style="dim", width=4)
    table.add_column("Category", style="cyan bold")

    for i, cat in enumerate(CATEGORIES, 1):
        table.add_row(str(i), cat)

    console.print(table)
    console.print(f"\n[dim]Use [bold]sdgis search <category>[/bold] to find datasets in a category.")


# ── url ────────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--type", "url_type", default="rest",
              type=click.Choice(["rest", "portal", "map", "geojson", "csv", "shapefile", "metadata"]),
              help="URL type to generate")
@click.pass_context
def url(ctx, dataset, url_type):
    """Generate URLs for a dataset.

    \b
    Examples:
      sdgis url Bikeways --type rest
      sdgis url Bikeways --type map
      sdgis url Bikeways --type geojson
    """
    urls = {
        "rest": service_url(dataset),
        "portal": f"{PORTAL_URL}/home/item.html?id=",
        "map": f"{PORTAL_URL}/apps/mapviewer/index.html?layers=",
        "geojson": f"{DOWNLOAD_BASE}/{dataset}.geojson",
        "csv": f"{DOWNLOAD_BASE}/{dataset}.csv",
        "shapefile": f"{DOWNLOAD_BASE}/{dataset}_shapefile.zip",
        "metadata": f"{DOWNLOAD_BASE}/{dataset}.pdf",
    }

    click.echo(urls[url_type])


# ── head ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("dataset")
@click.option("--layer", default=0, help="Layer index")
@click.option("-f", "--format", "fmt", default="table",
              type=click.Choice(["table", "json", "csv"]),
              help="Output format")
@click.pass_context
def head(ctx, dataset, layer, fmt):
    """Preview a dataset: first 3 records + all field names and types.

    \b
    Use this for a quick first look at an unfamiliar dataset.
    For more records use 'sdgis sample -n 10', for full schema use 'sdgis info'.

    \b
    Examples:
      sdgis head Bikeways
      sdgis head ABC_Licenses -f json
    """
    session = ctx.obj["session"]

    with err_console.status(f"Previewing {dataset}..."):
        try:
            lyr = get_layer_info(session, dataset, layer)
            data = query_features(session, dataset, result_record_count=3, layer=layer)
        except requests.exceptions.RequestException as e:
            handle_request_error(e, dataset)

    features = data.get("features", [])

    if fmt == "json":
        fields_info = [
            {"name": f["name"], "alias": f.get("alias", ""), "type": f.get("type", "").replace("esriFieldType", "")}
            for f in lyr.get("fields", [])
        ]
        click.echo(json.dumps({
            "name": lyr.get("name", dataset),
            "geometry_type": lyr.get("geometryType", ""),
            "max_record_count": lyr.get("maxRecordCount"),
            "fields": fields_info,
            "sample": [f.get("attributes", {}) for f in features],
        }, indent=2))
        return

    if fmt == "csv":
        click.echo(features_to_csv_str(features))
        return

    # Table output
    geom_type = lyr.get("geometryType", "?").replace("esriGeometry", "")
    all_fields = lyr.get("fields", [])
    num_fields = len(all_fields)

    console.print(Panel(
        f"[bold]{lyr.get('name', dataset)}[/]\n"
        f"Geometry: {geom_type}  |  Fields: {num_fields}  |  "
        f"Max/query: {lyr.get('maxRecordCount', '?')}",
        border_style="blue",
    ))

    # Fields table (always vertical — readable even for 60+ field datasets)
    field_table = Table(box=box.SIMPLE, show_header=True, show_lines=False)
    field_table.add_column("Field", style="cyan", min_width=20)
    field_table.add_column("Type", style="dim", min_width=12)
    field_table.add_column("Alias", style="dim")
    for f in all_fields:
        ftype = f.get("type", "").replace("esriFieldType", "")
        alias = f.get("alias", "")
        fname = f.get("name", "")
        field_table.add_row(fname, ftype, alias if alias != fname else "")
    console.print(field_table)

    if features:
        # Cap sample data table at 6 columns to keep it readable
        attrs = features[0].get("attributes", {})
        field_names = list(attrs.keys())
        show_fields = field_names[:6]
        if len(field_names) > 6:
            console.print(f"[dim]Sample data (first 6 of {len(field_names)} fields):[/]")

        table = Table(box=box.SIMPLE, show_lines=False)
        for fn in show_fields:
            table.add_column(fn, max_width=28, overflow="ellipsis")

        for feat in features:
            a = feat.get("attributes", {})
            table.add_row(*[str(a.get(fn, ""))[:28] if a.get(fn) is not None else "" for fn in show_fields])

        console.print(table)


# ── filter / sql (convenience) ─────────────────────────────────────────────────


@cli.command("filter")
@click.argument("dataset")
@click.argument("where_clause")
@click.option("--fields", default="*", help="Fields to return")
@click.option("--limit", default=50, type=int, help="Max results")
@click.option("-f", "--format", "fmt", default="table",
              type=click.Choice(["table", "json", "geojson", "csv"]))
@click.pass_context
def filter_cmd(ctx, dataset, where_clause, fields, limit, fmt):
    """Filter a dataset by a WHERE clause (shorthand for query --where).

    \b
    Examples:
      sdgis filter Bikeways "bike_class=1" --fields "road_name,bike_class"
      sdgis filter ABC_Licenses "LICENSE_TYPE='21'" -f csv
      sdgis filter Bikeways "jurisdiction='Carlsbad'" --limit 20

    \b
    Shell quoting tip: wrap the WHERE clause in double quotes so the shell
    passes single quotes inside unchanged:
      sdgis filter Bikeways "jurisdiction='City of San Diego'"
    Use 'sdgis values <dataset> <field>' to discover valid field values.
    """
    ctx.invoke(query, dataset=dataset, where=where_clause, fields=fields,
               limit=limit, fmt=fmt)


@cli.command("sql", hidden=True)
@click.argument("dataset")
@click.argument("where_clause")
@click.option("--fields", default="*", help="Fields to return")
@click.option("--limit", default=50, type=int, help="Max results")
@click.option("-f", "--format", "fmt", default="table",
              type=click.Choice(["table", "json", "geojson", "csv"]))
@click.pass_context
def sql(ctx, dataset, where_clause, fields, limit, fmt):
    """Alias for filter (kept for backward compatibility)."""
    ctx.invoke(query, dataset=dataset, where=where_clause, fields=fields,
               limit=limit, fmt=fmt)


# ── map ────────────────────────────────────────────────────────────────────────

@cli.command("map")
@click.argument("dataset")
@click.option("--where", default="1=1", help="SQL WHERE clause")
@click.option("--limit", default=500, type=int, help="Max features to render (default: 500)")
@click.option("--layer", default=0, type=int, help="Layer index")
@click.option("--width", default=1200, type=int, help="Image width in pixels")
@click.option("--height", default=800, type=int, help="Image height in pixels")
@click.option("--color", default=None, help="Feature color (default: red for points, blue for lines, orange for polygons)")
@click.option("-o", "--output", default=None, help="Output PNG path (default: <dataset>.png)")
@click.option("--open", "open_after", is_flag=True, help="Open image after saving")
@click.pass_context
def map_cmd(ctx, dataset, where, limit, layer, width, height, color, output, open_after):
    """Render dataset features as a PNG map image. (Optional — requires staticmap)

    \b
    Uses OpenStreetMap tiles as basemap. Supports points, lines, and polygons.
    Install: pipx inject sdgis-cli staticmap  (or: pip install sdgis-cli[map])

    \b
    Examples:
      sdgis map Bikeways
      sdgis map Bikeways --where "bike_class=1" --color "#e63946"
      sdgis map Affordable_Housing_Inventory -o housing.png --open
      sdgis map Hydrological_Basins --limit 200 --width 1600 --height 1000
    """
    try:
        from staticmap import StaticMap, CircleMarker, Line, Polygon
    except ImportError:
        raise click.ClickException(
            "staticmap is required.\n\n"
            "  pipx inject sdgis-cli staticmap   (if using pipx)\n"
            "  pip install sdgis-cli[map]         (if using pip)"
        )

    session = ctx.obj["session"]

    if output is None:
        output = f"{dataset}.png"

    with err_console.status(f"Fetching {dataset} features..."):
        data = query_features(
            session, dataset, where=where,
            return_geometry=True, result_record_count=limit,
            out_sr=4326, layer=layer,
        )

    features = data.get("features", [])
    if not features:
        raise click.ClickException("No features returned — nothing to map.")

    m = StaticMap(width, height, url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png")

    point_color = color or "red"
    line_color = color or "#1a6fad"
    poly_fill = color or "#f4a22680"
    poly_outline = color or "#e07b00"

    rendered = 0
    for feat in features:
        geom = feat.get("geometry")
        if not geom:
            continue

        if "x" in geom and "y" in geom:
            m.add_marker(CircleMarker((geom["x"], geom["y"]), point_color, 6))
            rendered += 1
        elif "paths" in geom:
            for path in geom["paths"]:
                coords = [tuple(c[:2]) for c in path]
                if len(coords) >= 2:
                    m.add_line(Line(coords, line_color, 2))
                    rendered += 1
        elif "rings" in geom:
            for ring in geom["rings"]:
                coords = [tuple(c[:2]) for c in ring]
                if len(coords) >= 3:
                    m.add_polygon(Polygon(coords, poly_outline, poly_fill, 1))
                    rendered += 1
        elif "points" in geom:
            for pt in geom["points"]:
                m.add_marker(CircleMarker(tuple(pt[:2]), point_color, 6))
                rendered += 1

    if rendered == 0:
        raise click.ClickException("No renderable geometries found in features.")

    with err_console.status("Rendering map..."):
        image = m.render()
        image.save(output)

    size = os.path.getsize(output)
    size_str = f"{size / 1024:.0f} KB"
    console.print(f"[green]✓[/] Saved [bold]{output}[/] ({width}×{height}px, {rendered} features, {size_str})")

    if data.get("exceededTransferLimit"):
        err_console.print(f"[yellow]  ⚠ Only first {limit} features shown. Use --limit to increase.")

    if open_after:
        import subprocess
        subprocess.run(["open", output], check=False)


if __name__ == "__main__":
    cli()
