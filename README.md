# sdgis — San Diego Regional Data Warehouse CLI

A command-line tool for exploring, querying, and downloading **360+ GIS datasets** from the [San Diego Regional Data Warehouse](https://geo.sandag.org) maintained by SANDAG and SanGIS.

## Why use this?

The SANDAG data warehouse is one of the most comprehensive public GIS repositories for San Diego County — but it's locked behind a web portal and ArcGIS REST APIs that are painful to work with directly. This CLI makes that data scriptable.

**Use it if you want to:**

- **Research or analyze San Diego** — parcels, zoning, census tracts, bike infrastructure, fire stations, hydrology, affordable housing, business licenses, broadband coverage, and much more
- **Feed data to an AI agent** — all commands output clean JSON to stdout, status goes to stderr, making it easy to pipe into LLM workflows
- **Script data pipelines** — pull live feature data with SQL-style filters, bounding boxes, and pagination; pipe directly to `jq`, `ogr2ogr`, or files
- **Explore what's available** — semantic search across 360 datasets lets you find relevant data without knowing exact dataset names

## Installation

```bash
pipx install sdgis-cli
```

Or with pip:

```bash
pip install sdgis-cli

# For semantic search (recommended):
pip install sdgis-cli[embed]
```

## Setup (first time)

Build the local search index. Downloads the dataset catalog and computes embeddings (~22MB model, takes ~30s):

```bash
sdgis index
```

## Quick Start

```bash
# Semantic search — find relevant datasets without knowing exact names
sdgis search "bike infrastructure"
sdgis search "water and flooding"
sdgis search "affordable housing near transit"

# Understand a dataset before querying it (great for agents)
sdgis describe Bikeways

# Count features (with optional filter)
sdgis count Bikeways
sdgis count ABC_Licenses --where "LICENSE_TYPE='21'"

# Query features
sdgis query Bikeways --limit 5
sdgis query Bikeways --where "RD_NAME='Coast Blvd'" --fields "RD_NAME,CLASS"
sdgis query ABC_Licenses --bbox "-117.2,32.7,-117.1,32.8" --limit 50

# Output as JSON or CSV
sdgis query Bikeways --limit 100 -f json
sdgis query Bikeways --limit 100 -f csv > bikeways.csv
sdgis query Bikeways --limit 100 -f geojson > bikeways.geojson

# Fetch ALL features with automatic pagination
sdgis query-all Bikeways -f geojson > all_bikeways.geojson

# Download pre-built exports
sdgis download Bikeways -f shapefile
```

## Commands

| Command | Description |
|---------|-------------|
| `index` | Build local SQLite index with semantic embeddings |
| `search <query>` | Semantic / FTS / fuzzy search across all datasets |
| `describe <dataset>` | Schema + feature count + sample rows as JSON (agent-friendly) |
| `list` | List all available datasets |
| `info <dataset>` | Show schema, fields, metadata, and links |
| `fields <dataset>` | List all fields with types and domains |
| `head <dataset>` | Quick preview: 3 rows + schema summary |
| `count <dataset>` | Count total features (supports WHERE clause) |
| `query <dataset>` | Query features with filters, pagination, bounding box |
| `query-all <dataset>` | Fetch all features with automatic pagination |
| `sql <dataset> <where>` | Shorthand for WHERE clause queries |
| `download <dataset>` | Download pre-built GeoJSON / CSV / Shapefile / FGDB |
| `url <dataset>` | Generate REST, portal, or download URLs |
| `categories` | List the 18 dataset categories |

## For AI Agents

Every command that returns data outputs **clean JSON to stdout** with no ANSI codes. Status messages go to stderr. This makes it easy to use with any LLM tool framework.

Typical agent workflow:

```bash
# 1. Find relevant datasets
sdgis search "stormwater infrastructure" --json-output

# 2. Understand a dataset's schema and sample data in one call
sdgis describe Hydrological_Basins

# 3. Count matching features before pulling all data
sdgis count Hydrological_Basins --where "AREA_SQMI > 10" --json-output

# 4. Pull the data
sdgis query Hydrological_Basins --where "AREA_SQMI > 10" -f geojson
```

## Dataset Categories

Agriculture, Business, Census, Community, District, Ecology & Parks, Elevation,
Fire, Health & Public Safety, Hydrology & Geology, Jurisdiction, Landbase,
Land Use, Miscellaneous, Place, Transportation, Utilities, Zoning

## Output Formats

- **table** — Rich formatted terminal table (default, human-readable)
- **json** — Raw ArcGIS JSON response
- **geojson** — Standard GeoJSON FeatureCollection
- **csv** — Comma-separated values (attributes only)

## Spatial Queries

Filter by bounding box (WGS84 lon/lat):

```bash
sdgis query ABC_Licenses --bbox "-117.2,32.7,-117.1,32.8" --limit 100 -f geojson
```

## Piping & Scripting

```bash
# Count features in every transportation dataset
sdgis search transportation --json-output | \
  jq -r '.[].name' | \
  while read ds; do
    echo -n "$ds: "
    sdgis count "$ds" --json-output 2>/dev/null
  done

# Convert to GeoPackage with ogr2ogr
sdgis query-all Bikeways -f geojson | ogr2ogr -f "GPKG" bikeways.gpkg /vsistdin/
```

## About the Data Warehouse

SanGIS and SANDAG have partnered to provide the San Diego region with a single authoritative source of GIS data through the **San Diego Regional Data Warehouse**. It contains hundreds of layers across 18 categories, collected from multiple sources including the City of San Diego, the County of San Diego, the State of California, and the federal government — all free for public use.

Datasets cover everything from addresses to zoning: roads/freeways, property and city boundaries, census areas, community planning areas, lakes, streams, business zones, and much more. Data is available as hosted feature services (for interactive viewing and metadata review) and as downloads in FileGDB, Shapefile, CSV, GeoJSON, and JSON formats.

> **Note:** Per California Assembly Bill AB1785, SanGIS no longer publishes parcel owner name and address information in publicly accessible online locations. For parcel owner data or technical issues, contact [webmaster@sangis.org](mailto:webmaster@sangis.org).

Data is provided for convenience with no warranty as to accuracy. Users should review the [SanGIS Legal Notice](https://www.sangis.org/legal-notices) and [SANDAG Privacy Policy](https://www.sandag.org/privacy-policy) prior to use.

## Data Source

All data comes from the **San Diego Regional Data Warehouse** operated by SANDAG (San Diego Association of Governments) and SanGIS.

- Portal: https://geo.sandag.org
- REST Services: https://geo.sandag.org/server/rest/services/Hosted
