# Contributing to the PoC Polars Pipeline

This document explains how to set up, run, and extend the PoC Polars pipeline.

## Prerequisites

- Python 3.12+
- PostgreSQL database with the data-inclusion schema
- Access to the source tables (france_travail, soliguide, etc.)

## Installation

### With pip

```bash
cd pipeline/poc_polars

# Install the processings package from datawarehouse (editable)
pip install -e ../../datawarehouse/processings

# Install this package (editable)
pip install -e .
```

### With uv

```bash
cd pipeline/poc_polars
uv sync
```

## Running the Pipeline

### Full pipeline run

```bash
python -m poc_polars.run
```

### Run a specific source

```bash
python -m poc_polars.run --source france-travail
python -m poc_polars.run --source soliguide
```

### Skip enrichments for faster development

```bash
python -m poc_polars.run --skip-enrichments
```

### Run and compare with DBT output

```bash
python -m poc_polars.run --compare
```

### Show enrichments summary

```bash
python -m poc_polars.run --enrichments
```

### Show rejected rows summary

```bash
python -m poc_polars.run --rejected
```

### Skip pipeline, only compare/show summaries

```bash
python -m poc_polars.run --skip-run --compare --enrichments
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://data-inclusion:data-inclusion@localhost:5455/data-inclusion` | PostgreSQL connection URL |

## Schema Version

This pipeline implements **v1** of the data-inclusion schema only.

- Uses `data_inclusion.schema.v1` for validation
- Outputs to tables suffixed with `_v1` (e.g., `marts__structures_v1`)
- Follows the v1 field naming conventions

## Enrichments

The pipeline includes enrichment steps run during the marts phase:

### Geocoding

- Uses the BAN (Base Adresse Nationale) API for geocoding
- Caches results in `poc_enrichments.geocodages` table
- Incremental: only geocodes new/changed addresses or those with low scores
- Applies fixes to address, commune, and coordinates
- Handles Paris, Lyon, Marseille arrondissement codes

### SIRET Validation

- Validates SIRET checksum (Luhn algorithm)
- Looks up SIRETs in the SIRENE database
- Sets `_siret_status` field: `valide`, `fermé-définitivement`, `invalide`, `inconnu`
- Sets `_is_closed` field based on establishment status

### URL Checking

- Checks URLs for validity and redirects
- Incremental: only checks new URLs or those older than 30 days
- Caches results in `poc_enrichments.url_checks` table
- Retries failed URLs up to 10 times
- Processes in batches of 1000 URLs

### Email Checks

- Identifies personal emails (firstname.lastname@provider.com)
- Uses the French first names database
- Sets `_has_pii` field for structures/services with personal emails

### Deduplication

- Finds duplicate structures using machine learning
- Assigns `_cluster_id` to structures in the same cluster
- Uses the trained model from `data-inclusion-processings`

## Output Schemas

| Layer | Schema | Description |
|-------|--------|-------------|
| Staging | `poc_staging` | Parsed raw data |
| Intermediate | `poc_intermediate` | Business logic applied |
| Marts | `poc_marts` | Final validated output |
| Rejected | `poc_rejected` | Rows that failed validation |
| Enrichments | `poc_enrichments` | Cached enrichment data |

## Implemented Sources

| Source | Status | Notes |
|--------|--------|-------|
| france-travail | ✅ Working | Row counts match DBT |
| soliguide | 🟡 Partial | Some validation differences |

## Adding a New Source

1. Create staging transformations in `staging/<source>.py`
2. Create intermediate transformations in `intermediate/<source>.py`
3. Create marts transformations in `marts/<source>.py`
4. Register the source in `run.py`

### Example structure for a new source

```python
# staging/my_source.py
from dataclasses import dataclass
import polars as pl
from ..utils.db import DatabaseConnection

@dataclass
class MySourceStaging:
    db: DatabaseConnection
    source_schema: str = "my_source"
    target_schema: str = "poc_staging"

    def run(self) -> dict[str, pl.DataFrame]:
        ...
```

## Dependencies

- `data-inclusion-processings` - Shared processing utilities
- `data-inclusion-schema` - Official v1 schema for validation
- `polars` - DataFrame operations
- `sqlalchemy` - Database operations
- `psycopg2-binary` - PostgreSQL driver
- `pyarrow` - Required for Polars to Pandas conversion

## Code Style

Follow the guidelines in `/AGENTS.md`:

- No comments explaining "what" the code does
- Self-documenting code through clear naming
- Line length: 88 characters (ruff default)
