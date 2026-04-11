# swimming-chile

Data project for loading, structuring, and analyzing swimming competition results in Chile.

## Overview

This project is focused on transforming raw competition files into a cleaner, queryable data model that supports analysis of swimmers, events, results, and competition history.

It is designed as a practical data workflow, not just a notebook or isolated script. The goal is to move from messy source files to a reusable pipeline and database structure.

## Main goals

- Import competition data from source files
- Standardize event, swimmer, and result records
- Load data into a relational database
- Keep staging and core layers separated
- Enable later analysis and reporting

## Project structure

- `scripts/` → ingestion and pipeline execution
- `sql/` → database objects, schema, and transformations
- `sql/analysis_queries.sql` → sample analysis queries for clubs, athletes, results, and relays
- `data/` or input files → source competition files
- `docs/` → notes, assumptions, and mapping decisions

## Workflow

1. Source files are ingested from Excel / CSV
2. Raw records are loaded into staging tables
3. Cleaning and normalization rules are applied
4. Core entities are populated
5. Data becomes ready for analysis and future dashboards

## Tech stack

- Python
- PostgreSQL
- SQL
- Excel / CSV as source inputs

## Current status

This repository is under active development. Current work is focused on making the ingestion flow more robust, improving data formatting consistency, and preparing cleaner analytical outputs.

## Next steps

- Improve pipeline modularity
- Standardize time/result formatting
- Add validation checks
- Document schema and loading rules
- Add sample queries or a first dashboard layer
