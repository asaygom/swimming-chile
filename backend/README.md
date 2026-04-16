# Natacion Chile backend

Backend de datos para extraer, normalizar y cargar resultados de competencias de natacion en Chile, con foco inicial en resultados master publicados por FCHMN.

## Objetivo

Convertir PDFs de resultados en datos consultables en PostgreSQL, manteniendo una separacion clara entre:

- archivos fuente y salidas raw/debug
- CSVs normalizados
- tablas staging
- tablas core listas para analisis

## Flujo actual

1. `scripts/parse_results_pdf.py` lee PDFs de resultados tipo HY-TEK/FCHMN.
2. El parser genera CSVs operativos:
   - `club.csv`
   - `event.csv`
   - `athlete.csv`
   - `result.csv`
   - `relay_team.csv`
   - `relay_swimmer.csv`
3. Tambien genera archivos de trazabilidad:
   - `raw_results.csv`
   - `raw_relay_team.csv`
   - `raw_relay_swimmer.csv`
   - `debug_unparsed_lines.csv`
   - `metadata.json`
4. `scripts/run_pipeline_results.py` carga los CSVs a staging.
5. El pipeline inserta datos normalizados en core:
   - `club`
   - `event`
   - `athlete`
   - `result`
   - `relay_result`
   - `relay_result_member`

## Capacidades soportadas

- resultados individuales
- resultados de relevos
- integrantes de relevos por orden de posta
- cursos `LC Meter` y `SC Meter`
- `seed_time_text` y `seed_time_ms`
- `result_time_text` y `result_time_ms`
- `age_at_event`
- `birth_year_estimated`
- `points` cuando existen en la fuente
- normalizacion de genero y estilo a catalogos canonicos

## Estructura

- `scripts/parse_results_pdf.py`: parser de PDFs y generador de CSVs
- `scripts/run_pipeline_results.py`: carga CSVs a staging y core en PostgreSQL
- `sql/schema.sql`: definicion de tablas core, staging, constraints e indices
- `sql/analysis_queries.sql`: consultas analiticas de ejemplo
- `docs/schema.md`: documentacion logica del modelo vigente
- `docs/parser_contracts.md`: contratos de entrada/salida del parser PDF
- `docs/data_artifacts.md`: politica de versionado de datos, raw, staging y fixtures

## Comandos de referencia

Parsear un PDF:

```powershell
python backend\scripts\parse_results_pdf.py `
  --pdf ruta\al\resultado.pdf `
  --out-dir backend\data\parsed\competencia_x `
  --competition-id 1 `
  --default-source-id 1
```

Cargar una carpeta generada por el parser:

```powershell
python backend\scripts\run_pipeline_results.py `
  --input-dir backend\data\parsed\competencia_x `
  --competition-id 1 `
  --default-source-id 1
```

El pipeline tambien puede recibir CSVs explicitos con flags como `--club-csv`, `--event-csv`, `--athlete-csv`, `--result-csv`, `--relay-team-csv` y `--relay-swimmer-csv`.

## Modelo de datos

El esquema mantiene tablas separadas para resultados individuales y relevos:

- `result`: resultados individuales por atleta
- `relay_result`: resultado del equipo de relevo
- `relay_result_member`: integrantes del relevo y orden de posta

La documentacion completa del esquema esta en `docs/schema.md`.

## Reglas importantes

- No usar el club como identidad rigida del atleta a largo plazo.
- `age_at_event` es contextual al evento.
- `birth_year_estimated = competition_year - age_at_event`.
- El parser concentra heuristicas propias de extraccion PDF.
- El pipeline debe hacer limpieza generica, normalizacion y carga, evitando heuristicas agresivas dependientes del PDF.

## Propuestas de mejoras proximas

- Agregar tests con fixtures pequenos para validar carga end-to-end de individuales y relevos.
- Definir constraints de unicidad para cargas repetidas de `result` y `relay_result`.
- Documentar comandos operativos con un ejemplo real de competencia.
- Separar en un modulo comun las normalizaciones compartidas por parser y pipeline.
- Decidir si `record.gender` debe usar el mismo canon competitivo de `event.gender` o mantenerse como catalogo propio.
