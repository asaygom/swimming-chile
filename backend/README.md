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
- `scripts/run_results_batch.py`: valida salidas del parser antes de cargar a core
- `scripts/run_fchmn_results_validation.py`: orquesta discovery, descarga y validacion FCHMN sin cargar a core
- `scripts/scrape_fchmn.py`: descubre enlaces PDF y escribe manifests JSONL sin descargar ni cargar
- `scripts/download_manifest_pdfs.py`: descarga PDFs declarados en un manifest sin parsear ni cargar
- `scripts/freeze_validated_manifest.py`: genera manifests congelados solo con documentos validados y scope curado
- `scripts/run_pipeline_results.py`: carga CSVs a staging y core en PostgreSQL
- `sql/schema.sql`: definicion de tablas core, staging, constraints e indices
- `sql/analysis_queries.sql`: consultas analiticas de ejemplo
- `docs/schema.md`: documentacion logica del modelo vigente
- `docs/parser_contracts.md`: contratos de entrada/salida del parser PDF
- `docs/batch_runner_contract.md`: contrato del batch runner y compuertas
- `docs/fchmn_results_validation.md`: runbook de validacion automatizada FCHMN
- `docs/pre_load_checklist.md`: checklist de backup, wipe y validacion post-load antes de cargas controladas
- `docs/data_artifacts.md`: politica de versionado de datos, raw, staging y fixtures
- `docs/ai_workflow.md`: metodologia para trabajar con IA y retomar contexto entre conversaciones

## Comandos de referencia

Instalar dependencias del backend:

```powershell
python -m pip install -r backend\requirements.txt
```

Descubrir PDFs desde HTML local y escribir un manifest JSONL:

```powershell
python backend\scripts\scrape_fchmn.py `
  --html-file backend\data\raw\fchmn_paginas\resultados.html `
  --base-url https://fchmn.cl/ `
  --manifest backend\data\raw\manifests\fchmn_2026.jsonl `
  --pdf-dir backend\data\raw\results_pdf\fchmn `
  --out-dir-root backend\data\raw\results_csv\fchmn `
  --default-source-id 1
```

El scraper agrupa las rutas generadas por año: `results_pdf\fchmn\<año>\...` y
`results_csv\fchmn\<año>\...`. Por defecto infiere el año desde la URL del PDF;
usa `--year` cuando el año de competencia no coincida con la ruta publicada.
Tambien filtra por defecto PDFs cuya URL contenga `resultado`; usa `--all-pdfs`
solo para inspeccion manual.
Cuando se usa `--url`, puede repetirse para consolidar paginas FCHMN en un solo
manifest deduplicado, por ejemplo resultados, sudamericanos y nacionales.

Descargar PDFs declarados en un manifest:

```powershell
python backend\scripts\download_manifest_pdfs.py `
  --manifest backend\data\raw\manifests\fchmn_2026.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_2026_download.json
```

Parsear un PDF:

```powershell
python backend\scripts\parse_results_pdf.py `
  --pdf ruta\al\resultado.pdf `
  --out-dir backend\data\raw\results_csv\competencia_x `
  --competition-id 1 `
  --default-source-id 1
```

Parsear y validar sin cargar a core:

```powershell
python backend\scripts\run_results_batch.py `
  --pdf ruta\al\resultado.pdf `
  --out-dir backend\data\raw\results_csv\competencia_x `
  --competition-id 1 `
  --default-source-id 1
```

Parsear, validar y cargar solo si las compuertas pasan:

```powershell
python backend\scripts\run_results_batch.py `
  --pdf ruta\al\resultado.pdf `
  --out-dir backend\data\raw\results_csv\competencia_x `
  --competition-id 1 `
  --default-source-id 1 `
  --load `
  --user postgres `
  --password tu_password `
  --summary-json backend\data\raw\batch_summaries\competencia_x.json
```

Procesar un manifest JSONL local:

```powershell
python backend\scripts\run_results_batch.py `
  --manifest backend\data\raw\manifests\fchmn_2026.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_2026.json
```

Flujo manual recomendado de validacion FCHMN:

1. Descubrir enlaces y escribir manifest con `scrape_fchmn.py`.
2. Descargar PDFs del manifest con `download_manifest_pdfs.py`.
3. Parsear y validar el mismo manifest con `run_results_batch.py`.
4. Congelar un manifest curado solo con documentos locales validados y
   `competition_scope=fchmn_local`.
5. Agregar `--load` solo cuando el resumen del batch quede `validated`.

Para congelar un manifest desde un summary de batch:

```powershell
python backend\scripts\freeze_validated_manifest.py `
  --batch-summary backend\data\raw\batch_summaries\fchmn_2026_batch.json `
  --manifest backend\data\raw\manifests\fchmn_2026_frozen_local.jsonl `
  --competition-scope fchmn_local `
  --allow-source-url-file backend\data\raw\manifests\fchmn_2026_allowed_urls.txt
```

Cada linea del manifest debe ser un objeto JSON con una carpeta parseada:

```json
{"input_dir": "backend/data/raw/results_csv/competencia_x"}
```

o con un PDF local y su salida:

```json
{"pdf": "backend/data/raw/results_pdf/competencia_x.pdf", "out_dir": "backend/data/raw/results_csv/competencia_x", "competition_id": 1}
```

Para carga a core, agregar `competition_scope` por documento. Por defecto
`--load` exige `competition_scope=fchmn_local`; documentos sin scope o con scope
distinto quedan `requires_review`.
Antes de una primera carga o full reload, seguir `docs/pre_load_checklist.md`.

`pdf_path` tambien se acepta como alias de `pdf` en manifests generados por otras herramientas.
Los manifests pueden estar en UTF-8 con o sin BOM.
Las rutas relativas dentro del manifest se resuelven desde la raiz del proyecto,
no desde la carpeta desde donde se ejecuta el comando.

Cargar una carpeta generada por el parser:

```powershell
python backend\scripts\run_pipeline_results.py `
  --input-dir backend\data\raw\results_csv\competencia_x `
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
- Definir el contrato minimo del batch runner con estados y compuertas de calidad.
- Decidir si `record.gender` debe usar el mismo canon competitivo de `event.gender` o mantenerse como catalogo propio.
