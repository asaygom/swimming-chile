# Proyecto: Natación Chile - FCHMN

## Objetivo
Extraer resultados de competencias master desde PDFs de FCHMN, normalizarlos y cargarlos a PostgreSQL.

## Flujo actual
1. `backend/scripts/parse_results_pdf.py`
   - Lee PDFs de resultados.
   - Extrae individuales y relevos.
   - Genera CSVs:
     - `club.csv`
     - `event.csv`
     - `athlete.csv`
     - `result.csv`
     - `relay_team.csv`
     - `relay_swimmer.csv`
     - además archivos raw/debug

2. `backend/scripts/run_pipeline_results.py`
   - Lee esos CSVs.
   - Carga staging y luego core en PostgreSQL.

3. `backend/scripts/run_results_batch.py`
   - Orquesta Fase 4 inicial.
   - Puede validar una carpeta ya parseada con `--input-dir`.
   - Puede ejecutar el parser desde un PDF con `--pdf` + `--out-dir` y luego validar.
   - No carga a core por defecto; solo carga con `--load` si el batch queda `validated`.

## Estado actual
- El parser ya soporta layouts FCHMN tipo HY-TEK.
- Soporta:
  - `LC Meter` y `SC Meter`
  - individuales y relevos
  - `seed_time`
  - `result_time`
  - `age_at_event`
  - `birth_year_estimated`
  - `points` cuando existan
- El pipeline ya carga:
  - `club`
  - `event`
  - `athlete`
  - `result`
  - `relay_result`
  - `relay_result_member`
- Fase 3 quedo cerrada para normalizaciones compartidas de bajo riesgo:
  - tiempos
  - generos
  - estilos
  - status
- Fase 4 esta iniciada:
  - contrato minimo de batch runner y compuertas
  - validacion previa a carga
  - parseo automatico desde PDF antes de validar
  - carga a core explicita con `--load` protegida por compuertas
  - resumen auditable opcional con `--summary-json`
  - manifest JSONL local con multiples documentos antes del scraper
  - entradas de manifest con `input_dir`, `pdf` o `pdf_path`

## Canon de datos
### event.gender
- `women`
- `men`
- `mixed`

### athlete.gender
- `female`
- `male`

### event.stroke
- `freestyle`
- `backstroke`
- `breaststroke`
- `butterfly`
- `individual_medley`
- `medley_relay`
- `freestyle_relay`

## Reglas importantes
- No usar el club como identidad rígida del atleta a largo plazo.
- `age_at_event` es contextual al evento.
- `birth_year_estimated = competition_year - age_at_event`
- El parser corrige errores típicos de extracción PDF.
- El pipeline debe hacer solo limpieza genérica, no heurísticas agresivas del PDF.
- Las compuertas duras de Fase 4 viven antes de `run_pipeline_results.py`.
- Si `run_results_batch.py` devuelve `requires_review`, no se debe cargar a core.
- `--load` solo debe ejecutar `run_pipeline_results.py` cuando el estado previo sea `validated`.
- No guardar passwords en resumenes auditables; los comandos deben ir con password redactado.
- Manifest procesa documentos uno a uno; un `requires_review` no debe contaminar otros documentos.
- El scraper FCHMN debe emitir manifests JSONL y no debe parsear ni cargar a core.

## Archivos clave
- `backend/scripts/parse_results_pdf.py`
- `backend/scripts/run_results_batch.py`
- `backend/scripts/run_pipeline_results.py`
- `backend/sql/schema.sql`
- `backend/docs/schema.md`
- `backend/docs/batch_runner_contract.md`
- `backend/docs/ai_workflow.md`

## Forma de trabajar
- No renombrar archivos innecesariamente.
- Hacer cambios mínimos y localizados.
- Explicar primero el diagnóstico y luego proponer el patch.
- Si cambia una regex o lógica de parseo, agregar comentario breve.
- Mantener compatibilidad con los PDFs ya validados.
- No saltar fases del `implementation_plan.md`.
- En Fase 4, mantener descarga, parseo, validacion y carga separadas.
- Despues de ejecutar cambios siempre revisar git status y proponer commit
