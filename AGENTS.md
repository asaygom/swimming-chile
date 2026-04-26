# Proyecto: Natación Chile - FCHMN

## Rol de este archivo
`AGENTS.md` contiene reglas operativas para agentes que trabajan en este repositorio. Debe ser corto, imperativo y orientado a ejecucion.

Para metodologia, continuidad entre conversaciones y estado extendido, leer `backend/docs/ai_workflow.md`. No usar este archivo como changelog largo.

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
   - Valida salidas del parser antes de cargar a core.
   - Puede validar una carpeta ya parseada con `--input-dir`.
   - Puede ejecutar el parser desde un PDF con `--pdf` + `--out-dir` y luego validar.
   - No carga a core por defecto; solo carga con `--load` si el batch queda `validated`.

4. `backend/scripts/scrape_fchmn.py`
   - Descubre enlaces PDF desde HTML local o URL.
   - Emite manifests JSONL con `source_url`, `pdf` y `out_dir`.
   - No descarga, no parsea ni carga a core.

5. `backend/scripts/download_manifest_pdfs.py`
   - Descarga PDFs declarados en un manifest desde `source_url`.
   - Calcula `pdf_sha256` y escribe resumen auditable.
   - Con `--overwrite`, reporta `updated` si cambia el checksum y `unchanged` si no cambia.
   - No parsea, no valida CSVs ni carga a core.

6. `backend/scripts/run_fchmn_results_validation.py`
   - Orquesta discovery -> download -> batch validation para resultados FCHMN.
   - No acepta ni pasa `--load`.
   - Es la automatizacion segura actual; la carga a core sigue siendo explicita y separada.

7. `backend/scripts/freeze_validated_manifest.py`
   - Lee summaries de batch validation.
   - Genera manifests congelados solo con documentos `validated`.
   - Excluye documentos `requires_review` y `failed`.
   - Agrega `competition_scope` curado.
   - No descarga, no parsea, no valida CSVs ni carga a core.

8. `backend/scripts/curate_athlete_names.py`
   - Consume manifests de carpetas parseadas, agrupa variantes OCR por firma robusta de nombre.
   - Propone reemplazos auditables pre-load sin tocar parser ni cargar a core.
   - Materializa CSVs curados en carpeta separada con manifest nuevo.
   - Aplica OCR name rules, reparaciones deterministicas de residuos OCR conocidos, correcciones de birth_year, consolidaciones de birth_year faltante, consolidaciones de nombres parciales y canonizacion de orden de nombre.
   - Usa `canonicalize_space_ordered_name` para transformar nombres `Nombre Apellido` a `Apellido, Nombre` cuando no hay coma ni digitos.
   - No descarga, no parsea, no valida CSVs ni carga a core.

## Estado actual
- El trabajo activo del proyecto esta en backend/data pipeline.
- `frontend/` existe como area planificada para Fase 6, pero todavia no tiene implementacion activa ni reglas propias.
- Cuando empiece trabajo real de frontend, crear documentacion especifica en `frontend/README.md` y, si hace falta, `frontend/AGENTS.md`.
- El parser ya soporta layouts FCHMN tipo HY-TEK y layout brasileno "Swim It Up" (Sudamericano Recife).
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
  - scraper de apuntamiento FCHMN separado del downloader
  - downloader separado con checksums y deteccion local de PDFs modificados
  - summaries de manifests con `state_counts`
  - validacion automatizada FCHMN sin carga a core
  - E2E real desde `https://fchmn.cl/resultados/` validado sin cargar a core
  - soporte de layout brasileno "Swim It Up" con parser `0.1.12`
  - regresion completa 23/23 validated sin romper PDFs HY-TEK previos
  - materializacion pre-load de curaciones de atleta con manifest curado
  - compuerta pre-load de residuos OCR de nombres sobre `athlete.csv`, `result.csv` y `relay_swimmer.csv`
  - pipeline usa clave normalizada para deduplicar atletas y enlazar resultados, honrando CSVs curados

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
- Si el parser falla para un documento del manifest, ese documento queda `failed` y no debe detener los demas.
- El scraper FCHMN debe emitir manifests JSONL y no debe parsear ni cargar a core.
- El downloader debe descargar y calcular checksum, pero no debe parsear ni cargar a core.
- `run_fchmn_results_validation.py` automatiza solo hasta batch validation; no debe cargar a core.
- Mantener nombres de producto en scripts, docs operativos, tests y artefactos; evitar nombres transicionales como numeros de fase fuera del roadmap/metodologia.
- Los PDFs con formato no soportado todavia no son ruido: conservarlos como candidatos y agregar soporte de parser con fixtures chicos cuando corresponda.
- Competencias internacionales como los Sudamericano se parsean y validan pero no se cargan a core sin un filtro de ambito/federacion. El parser no decide que cargar; la compuerta de carga debe distinguir circuito local de internacional.
- El proyecto es extensible a multiples fuentes de competencias (FCHMN, Fechida, etc.); la identidad de competencia debe soportar ese crecimiento.

## Archivos clave
- `backend/scripts/parse_results_pdf.py`
- `backend/scripts/scrape_fchmn.py`
- `backend/scripts/download_manifest_pdfs.py`
- `backend/scripts/run_results_batch.py`
- `backend/scripts/run_fchmn_results_validation.py`
- `backend/scripts/freeze_validated_manifest.py`
- `backend/scripts/curate_athlete_names.py`
- `backend/scripts/run_pipeline_results.py`
- `backend/sql/schema.sql`
- `backend/docs/schema.md`
- `backend/docs/batch_runner_contract.md`
- `backend/docs/fchmn_results_validation.md`
- `backend/docs/pre_load_checklist.md`
- `backend/docs/ai_workflow.md`

## Forma de trabajar
- No renombrar archivos innecesariamente.
- Hacer cambios mínimos y localizados.
- Explicar primero el diagnóstico y luego proponer el patch.
- Si cambia una regex o lógica de parseo, agregar comentario breve.
- Mantener compatibilidad con los PDFs ya validados.
- No saltar fases del `implementation_plan.md`.
- En Fase 4, mantener descarga, parseo, validacion y carga separadas.
- Si cambia una regla operativa para agentes, actualizar `AGENTS.md`.
- Si cambia metodologia, estado extendido, handoff o siguiente paso sugerido, actualizar `backend/docs/ai_workflow.md`.
- Si cambia comportamiento tecnico, actualizar tests y el contrato tecnico correspondiente antes de resumir.
- Despues de ejecutar cambios siempre revisar git status y proponer commit
- Antes de una primera carga o full reload, seguir `backend/docs/pre_load_checklist.md`.
