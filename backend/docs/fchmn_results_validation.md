# Validacion automatizada de resultados FCHMN

Este runbook documenta la cadena operativa controlada para resultados FCHMN. Mantiene
separadas las responsabilidades: descubrir URLs, descargar PDFs, parsear,
validar y cargar a core solo cuando se pida explicitamente.

## Principios

- No ejecutar carga a core en pruebas de discovery o descarga.
- No usar `--load` salvo pedido explicito.
- No crear tablas nuevas ni migraciones para esta operacion.
- No versionar PDFs, CSVs completos, Excels ni summaries generados.
- Guardar evidencia auditable en `backend/data/raw/batch_summaries/`.
- Si un documento queda `failed` o `requires_review`, no cargarlo a core.

## Discovery desde pagina de resultados FCHMN

Este comando descubre PDFs de resultados desde la pagina real de resultados y
emite un manifest JSONL local. No descarga, no parsea y no carga.

```powershell
backend\.venv\Scripts\python.exe backend\scripts\scrape_fchmn.py `
  --url https://fchmn.cl/resultados/ `
  --manifest backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD.jsonl `
  --pdf-dir backend\data\raw\results_pdf\fchmn_resultados_e2e `
  --out-dir-root backend\data\raw\results_csv\fchmn_resultados_e2e `
  --limit 5 `
  --json
```

Salida esperada:

- `state`: `discovered`
- `documents`: cantidad de PDFs incluidos en el manifest
- manifest JSONL con `source_url`, `pdf`, `out_dir`, `competition_id` y
  `default_source_id`

## Download separado

Este comando lee el manifest y descarga los PDFs declarados. No parsea, no
valida y no carga.

```powershell
backend\.venv\Scripts\python.exe backend\scripts\download_manifest_pdfs.py `
  --manifest backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_resultados_e2e_YYYYMMDD_download.json `
  --json
```

Salida esperada:

- `state`: `downloaded` cuando al menos un PDF nuevo se descargo y no hubo
  fallos.
- `state`: `skipped` cuando todos los PDFs ya existian y no se uso
  `--overwrite`.
- `state`: `failed` si una descarga falla.
- `state`: `updated` con `--overwrite` cuando el checksum nuevo difiere del
  checksum local anterior.
- `state`: `unchanged` con `--overwrite` cuando el checksum no cambia.
- `state`: `failed` si el manifest no contiene documentos.
- `state_counts`: cantidad de documentos por estado.
- Por documento: bytes descargados y `pdf_sha256`.

Usar `--overwrite` solo cuando se quiera reemplazar PDFs locales ya existentes.
El resumen incluye `previous_pdf_sha256` cuando existia un PDF local antes del
reemplazo.

## Batch validation sin carga

Este comando parsea cada PDF del manifest, evalua compuertas y escribe un
resumen auditable. No carga a core porque no usa `--load`.

```powershell
backend\.venv\Scripts\python.exe backend\scripts\run_results_batch.py `
  --manifest backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_resultados_e2e_YYYYMMDD_batch.json `
  --json
```

Salida esperada para un batch sano:

- manifest `state`: `validated`
- `state_counts`: cantidad de documentos por estado
- cada documento `state`: `validated`
- `issues`: vacio
- `debug_unparsed_lines`: bajo el umbral del contrato
- `commands.load`: `null`

Si el parser falla para un documento, ese documento queda `failed` con issue
`parser_failed` y el resto del manifest continua. Si una compuerta bloquea, el
documento queda `requires_review`.

## Automatizacion segura sin carga

Este comando encadena discovery, download y batch validation. No carga a core y
falla si el resultado final no queda `validated`.

```powershell
backend\.venv\Scripts\python.exe backend\scripts\run_fchmn_results_validation.py `
  --url https://fchmn.cl/resultados/ `
  --run-id fchmn_resultados_YYYYMMDD `
  --limit 5 `
  --json
```

El orquestador escribe:

- manifest: `backend/data/raw/manifests/fchmn_results_validation_<run_id>.jsonl`
- descarga: `backend/data/raw/batch_summaries/fchmn_results_validation_<run_id>_download.json`
- batch: `backend/data/raw/batch_summaries/fchmn_results_validation_<run_id>_batch.json`

El resumen incluye `discovered_documents`. Si discovery no encuentra ningun PDF,
el orquestador termina en `failed` antes de descargar o validar, porque un smoke
sin documentos no entrega evidencia operativa.

Usar este comando para monitoreo o smoke operativo. Para cargar a core, revisar
primero el summary de batch y ejecutar `run_results_batch.py --load` como paso
separado.

Para inventario historico desde la portada WordPress, usar paginacion explicita:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\run_fchmn_results_validation.py `
  --url https://fchmn.cl/ `
  --run-id fchmn_historical_inventory_YYYYMMDD `
  --crawl-pages 7 `
  --limit 200 `
  --json
```

`--crawl-pages` recorre `https://fchmn.cl/`, luego `/page/2/`, etc.; deduplica
PDFs entre paginas y se detiene si una pagina paginada devuelve 404. Sigue sin
cargar a core.

Inventario exploratorio del 2026-04-20:

- `backend/data/raw/manifests/fchmn_historical_discovery_20260420.jsonl`
  descubrio 108 PDFs deduplicados entre 2013 y 2026.
- Discovery por ano desde URL: 2013=1, 2015=8, 2016=3, 2017=2, 2018=17,
  2019=10, 2022=11, 2023=15, 2024=17, 2025=15, 2026=9.
- La clasificacion por nombre de URL fue solo exploratoria y no debe usarse como
  compuerta: Coppa Italia es parte del circuito FCHMN y Copa Cordillera es
  organizada por FCHMN, aunque tenga etapa Chile y etapa Argentina. La compuerta
  real debe modelar circuito/federacion/ambito/sede con datos curados.
- Validaciones parciales sin carga: portada full `validated: 23`; pagina 2 full
  `validated: 14`, `requires_review: 3`, `failed: 4`; Nacional 2026
  `validated: 1`.
- El barrido paginado de portada es una herramienta one-shot para backfill
  historico. Para operacion futura se espera monitorear resultados recientes y
  cambios de checksum, sin recorrer todo el historico en cada corrida.
- Fuentes historicas aun omitidas del discovery paginado: `https://fchmn.cl/sudamericanos-master/`
  y paginas de campeonatos nacionales, por ejemplo
  `https://fchmn.cl/campeonatos-nacionales-master/ii-campeonato-nacional-master-2007/`.

## E2E real validado

Validacion controlada realizada contra `https://fchmn.cl/resultados/`:

- `resultados-coppa-italia-master-2026.pdf`
  - `pdf_sha256`: `07e70c3202dd89d631bb312a76022f3adbd84c1e2787939c027c0b5d93d08256`
  - `event`: 232
  - `result`: 1238
  - `relay_team`: 80
  - `relay_swimmer`: 320
  - `debug_unparsed_lines`: 0
- `resultados-ii-copa-chile.pdf`
  - `pdf_sha256`: `a6c4cb231374d9b263112ecc98e8bc4265a0cb0b3bd9f2b89432ac83a08b3af7`
  - `event`: 133
  - `result`: 546
  - `relay_team`: 26
  - `relay_swimmer`: 104
  - `debug_unparsed_lines`: 0

Ambos quedaron `validated` sin carga a core.

## Regresion parser 0.1.12

El parser `0.1.12` agrega soporte para el layout brasileno "Swim It Up" observado
en el Sudamericano Recife. La regresion amplia FCHMN quedo documentada en:

- `backend/data/raw/batch_summaries/regression_parser_012.json`

Resultado esperado de esa evidencia:

- `state`: `validated`
- `state_counts.validated`: 23
- `failed`: 0
- `requires_review`: 0
- Los 18 PDFs HY-TEK previos siguen validados.
- Los 5 PDFs Recife que antes fallaban ahora quedan validados:

- `resultados-1a-etapa.pdf`
- `resultados-2a-etapa.pdf`
- `resultados-3a-etapa.pdf`
- `resultados-4a-etapa.pdf`
- `resultados-5a-etapa.pdf`

Esto es validacion sin carga. Las competencias internacionales siguen sin
cargarse a core hasta definir compuerta de scope/federacion.

## Carga a core

La carga a core queda fuera del smoke de discovery. Solo se ejecuta con
`--load`, despues de revisar que el batch este `validated` y con credenciales
proporcionadas para esa corrida.

Plantilla:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\run_results_batch.py `
  --manifest backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_resultados_e2e_YYYYMMDD_load.json `
  --load `
  --host localhost `
  --port 5432 `
  --dbname natacion_chile `
  --user postgres `
  --password <password> `
  --schema core `
  --json
```

Los summaries auditables redactan el valor de `--password` en los comandos
registrados.
