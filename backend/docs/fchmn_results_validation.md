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

Usar este comando para monitoreo o smoke operativo. Para cargar a core, revisar
primero el summary de batch y ejecutar `run_results_batch.py --load` como paso
separado.

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

## PDFs descubiertos en portada con formato pendiente

La portada `https://fchmn.cl/` puede publicar PDFs con `resultado` en la URL que
no corresponden al layout HY-TEK soportado actualmente por el parser. El caso
observado corresponde a un Sudamericano Master realizado en Brasil, donde la
publicacion cambio el formato de los resultados:

- `resultados-1a-etapa.pdf`
- `resultados-2a-etapa.pdf`
- `resultados-3a-etapa.pdf`
- `resultados-4a-etapa.pdf`
- `resultados-5a-etapa.pdf`

Estos PDFs descargan correctamente, pero el batch runner debe marcarlos como
`failed` si el parser no extrae filas. No se deben cargar a core.
No deben excluirse por keyword: en futuras competencias, `etapa` puede ser parte
de un resultado valido y este formato tambien deberia soportarse mas adelante.
El camino correcto es conservarlos como candidatos y abrir soporte de parser con
fixtures pequenos cuando se aborde ese layout.

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
