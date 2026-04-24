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
  --url https://fchmn.cl/sudamericanos-master/ `
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

`--url` puede repetirse para consolidar paginas especificas de menu,
sudamericanos o campeonatos nacionales en un solo manifest deduplicado. El
scraper sigue sin descargar, parsear ni cargar.

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
  --url https://fchmn.cl/sudamericanos-master/ `
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

La automatizacion segura acepta varias opciones `--url`, pero no etiqueta
automaticamente el scope de carga. Antes de cualquier `--load`, congelar un
manifest curado que excluya documentos `requires_review`/`failed` y agregue
`competition_scope` por documento.

## Manifest congelado para carga

Despues de revisar un summary de batch, generar un archivo de URLs curadas con
una `source_url` local por linea. Luego crear el manifest congelado:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\freeze_validated_manifest.py `
  --batch-summary backend\data\raw\batch_summaries\fchmn_resultados_e2e_YYYYMMDD_batch.json `
  --manifest backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD_frozen_local.jsonl `
  --competition-scope fchmn_local `
  --allow-source-url-file backend\data\raw\manifests\fchmn_resultados_e2e_YYYYMMDD_allowed_urls.txt `
  --json
```

Si la curacion se apoya en varias evidencias de validacion, repetir
`--batch-summary` para consolidarlas en un unico manifest congelado. La
allow-list sigue siendo explicita y el freezer deduplica por `source_url`:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\freeze_validated_manifest.py `
  --batch-summary backend\data\raw\batch_summaries\summary_a.json `
  --batch-summary backend\data\raw\batch_summaries\summary_b.json `
  --manifest backend\data\raw\manifests\fchmn_resultados_YYYYMMDD_frozen_local.jsonl `
  --competition-scope fchmn_local `
  --allow-source-url-file backend\data\raw\manifests\fchmn_resultados_YYYYMMDD_allowed_urls.txt `
  --json
```

El freezer no descarga, no parsea, no valida y no carga. Solo copia documentos
`validated`, excluye `requires_review`/`failed` y agrega `competition_scope`.
Usar `--allow-all-validated` solo si todos los documentos `validated` del summary
ya fueron curados manualmente como locales.

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
- Decision curada posterior: Copa Cordillera / Dual Internacional pertenece al
  circuito master FCHMN aunque una etapa sea Argentina. Los documentos
  `resultados-copa-argentina.pdf` y `resultados-etapa-argentina.pdf` no deben
  excluirse por sede argentina; deben cargarse solo si el manifest congelado los
  marca con scope curado adecuado. Los Sudamericanos se mantienen como flujo
  separado.
- Validaciones parciales sin carga: portada full `validated: 23`; pagina 2 full
  `validated: 14`, `requires_review: 3`, `failed: 4`; Nacional 2026
  `validated: 1`.
- Para auditorias de brechas, tratar como evidencia principal el discovery
  historico deduplicado, los summaries `*_download.json` con checksums, los
  summaries `*_batch.json` revisados y las regresiones amplias del parser.
  Corridas `scratch`, probes, smoke tests y rechecks puntuales son evidencia
  exploratoria: conservarlas mientras ayuden a diagnosticar, pero no usarlas
  como manifest canonico ni como autorizacion de carga.
- La auditoria local de brechas puede reproducirse sin red y sin carga a core:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\audit_fchmn_artifacts.py `
  --focus-manifest backend\data\raw\manifests\fchmn_historical_discovery_20260420.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_historical_gap_audit_YYYYMMDD.json `
  --json
```

  El script solo lee manifests, summaries, PDFs locales y carpetas CSV. Clasifica
  documentos como `missing_download`, `missing_parse`, `missing_validation`,
  `requires_review`, `failed`, `validated_local_candidate` o
  `validated_non_local_candidate`. La separacion local/no-local es diagnostica;
  la compuerta final de carga sigue requiriendo `competition_scope` curado.
- Auditoria y descarga historica del 2026-04-20:
  - `fchmn_historical_gap_audit_20260420.json`: 108 documentos; 64
    `missing_download`, 4 `failed`, 3 `requires_review`, 30
    `validated_local_candidate`, 7 `validated_non_local_candidate`.
  - `fchmn_historical_missing_download_20260420.jsonl`: manifest derivado solo
    para los 64 documentos faltantes de descarga. No es manifest de carga.
  - `fchmn_historical_missing_download_20260420_download.json`: 64 PDFs
    descargados con checksum. No parsea ni carga.
  - `fchmn_historical_missing_download_20260420_batch.json`: parseo y validacion
    sin carga de esos 64 PDFs; `validated: 16`, `requires_review: 24`,
    `failed: 24`.
  - `fchmn_historical_gap_audit_after_batch_20260420.json`: foto consolidada del
    historico; `validated_local_candidate: 46`,
    `validated_non_local_candidate: 7`, `requires_review: 27`, `failed: 28`.
  - `fchmn_historical_blockers_20260420.json`: clasificacion diagnostica de
    bloqueadores para priorizar soporte de parser y revision de canon.
  - `fchmn_historical_validated_local_candidates_20260420.txt`: lista
    diagnostica de candidatos locales validados; no es allow-list final para
    `freeze_validated_manifest.py`.
- Foco operativo 2022-2026 definido el 2026-04-21:
  - El año de carpeta inferido desde URL WordPress representa la ruta
    `/uploads/<year>/`, no necesariamente el año de competencia.
  - `fchmn_historical_2022_2026_focus_20260421.jsonl`: 67 documentos del
    inventario historico con ruta de carga WordPress 2022-2026.
  - `fchmn_historical_2022_2026_focus_audit_20260421.json`: 46
    `validated_local_candidate`, 7 `validated_non_local_candidate`, 8
    `requires_review` y 6 `failed`.
  - `fchmn_historical_2022_2026_available_for_scope_review_20260421.csv`:
    53 documentos validados disponibles para curar scope manualmente.
    Incluye dos documentos de Copa Cordillera / Dual Internacional inicialmente
    diagnosticados como no-locales por pista de nombre; deben revisarse como
    circuito master FCHMN, no excluirse automaticamente.
  - `fchmn_historical_2022_2026_blockers_20260421.json`: 14 documentos
    bloqueados que deben resolverse antes de considerar el rango 2022-2026
    completo.
  - Los 14 bloqueados iniciales fueron resueltos despues con el parser `0.1.13`;
    ver `scratch_2022_2026_blockers_recheck_20260421_v3.json`.
  - El cierre de scope no debe usar solo el CSV inicial: tambien debe incorporar
    los documentos desbloqueados por el recheck v3.
  - Los formatos pre-2022 quedan como backlog legacy y no deben bloquear el
    cierre operativo inicial de Fase 4.
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

## Resolucion blockers 2022-2026 parser 0.1.13

El parser `0.1.13` agrega soporte de bajo riesgo para los 14 bloqueados del foco
operativo 2022-2026:

- planillas `Quadathlon`, convertidas a cuatro pruebas 50m canonicas;
- encabezados HY-TEK con `Damas`/`Varones`, `Metros`, `Comb. Ind.` y sufijos de
  record en tiempos;
- variantes de relevo como `4x50 comb`, `combinado relay`, `libre relay` y
  `crol relay`;
- OCR `E vento` en encabezados;
- parciales/splits HY-TEK omitidos como lineas auxiliares;
- layout HY-TEK multi-columna observado en `resultados-iii-copa-lqblo.pdf`.

Evidencia sin carga:

- manifest temporal: `backend/data/raw/manifests/scratch_2022_2026_blockers_recheck_20260421.jsonl`
- summary final: `backend/data/raw/batch_summaries/scratch_2022_2026_blockers_recheck_20260421_v3.json`
- resultado esperado: `state_counts.validated = 14`

Los Sudamericanos siguen siendo flujo separado de scope; que validen no autoriza
carga al manifest local principal.

## Correccion parser 0.1.15 para clubes abreviados/multicolumna

El parser `0.1.15` corrige problemas detectados despues de la primera carga del
manifest local congelado:

- layout HY-TEK a dos columnas observado en `resultados-copa-uc-master.pdf`,
  que antes podia fusionar dos resultados en un solo `club_name`;
- lineas OCR fragmentadas en HY-TEK multicolumna, por ejemplo letras separadas
  en nombres/equipos y tiempos;
- fragmentos de encabezado HY-TEK partidos por columna que antes quedaban como
  lineas no parseadas.
- edad adulta duplicada/parcialmente segmentada antes del club, observada como
  `Rojas, 2 20 Escuela de Suboficiales del Ej`, que ahora conserva el club
  limpio sin resolverlo por alias.

Evidencia sin carga en scratch:

- `resultados-copa-uc-master.pdf`: `validated`, `debug_unparsed_lines = 0`,
  `club = 32`;
- `resultados-torneo-apertura-master-2022.pdf`: `validated`,
  `debug_unparsed_lines = 0`, `club = 38`;
- `resultados-iii-copa-lqblo.pdf`: `validated`, `debug_unparsed_lines = 0`,
  `club = 16`.

Estos PDFs siguen usando codigos abreviados de equipo en el resultado fuente.
Los codigos deben resolverse con aliases curados antes de una recarga a core,
no con fuzzy automatico.

## Correccion parser 0.1.16 para nombres de atletas/relevos

El parser `0.1.16` corrige artefactos OCR acotados en nombres de atletas y
nadadores de relevo. Las reglas nuevas solo aplican cuando hay evidencia de
layout o respaldo de pruebas individuales del mismo club/genero/edad; no
reescriben texto fuente como `Rojas, 2`.

Evidencia sin carga:

- scratch parser:
  `backend/data/raw/results_csv/scratch_parser_016_athlete_audit_20260422/`
- auditoria vigente:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_audit_20260422_v8_rivas_verified.json`
- review CSV vigente:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_review_20260422_v8_rivas_verified.csv`
- resultado: 61 documentos, 15 overrides scratch, 97342 observaciones de
  nombres y 2 filas sospechosas restantes, ambas de `Rojas, 2`.

Las filas `Rojas, 2` se conservan sin limpieza automatica porque corresponden
al texto fuente.

Auditoria posterior de `core.club` esperado sin carga:

- despues del fix de parser para `20 Escuela...`, ordenar `club_alias.csv` por
  `canonical_name` y aplicar los aliases curados actuales, la simulacion del
  manifest local congelado baja a 356 filas esperadas en `core.club`;
- al aplicar como aliases explicitos los 42 grupos fuertes de duplicados
  logicos, la simulacion baja a 293 filas esperadas en `core.club` y quedan
  0 grupos fuertes pendientes;
- tras la curacion adicional manual de `club_alias.csv`, la foto vigente baja a
  258 filas esperadas en `core.club`, mantiene 0 grupos fuertes pendientes y
  deja 12 codigos/abreviaturas por curar;
- `A84` se conserva como codigo fuente porque asi aparece en los PDFs revisados.

Evidencia vigente:

- `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_expected_core_club_audit_after_current_aliases_20260422_v10_manual_review_aliases.json`
- `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_expected_core_club_after_current_aliases_20260422_v10_manual_review_aliases.csv`
- `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_expected_core_club_unresolved_codes_after_current_aliases_20260422_v10_manual_review_aliases.csv`

## Auditoria de clubes por atleta-año

El detector de grupos fuertes de `core.club` esperado usa similitud de nombres
y aliases ya aplicados. Un resultado de 0 grupos fuertes pendientes no prueba
que no existan relaciones semanticas pendientes; solo indica que esa compuerta
no encontro mas pares con sus reglas actuales.

Para revisar relaciones adicionales y validar aliases ya aplicados, usar la
auditoria por atleta-año. Esta auditoria cruza `athlete.csv` de cada documento
del manifest, aplica `club_alias.csv` y reporta:

- pares de clubes canonicos distintos donde el mismo atleta aparece en mas de
  un club dentro del mismo año de competencia;
- grupos de aliases ya colapsados que conservan evidencia de variantes raw y
  atletas compartidos.

Ejemplo sin carga:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\audit_club_athlete_year_overlap.py `
  --manifest backend\data\raw\manifests\fchmn_historical_2022_2026_frozen_local_20260421.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_historical_2022_2026_club_athlete_year_overlap_YYYYMMDD.json `
  --candidate-csv backend\data\raw\batch_summaries\fchmn_historical_2022_2026_club_athlete_year_overlap_candidates_YYYYMMDD.csv `
  --alias-evidence-csv backend\data\raw\batch_summaries\fchmn_historical_2022_2026_club_athlete_year_overlap_alias_evidence_YYYYMMDD.csv `
  --min-shared-athletes 2 `
  --json
```

La salida es evidencia para revision humana. Un atleta puede cambiar de club
entre años; por eso la comparacion se limita al mismo `competition_year`. La
evidencia positiva exige competencias distintas del mismo año; si dos clubes
aparecen para el mismo atleta dentro del mismo `source_url`, el auditor lo
cuenta como conflicto intra-competencia excluido, no como relacion de alias.

Evidencia vigente sin carga:

- summary final:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_club_athlete_year_overlap_20260422_v10_manual_review_aliases.json`
- candidates CSV final:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_club_athlete_year_overlap_candidates_20260422_v10_manual_review_aliases.csv`
- resultado: 61 documentos, 23271 observaciones de atleta, 31 pares candidatos,
  40 conflictos intra-competencia excluidos, 96 grupos de aliases con multiples
  variantes raw y 0 `missing_athlete_csv_documents`.

Revision humana posterior: esos 31 pares candidatos no deben aplicarse como
aliases. La revision confirmo que no tienen relacion nominal suficiente entre
clubes y que representan cambios de club de atletas dentro del mismo año, no
duplicados de `core.club`. Mantenerlos omitidos salvo nueva evidencia externa o
una correccion de parser/fuente que cambie el diagnostico.

La simulacion de `core.club` esperado, recalculada despues de esas curaciones
contra el manifest local congelado y los overrides scratch del parser 0.1.15,
quedo inicialmente en 221 filas. Tras resolver codigos puntuales (`NAUTI`,
`UCM`, `UCMAU`, `UC Maule` y `Magal`) y aplicar variantes revisadas manualmente
como `Aquamacul`, `Club Deportivo Aleman de Valp`, `Club Dpto Recreativo`,
`Club Deportivo Ufro`, `Club Recrear Macul`, `Elite Sports Management`,
`Millahire`, `Nautilus Master`, `Salmoán Swim`, `Smart Swin Team`, `Smartswim`
y `Sve Hamburg`, queda en:

- summary:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_expected_core_club_audit_after_current_aliases_20260422_v10_manual_review_aliases.json`
- `expected_core_club_rows_after_core_match_key`: 199
- `unresolved_code_like_clubs`: 13 por heuristica amplia; esta lista mezcla
  codigos reales pendientes con nombres cortos validos, por lo que debe
  revisarse manualmente antes de aplicar aliases.
- Pares aun separados por requerir evidencia adicional o decision humana:
  `Acuaótico Cordillera`/`Acuaticos`, `Master del Ñielol`/`Ñielol Sin
  Fronteras`, `Club Panguipulli`/`Pangu`, y
  `Patagonia`/`Patagonia Kumen Coyhaique`.

## Auditoria de nombres de atletas

Antes de una recarga completa, auditar nombres de atletas desde los CSVs
parseados actuales. Esta auditoria no resuelve identidad probabilistica; solo
detecta señales de extraccion sospechosa que deben clasificarse como fix de
parser, ruido valido del PDF o deuda de Fase 5.

Ejemplo sin carga:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\audit_athlete_names.py `
  --manifest backend\data\raw\manifests\fchmn_historical_2022_2026_frozen_local_20260421.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_historical_2022_2026_athlete_name_audit_YYYYMMDD.json `
  --review-csv backend\data\raw\batch_summaries\fchmn_historical_2022_2026_athlete_name_review_YYYYMMDD.csv `
  --json
```

Cuando existan salidas scratch generadas por un parser mas nuevo que las
carpetas apuntadas por el manifest congelado, usar `--override-input-dir`
repetido por `source_url` para medir la condicion actual sin mezclar errores ya
corregidos.

Evidencia vigente sin carga:

- summary:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_audit_20260422_v8_rivas_verified.json`
- review CSV:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_review_20260422_v8_rivas_verified.csv`
- resultado: 61 documentos, 15 overrides scratch, 97342 observaciones de
  nombres y 2 filas sospechosas restantes, ambas de `Rojas, 2`.

## Curaduria post-parser/pre-load de nombres de atletas

Cuando el parser ya no reduce mas ruido sin meter heuristicas frágiles,
consolidar variantes OCR en una etapa separada y auditable, antes del load:

```powershell
backend\.venv\Scripts\python.exe backend\scripts\curate_athlete_names.py `
  --manifest backend\data\raw\manifests\scratch_fchmn_historical_2022_2026_frozen_local_athlete_preview_20260423.jsonl `
  --summary-json backend\data\raw\batch_summaries\fchmn_historical_2022_2026_athlete_name_curation_20260423.json `
  --review-csv backend\data\raw\batch_summaries\fchmn_historical_2022_2026_athlete_name_curation_20260423.csv `
  --json
```

- summary:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_curation_20260423.json`
- review CSV:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_athlete_name_curation_20260423.csv`
- resultado: 61 documentos, 97341 observaciones de nombres, 333 grupos de
  variantes y 129 reemplazos propuestos.

Esta etapa no modifica el parser ni carga a core. Sirve para consolidar
variantes tipo `Goámez/Goémez/Goómez -> Gomez`, `Muüller -> Muller` y
`Pasaríán/Pasaríón -> Pasarin` antes de una recarga. Los casos sin una variante
corroborada dentro del manifest siguen requiriendo decision humana o parser.

## Scope congelado 2022-2026 sin carga

Curacion operativa del 2026-04-21:

- allow-list local:
  `backend/data/raw/manifests/fchmn_historical_2022_2026_frozen_local_allowed_urls_20260421.txt`
- manifest local congelado:
  `backend/data/raw/manifests/fchmn_historical_2022_2026_frozen_local_20260421.jsonl`
- validacion local sin carga:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_frozen_local_validation_20260421.json`
- resultado local esperado: `state_counts.validated = 61`
- revalidacion real con parser `0.1.16` sobre los 61 PDFs del manifest en rutas
  canonicas `results_csv/fchmn_auto/<anio>/...`:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_frozen_local_validation_20260423_parser016_canonical.json`
  con `state_counts.validated = 61`

La allow-list local incluye los candidatos locales validados, las etapas
Copa Cordillera / Dual Internacional Argentina como circuito master FCHMN, y los
documentos locales desbloqueados en `scratch_2022_2026_blockers_recheck_20260421_v3.json`.

Flujo Sudamericano separado:

- allow-list Sudamericano:
  `backend/data/raw/manifests/fchmn_historical_2022_2026_sudamericano_allowed_urls_20260421.txt`
- manifest Sudamericano:
  `backend/data/raw/manifests/fchmn_historical_2022_2026_frozen_sudamericano_20260421.jsonl`
- validacion Sudamericano sin carga:
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_frozen_sudamericano_validation_20260421.json`
- resultado Sudamericano esperado: `state_counts.validated = 6`

Estos manifests fueron congelados desde allow-lists explicitas de `source_url`.
No descargan, no parsean de nuevo, no cargan a core y no sustituyen el checklist
pre-carga.

## Carga a core

La carga a core queda fuera del smoke de discovery. Solo se ejecuta con
`--load`, despues de revisar que el batch este `validated` y con credenciales
proporcionadas para esa corrida.

Antes de cargar, aplicar `backend/sql/migrations/002_competition_scope.sql` si
la base existente aun no tiene `competition.competition_scope`.

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

La carga requiere una compuerta de scope curado:

```json
{"pdf": "backend/data/raw/results_pdf/fchmn/2026/resultados-ii-copa-chile.pdf", "out_dir": "backend/data/raw/results_csv/fchmn/2026/resultados-ii-copa-chile", "competition_scope": "fchmn_local"}
```

Por defecto `run_results_batch.py --load` solo carga documentos con
`competition_scope=fchmn_local`. Documentos sin scope o con scope distinto quedan
`requires_review` y no ejecutan el pipeline.
