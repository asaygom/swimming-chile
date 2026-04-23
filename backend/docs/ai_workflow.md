# Metodologia de trabajo con IA

Este documento permite retomar el proyecto en otra conversacion sin depender del historial del chat. Resume la forma de trabajo acordada para Natacion Chile.

## Fuentes de verdad

- Hoja de ruta: `implementation_plan.md`.
- Reglas operativas para agentes: `AGENTS.md`.
- Politica de artefactos: `backend/docs/data_artifacts.md`.
- Contratos del parser: `backend/docs/parser_contracts.md`.
- Contrato del batch runner: `backend/docs/batch_runner_contract.md`.
- Validacion automatizada FCHMN: `backend/docs/fchmn_results_validation.md`.
- Checklist pre-carga/full reload: `backend/docs/pre_load_checklist.md`.
- Modelo vigente: `backend/docs/schema.md`.
- Trazabilidad e idempotencia: `backend/docs/traceability_idempotency.md`.

## Principio central

El aprendizaje con IA debe avanzar junto con el plan real del proyecto. Cada sesion debe empujar una fase del producto y explicar el donde, por que y para que de los cambios.

`AGENTS.md` y este documento se complementan:

- `AGENTS.md`: reglas cortas e imperativas para actuar dentro del repo.
- `backend/docs/ai_workflow.md`: memoria metodologica, continuidad entre conversaciones, estado extendido y siguiente paso.
- Los contratos tecnicos viven en docs especificas como `parser_contracts.md`, `batch_runner_contract.md` o `traceability_idempotency.md`.
- Evitar duplicar detalles largos entre documentos; enlazar o resumir cuando baste.

Orden actual del proyecto:

1. Blindar.
2. Trazabilidad e idempotencia.
3. Modularizar.
4. Automatizar con compuertas.
5. Curar identidad de atletas.
6. Exponer producto de datos.

## Flujo obligatorio por cambio

1. Diagnostico:
   - Revisar `git status`.
   - Leer archivos relevantes antes de proponer cambios.
   - Identificar cambios locales del usuario y no sobrescribirlos.
2. Propuesta corta:
   - Explicar el patch antes de editar.
   - Mantener cambios minimos y localizados.
3. Implementacion:
   - Seguir patrones existentes.
   - No refactorizar fuera de alcance.
   - Si cambia regex o parseo, agregar comentario breve.
4. Verificacion:
   - Ejecutar tests relevantes.
   - Ejecutar `py_compile` si se modifican scripts Python.
   - Si toca DB, explicar si falta aplicar migracion.
5. Cierre:
   - Revisar `git status`.
   - Resumir diagnostico, cambios y verificacion.
   - Proponer mensaje de commit.

## Regla de sincronizacion documental

Si cambia comportamiento o contrato:

1. Actualizar tests.
2. Actualizar el contrato tecnico correspondiente.
3. Actualizar `README.md` si afecta uso humano.
4. Actualizar `AGENTS.md` solo si cambia una regla operativa para agentes.
5. Actualizar `backend/docs/ai_workflow.md` si cambia metodologia, handoff, estado extendido o siguiente paso sugerido.

## Estado metodologico actual

Estado vigente:

- Fase activa: Fase 4, scraper y batch runner con compuertas de calidad.
- La hoja de ruta, reglas operativas, politica de artefactos, contratos, schema y runbook FCHMN estan versionados como fuentes de verdad.
- El parser soporta resultados FCHMN tipo HY-TEK y layout brasileno "Swim It Up"; genera CSVs operativos, raw/debug y `metadata.json`.
- El pipeline carga `club`, `event`, `athlete`, `result`, `relay_result` y `relay_result_member`, y registra trazabilidad e issues de validacion.
- La normalizacion compartida de tiempos, generos, estilos y status vive en `backend/natacion_chile/domain/normalization.py`.
- Discovery, descarga, parseo, validacion y carga estan separados: `scrape_fchmn.py`, `download_manifest_pdfs.py`, `run_results_batch.py`, `run_fchmn_results_validation.py` y `run_pipeline_results.py`.
- Los manifests JSONL aceptan `input_dir`, `pdf` o `pdf_path`, conservan `source_url`, resuelven rutas relativas desde la raiz del proyecto y procesan documentos de forma aislada.
- `run_fchmn_results_validation.py` automatiza discovery -> download -> batch validation sin aceptar ni pasar `--load`.
- `freeze_validated_manifest.py` genera manifests congelados desde summaries de batch, incluyendo solo documentos `validated` y agregando `competition_scope` curado.
- `audit_fchmn_artifacts.py` audita brechas locales de Fase 4 cruzando manifests, summaries, PDFs y carpetas CSV sin descargar, parsear ni cargar a core.
- `scrape_fchmn.py` y `run_fchmn_results_validation.py` soportan `--crawl-pages` y multiples `--url` para consolidar fuentes FCHMN en un manifest deduplicado.
- `run_results_batch.py --load` solo ejecuta el pipeline si el documento esta `validated` y su `competition_scope` coincide con el scope requerido (`fchmn_local` por defecto).
- `competition_type` clasifica el tipo deportivo general; `competition_scope`
  clasifica el circuito/ambito curado para filtros de carga y analitica. El
  scope del manifest se persiste en `competition.competition_scope`.

Decisiones vigentes:

- El parser parsea PDFs publicados; no decide si un documento se carga a core.
- La compuerta de carga debe distinguir circuito, federacion, ambito, sede o etapa con datos curados, no con keywords automaticas.
- Coppa Italia pertenece al circuito FCHMN local. Copa Cordillera / Dual Internacional tambien pertenece al circuito master FCHMN, incluyendo etapa Chile y etapa Argentina; no excluirla por sede argentina, pero modelar sede/etapa en scope o metadata curada. Sudamericanos pueden validarse, pero deben tratarse como flujo aparte y no cargarse a core local sin scope adecuado.
- Un documento `failed` o `requires_review` no debe contaminar otros documentos del manifest ni cargarse a core.
- Los PDFs con formato no soportado son candidatos a soporte futuro, no ruido descartable.

Evidencia historica:

- La evidencia auditable de discovery, descarga, validacion, regresiones y cargas previas vive en `backend/data/raw/batch_summaries/` y `backend/data/raw/manifests/`.
- El runbook `backend/docs/fchmn_results_validation.md` conserva los comandos reproducibles y los resultados exploratorios relevantes.
- El checklist `backend/docs/pre_load_checklist.md` ordena backup, wipe controlado, carga y validacion post-load antes de una primera carga o full reload.
- La regresion amplia con parser `0.1.12` dejo `backend/data/raw/batch_summaries/regression_parser_012.json` como evidencia principal de compatibilidad HY-TEK + Swim It Up.
- La auditoria de brechas historicas debe quedar como summary JSON local antes de proponer descargas, reparseos o manifests congelados.
- Al 2026-04-20, el inventario historico FCHMN de 108 documentos ya no tiene `missing_download`: los 64 faltantes fueron descargados con checksums y validados sin carga. La foto consolidada quedo en `fchmn_historical_gap_audit_after_batch_20260420.json`: 46 `validated_local_candidate`, 7 `validated_non_local_candidate`, 27 `requires_review` y 28 `failed`.
- La lista `fchmn_historical_validated_local_candidates_20260420.txt` es diagnostica y no reemplaza una allow-list curada para freeze/load.
- Al 2026-04-21, el cierre operativo de Fase 4 se acoto a formatos 2022-2026. La foto `fchmn_historical_2022_2026_focus_audit_20260421.json` contiene 67 documentos: 46 `validated_local_candidate`, 7 `validated_non_local_candidate`, 8 `requires_review` y 6 `failed`. Los formatos pre-2022 quedan como backlog legacy. Dentro de los 7 `validated_non_local_candidate`, las dos Copa Argentina / Dual Internacional corresponden al circuito master FCHMN y deben reclasificarse manualmente como cargables si el scope curado lo confirma; los 5 Sudamericanos Recife deben revisarse en flujo separado.
- `fchmn_historical_2022_2026_available_for_scope_review_20260421.csv` lista las 53 competencias/documentos validados disponibles para curar scope (`fchmn_local` u otro). Es insumo de revision humana, no compuerta final automatica. Al clasificar, tratar Copa Cordillera / Dual Internacional como circuito master FCHMN, no como exclusion no-local automatica.
- Al 2026-04-21, el parser `0.1.13` resolvio los 14 bloqueados 2022-2026 sin carga a core. La evidencia final es `scratch_2022_2026_blockers_recheck_20260421_v3.json` con `state_counts.validated = 14`. Los cambios cubren Quadathlon, HY-TEK multi-columna, variantes de encabezado/canon y omision de parciales/splits. Los Sudamericanos validados siguen fuera del manifest local principal hasta curar scope separado.
- Al 2026-04-21, se congelo el scope operativo 2022-2026 sin carga a core:
  `fchmn_historical_2022_2026_frozen_local_20260421.jsonl` incluye 61
  documentos con `competition_scope=fchmn_local`, usando allow-list explicita de
  `source_url` y sumando los documentos locales desbloqueados en el recheck v3.
  `fchmn_historical_2022_2026_frozen_sudamericano_20260421.jsonl` separa 6
  documentos con `competition_scope=sudamericano_master`. Las validaciones
  posteriores quedaron `validated`: local 61/61 y Sudamericano 6/6.
- La migracion `backend/sql/migrations/002_competition_scope.sql` ya fue aplicada
  antes de continuar hacia la carga, por lo que `core.competition` puede
  persistir el scope curado.
- La base contiene calendario planificado FCHMN 2026 en `competition` sin
  eventos cargados y datos de `pool` fuera del alcance actual. Para una recarga
  de resultados, usar una limpieza quirurgica que preserve esas filas. Esa
  limpieza operacional ya se ejecuto manualmente y no se conserva como script
  versionado.
- Despues de la primera carga explicita del manifest local congelado se
  detectaron duplicados de identidad observada en `athlete` y variantes/errores
  de club. El patch en curso deduplica atletas por identidad normalizada dentro
  de cada carga y amplia `club_alias.csv` con correcciones curadas de alta
  confianza. La base ya cargada requiere backup, limpieza quirurgica y recarga
  explicita para reflejar estas correcciones.
- El parser `0.1.15` corrige el layout HY-TEK a dos columnas de Copa UC 2022 y
  lineas OCR fragmentadas en multicolumna, reduciendo clubes basura antes de
  aplicar aliases, y repara edad adulta duplicada/parcialmente segmentada antes
  del club sin recurrir a alias. Los formatos abreviados de club en Copa UC
  2022, Torneo Apertura 2022 y LQBLO 2023 quedan validados en scratch sin carga.
- La curacion incremental de `club_alias.csv` debe seguir siendo explicita y
  revisable. Tras ordenar el archivo por `canonical_name`, aplicar aliases
  curados actuales, aplicar los 42 grupos fuertes como aliases explicitos y
  conservar `A84` como codigo fuente, la simulacion local vigente del manifest
  congelado bajo a 199 filas esperadas en `core.club` despues de la curacion por
  atleta-ano y la revision manual de variantes logicas. La evidencia vigente es
  `fchmn_historical_2022_2026_expected_core_club_audit_after_current_aliases_20260422_v10_manual_review_aliases.json`.
- "Sin grupos fuertes pendientes" solo describe la compuerta por similitud de
  nombre vigente. Para detectar relaciones no capturadas por esa regla y validar
  aliases ya aplicados, usar `backend/scripts/audit_club_athlete_year_overlap.py`,
  que cruza atletas por ano de competencia sin cargar a core. La corrida
  corregida `v2_cross_competition` del 2026-04-22 cuenta como evidencia positiva
  solo atletas compartidos entre competencias distintas del mismo ano, excluye
  solapes dentro de una misma competencia y encontro 79 pares candidatos con al
  menos 2 atleta-anos compartidos, 40 conflictos intra-competencia excluidos y
  90 grupos de aliases con multiples variantes raw. Tras aplicar 30 aliases
  seguros desde esa bandeja curada, expandir 7 relaciones hacia canonicals ya
  existentes, aplicar las 6 relaciones restantes curadas por el usuario y
  colapsar aliases que apuntaban a canonicals antiguos, la corrida
  `v7_collapsed_canonical_groups` bajo a 31 pares candidatos, mantuvo 40
  conflictos intra-competencia excluidos y dejo 96 grupos de aliases con
  multiples variantes raw. Esos 31 pares fueron revisados manualmente y se
  descartan como insumo de alias porque no tienen relacion nominal suficiente y
  corresponden a cambios de club; no deben reabrirse salvo nueva evidencia
  externa o correccion de parser/fuente.
- Antes de recargar core, auditar nombres de atletas con
  `backend/scripts/audit_athlete_names.py`. Evidencia vigente:
  `fchmn_historical_2022_2026_athlete_name_audit_20260422_v8_rivas_verified.json`,
  con 2 filas sospechosas restantes, ambas del caso fuente `Rojas, 2`, que se
  conserva sin limpieza automatica.

No implementado todavia:

- Memoria persistente tipo Engram.  https://github.com/Gentleman-Programming/engram 
- Skills de `gentle-ai`. https://github.com/Gentleman-Programming/gentle-ai 
- MCP externo para memoria.
- Orquestacion multiagente.
- Persistencia explicita de estado de batch en tabla operativa propia.

La metodologia del curso se esta aplicando manualmente: Plan Mode, SDD, contratos, tests, docs vivas y human-in-the-loop. Las herramientas Gentleman/Engram quedan como fase posterior, no como dependencia actual.

## Prompt recomendado para retomar

Usar este texto al iniciar una nueva conversacion. 

```text
Lee primero implementation_plan.md, backend/docs/ai_workflow.md,
backend/docs/fchmn_results_validation.md y backend/docs/pre_load_checklist.md.
Luego revisa git status y los docs/scripts relevantes antes de proponer cambios.
Continua segun la metodologia acordada: diagnostico, propuesta corta, patch
minimo, tests, git status y propuesta de commit.
No saltes fases del implementation_plan.md.
Si hay cambios locales del usuario, respetalos y trabaja alrededor de ellos.
Explica el que, el por que, el donde y lo aprendido de cada cambio.
Fase activa: Fase 4. No cargues a core ni uses --load salvo pedido explicito.

Contexto vigente:
- El cierre operativo inicial se acoto a formatos 2022-2026.
- Los formatos pre-2022 quedan como backlog legacy.
- La evidencia operativa vigente esta resumida en backend/docs/fchmn_results_validation.md.
- El manifest local congelado vigente es
  backend/data/raw/manifests/fchmn_historical_2022_2026_frozen_local_20260421.jsonl,
  validado sin --load con 61 documentos y competition_scope=fchmn_local.
- Los 61 parseos locales vigentes ya fueron re-hechos con parser `0.1.16` en
  rutas canonicas `backend/data/raw/results_csv/fchmn_auto/<anio>/...`; usar
  como evidencia el summary
  `backend/data/raw/batch_summaries/fchmn_historical_2022_2026_frozen_local_validation_20260423_parser016_canonical.json`.
- Copa Cordillera / Dual Internacional pertenece al circuito master FCHMN,
  incluyendo etapa Argentina; quedo incluida en el manifest local congelado.
- Sudamericanos deben tratarse como flujo separado y no mezclarse
  automaticamente con el manifest local principal; ver manifest y validacion
  separados en backend/docs/fchmn_results_validation.md.
- No usar keywords como compuerta final de carga.
- Antes de recargar core, usar como evidencia vigente de nombres de atletas
  fchmn_historical_2022_2026_athlete_name_audit_20260422_v8_rivas_verified.json,
  con 2 filas sospechosas restantes, ambas del caso fuente `Rojas, 2`.

Si la conversacion retoma una carga o recarga, seguir backend/docs/pre_load_checklist.md
sin saltar backup, wipe controlado, summary auditable y validacion post-load.
Si no hay pedido explicito de carga, mantenerse en diagnostico, parser, curaduria
o documentacion sin usar --load.
```

## Siguiente paso sugerido

Fase 4 quedo avanzada con contrato, scraper de apuntamiento, descarga separada,
parseo automatico previo a validacion, carga explicita protegida por compuertas,
resumen JSON auditable opcional, manifest local de multiples documentos,
auditoria local de brechas y freezer de manifests validados. El manifest
soporta carpetas parseadas y PDFs locales (`pdf` o `pdf_path`) y conserva
`source_url` para trazabilidad.

Proximo objetivo sugerido:

- Mantener descarga, manifest, parseo, validacion y carga separados.
- Usar el runbook `backend/docs/fchmn_results_validation.md` como resumen
  vigente de evidencia y artefactos, evitando volver a expandir cronologias
  intermedias en `ai_workflow.md`.
- Mantener `fchmn_historical_2022_2026_frozen_local_20260421.jsonl` como
  manifest local congelado de referencia y el flujo Sudamericano separado.
- Si se retoma una carga explicita, seguir `backend/docs/pre_load_checklist.md`:
  verificar estado real de staging/resultados/trazabilidad, preservar `pool` y
  calendario planificado, ejecutar `--load` con summary auditable y validar
  duplicados post-load.
- Si no se retoma carga, concentrar el trabajo en parser, curaduria y
  documentacion de Fase 4 sin usar `--load`.
- Diseñar automatizacion futura para detectar PDFs nuevos o cambios de checksum,
  validar y reportar sin cargar automaticamente.
- No crear tablas nuevas sin una migracion explicita.
