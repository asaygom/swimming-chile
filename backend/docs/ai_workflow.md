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

Retoma desde la auditoria historica 2022-2026:
- focus audit: backend/data/raw/batch_summaries/fchmn_historical_2022_2026_focus_audit_20260421.json
- blockers: backend/data/raw/batch_summaries/fchmn_historical_2022_2026_blockers_20260421.json
- scope review CSV: backend/data/raw/batch_summaries/fchmn_historical_2022_2026_available_for_scope_review_20260421.csv

Contexto vigente:
- El cierre operativo inicial se acoto a formatos 2022-2026.
- Los formatos pre-2022 quedan como backlog legacy.
- Copa Cordillera / Dual Internacional pertenece al circuito master FCHMN,
  incluyendo etapa Argentina; no excluir por sede argentina.
- Sudamericanos deben tratarse como flujo separado y no mezclarse
  automaticamente con el manifest local principal.
- No usar keywords como compuerta final de carga.

Siguiente tarea:
Resolver primero los 14 bloqueados 2022-2026: 8 requires_review y 6 failed.
Para cada familia, inspeccionar PDF/debug, agregar fixture chico si corresponde,
ajustar parser/canon de bajo riesgo y revalidar solo el lote afectado sin
--load. Despues auditar las competencias validadas disponibles en el scope
review CSV para clasificar scope fchmn_local u otro antes de congelar manifests.
```

## Siguiente paso sugerido

Fase 4 quedo avanzada con contrato, scraper de apuntamiento, descarga separada, parseo automatico previo a validacion, carga explicita protegida por compuertas, resumen JSON auditable opcional, manifest local de multiples documentos, auditoria local de brechas y freezer de manifests validados. El manifest soporta carpetas parseadas y PDFs locales (`pdf` o `pdf_path`) y conserva `source_url` para trazabilidad.

Proximo objetivo sugerido:

- Mantener descarga, manifest, parseo, validacion y carga separados.
- Usar la auditoria de brechas como punto de control canonico antes de nuevas acciones.
- Resolver primero los documentos 2022-2026 `failed` o `requires_review`; priorizar `fchmn_historical_2022_2026_blockers_20260421.json` antes que backlog pre-2022.
- En la proxima sesion, continuar con revision de parse para los 14 bloqueados 2022-2026: 8 `requires_review` y 6 `failed`. Para cada familia, inspeccionar PDF/debug, agregar fixture chico si corresponde, ajustar parser/canon de bajo riesgo y revalidar solo el lote afectado sin `--load`.
- Curar manualmente cuales `validated_local_candidate` pertenecen efectivamente a `fchmn_local`; no usar la categoria diagnostica como compuerta final.
- Curar `competition_scope` por documento y congelar un manifest solo con documentos locales validados usando lista explicita de `source_url`.
- Ejecutar checklist de wipe/full reload antes de una carga completa: backup, manifest congelado, checksums, orden de carga, carga explicita con `--load` y validacion post-load.
- Diseñar automatizacion futura para detectar PDFs nuevos o cambios de checksum, validar y reportar sin cargar automaticamente.
- No crear tablas nuevas sin una migracion explicita.
