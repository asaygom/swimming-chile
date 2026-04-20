# Metodologia de trabajo con IA

Este documento permite retomar el proyecto en otra conversacion sin depender del historial del chat. Resume la forma de trabajo acordada para Natacion Chile.

## Fuentes de verdad

- Hoja de ruta: `implementation_plan.md`.
- Reglas operativas para agentes: `AGENTS.md`.
- Politica de artefactos: `backend/docs/data_artifacts.md`.
- Contratos del parser: `backend/docs/parser_contracts.md`.
- Contrato del batch runner: `backend/docs/batch_runner_contract.md`.
- Validacion automatizada FCHMN: `backend/docs/fchmn_results_validation.md`.
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
- `scrape_fchmn.py` y `run_fchmn_results_validation.py` soportan `--crawl-pages` y multiples `--url` para consolidar fuentes FCHMN en un manifest deduplicado.
- `run_results_batch.py --load` solo ejecuta el pipeline si el documento esta `validated` y su `competition_scope` coincide con el scope requerido (`fchmn_local` por defecto).

Decisiones vigentes:

- El parser parsea PDFs publicados; no decide si un documento se carga a core.
- La compuerta de carga debe distinguir circuito, federacion, ambito, sede o etapa con datos curados, no con keywords automaticas.
- Coppa Italia pertenece al circuito FCHMN local; Copa Cordillera requiere modelar sede/etapa; Sudamericanos pueden validarse, pero no cargarse a core sin scope adecuado.
- Un documento `failed` o `requires_review` no debe contaminar otros documentos del manifest ni cargarse a core.
- Los PDFs con formato no soportado son candidatos a soporte futuro, no ruido descartable.

Evidencia historica:

- La evidencia auditable de discovery, descarga, validacion, regresiones y cargas previas vive en `backend/data/raw/batch_summaries/` y `backend/data/raw/manifests/`.
- El runbook `backend/docs/fchmn_results_validation.md` conserva los comandos reproducibles y los resultados exploratorios relevantes.
- La regresion amplia con parser `0.1.12` dejo `backend/data/raw/batch_summaries/regression_parser_012.json` como evidencia principal de compatibilidad HY-TEK + Swim It Up.

No implementado todavia:

- Memoria persistente tipo Engram.  https://github.com/Gentleman-Programming/engram 
- Skills de `gentle-ai`. https://github.com/Gentleman-Programming/gentle-ai 
- MCP externo para memoria.
- Orquestacion multiagente.
- Persistencia explicita de estado de batch en tabla operativa propia.

La metodologia del curso se esta aplicando manualmente: Plan Mode, SDD, contratos, tests, docs vivas y human-in-the-loop. Las herramientas Gentleman/Engram quedan como fase posterior, no como dependencia actual.

## Prompt recomendado para retomar

Usar este texto al iniciar una nueva conversacion:

```text
Lee primero implementation_plan.md y backend/docs/ai_workflow.md.
Luego revisa git status y los docs relevantes antes de proponer cambios.
Continua segun la metodologia acordada: diagnostico, propuesta corta, patch minimo, tests, git status y propuesta de commit.
No saltes fases del implementation_plan.md.
Si hay cambios locales del usuario, respetalos y trabaja alrededor de ellos.
Explica el qué (what), el por qué (why), el donde (where) y lo aprendido (learned) de cada cambio.
```

## Siguiente paso sugerido

Fase 4 quedo iniciada con contrato, scraper de apuntamiento, descarga separada, parseo automatico previo a validacion, carga explicita protegida por compuertas, resumen JSON auditable opcional y manifest local de multiples documentos. El manifest soporta carpetas parseadas y PDFs locales (`pdf` o `pdf_path`) y conserva `source_url` para trazabilidad.

Proximo objetivo sugerido:

- Mantener descarga, manifest, parseo, validacion y carga separados.
- Consolidar un manifest historico deduplicado desde fuentes FCHMN especificas, incluyendo paginas de menu, sudamericanos y nacionales.
- Curar `competition_scope` por documento y congelar un manifest solo con documentos locales validados.
- Excluir documentos `requires_review` y `failed` del manifest congelado para carga.
- Preparar checklist de wipe/full reload antes de una carga completa: backup, manifest congelado, checksums, orden de carga y validacion post-load.
- Diseñar automatizacion futura para detectar PDFs nuevos o cambios de checksum, validar y reportar sin cargar automaticamente.
- No crear tablas nuevas sin una migracion explicita.
