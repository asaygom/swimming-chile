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

Implementado en el repo:

- Plan del proyecto versionado.
- Politica de datos y artefactos.
- Contratos del parser.
- Tests unitarios y fixtures chicos.
- Trazabilidad e idempotencia inicial en schema, migracion y pipeline.
- Trazabilidad e idempotencia validadas contra PostgreSQL local con recarga real de `coppa_italia_2026`.
- Modularizacion controlada inicial: normalizacion compartida de tiempos, generos, estilos y status en `backend/natacion_chile/domain/normalization.py`.
- Validaciones persistidas como `validation_issue`.
- Contrato minimo de Fase 4 para batch runner y compuertas en `backend/docs/batch_runner_contract.md`.
- `backend/scripts/run_results_batch.py` puede validar `--input-dir`, ejecutar `--pdf` + `--out-dir` antes de validar y cargar con `--load` solo si el batch queda `validated`.
- `backend/scripts/scrape_fchmn.py` puede descubrir enlaces PDF desde HTML local o URL y escribir un manifest JSONL sin descargar, parsear ni cargar.
- `backend/scripts/download_manifest_pdfs.py` puede descargar PDFs declarados en un manifest desde `source_url` hacia `pdf`/`pdf_path`, con `--overwrite` explicito y resumen con checksum.
- Con `--overwrite`, `download_manifest_pdfs.py` reporta si un PDF fue `updated` por cambio de checksum o `unchanged` si el contenido remoto coincide con el local.
- Downloader, batch runner y orquestador FCHMN comparten la lectura JSONL de manifests desde `backend/natacion_chile/manifest.py`.
- Los summaries de manifests de descarga y batch incluyen `state_counts` para auditoria agregada.
- El batch runner marca como `failed` un manifest sin documentos; validar cero unidades no cuenta como batch sano.
- El contrato manual manifest -> descarga -> batch runner esta cubierto con fixture controlado, sin red real ni carga a core.
- Smoke real controlado con `resultados-ii-copa-chile-1.pdf`: descarga, parseo y validacion quedaron `validated` sin cargar a core.
- Segundo smoke real controlado con `resultados-coppa-italia-master-2026.pdf`: descarga, parseo y validacion quedaron `validated` sin cargar a core.
- Los manifests del scraper agrupan PDFs y CSVs por año bajo `results_pdf/fchmn/<año>/` y `results_csv/fchmn/<año>/`.
- Smoke real de descubrimiento contra portada FCHMN: el scraper filtra convocatorias y emite un manifest con PDF de resultados.
- Las rutas relativas en manifests se resuelven desde la raiz del proyecto para evitar fallos al ejecutar tests o comandos desde subcarpetas.
- El batch runner conserva `source_url` desde manifests y la entrega al pipeline durante `--load` para trazabilidad de `competition` y `source_document`.
- Fallos tecnicos del parser en un documento de manifest quedan aislados como `failed` y no detienen el resto del manifest.
- `backend/scripts/run_fchmn_results_validation.py` automatiza discovery -> download -> batch validation sin carga a core.
- La automatizacion FCHMN reporta `discovered_documents` y falla temprano si discovery no encuentra PDFs, para evitar smokes falsamente sanos.
- E2E real controlado desde `https://fchmn.cl/resultados/`: discovery -> download -> batch validation quedo `validated` para `resultados-coppa-italia-master-2026.pdf` y `resultados-ii-copa-chile.pdf`, sin cargar a core.
- Runbook de validacion automatizada FCHMN en `backend/docs/fchmn_results_validation.md` con comandos reproducibles para discovery, download y batch validation.
- El fixture del scraper conserva candidatos de portada como `resultados-1a-etapa.pdf`; no se excluyen por keyword aunque el parser aun pueda marcarlos como `failed`.

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

Primer objetivo sugerido:

- Ampliar pruebas controladas de manifest contra pocas URLs FCHMN antes de automatizar volumen.
- Mantener descarga, manifest, parseo, validacion y carga separados.
- Conservar como candidatos los PDFs de portada `resultados-1a-etapa.pdf` y similares: son de un Sudamericano Master en Brasil con formato pendiente de soporte, no ruido para excluir por keyword.
- No crear tablas nuevas sin una migracion explicita.
