# Metodologia de trabajo con IA

Este documento permite retomar el proyecto en otra conversacion sin depender del historial del chat. Resume la forma de trabajo acordada para Natacion Chile.

## Fuentes de verdad

- Hoja de ruta: `implementation_plan.md`.
- Reglas del proyecto: `AGENTS.md` cuando este disponible en el entorno local.
- Politica de artefactos: `backend/docs/data_artifacts.md`.
- Contratos del parser: `backend/docs/parser_contracts.md`.
- Modelo vigente: `backend/docs/schema.md`.
- Trazabilidad e idempotencia: `backend/docs/traceability_idempotency.md`.

## Principio central

El aprendizaje con IA debe avanzar junto con el plan real del proyecto. Cada sesion debe empujar una fase del producto y explicar el donde, por que y para que de los cambios.

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

No implementado todavia:

- Memoria persistente tipo Engram.  https://github.com/Gentleman-Programming/engram 
- Skills de `gentle-ai`. https://github.com/Gentleman-Programming/gentle-ai 
- MCP externo para memoria.
- Orquestacion multiagente.
- Scraper o batch runner con compuertas duras.

La metodologia del curso se esta aplicando manualmente: Plan Mode, SDD, contratos, tests, docs vivas y human-in-the-loop. Las herramientas Gentleman/Engram quedan como fase posterior, no como dependencia actual.

## Prompt recomendado para retomar

Usar este texto al iniciar una nueva conversacion:

```text
Estamos trabajando en Natacion Chile. Lee primero implementation_plan.md y backend/docs/ai_workflow.md.
Luego revisa git status y los docs relevantes antes de proponer cambios.
Continua segun la metodologia acordada: diagnostico, propuesta corta, patch minimo, tests, git status y propuesta de commit.
No saltes fases del implementation_plan.md.
Si hay cambios locales del usuario, respetalos y trabaja alrededor de ellos.
Explica el donde, por que y para que de cada cambio.
```

## Siguiente paso sugerido

Fase 3 quedo completada para las normalizaciones compartidas de bajo riesgo. El siguiente paso es iniciar Fase 4: scraper y batch runner con compuertas de calidad.

Primer objetivo sugerido:

- Definir el contrato minimo del batch runner antes de implementar.
- Mantener descarga/parseo/validacion separados de la carga a core.
- Registrar estados tipo descargado, parseado, validado y requiere revision.
- No bloquear cargas actuales hasta que existan compuertas verificadas con fixtures.
