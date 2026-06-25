# Metodología de trabajo con IA

Este documento permite retomar el proyecto en otra conversación sin depender del historial del chat. Resume la forma de trabajo acordada para Natación Chile.

## Fuentes de verdad

- **Arquitectura y Uso**: `backend/README.md`
- **Hoja de ruta**: `docs/plans/implementation_plan.md`
- **Reglas operativas para agentes**: `AGENTS.md`
- **Política de artefactos**: `backend/docs/data_artifacts.md`
- **Contratos del parser**: `backend/docs/parser_contracts.md`
- **Contrato del batch runner**: `backend/docs/batch_runner_contract.md`
- **Validación automatizada**: `backend/docs/fchmn_results_validation.md`
- **Checklist pre-carga/full reload**: `backend/docs/pre_load_checklist.md`
- **Modelo de Datos vigente**: `backend/docs/schema.md`
- **Trazabilidad e idempotencia**: `backend/docs/traceability_idempotency.md`
- **Historial de Decisiones (Log)**: `backend/docs/CHANGELOG.md`

## Principio central

El aprendizaje con IA debe avanzar junto con el plan real del proyecto. Cada sesión debe empujar una fase del producto y explicar el dónde, por qué y para qué de los cambios.

`AGENTS.md` y este documento se complementan:
- `AGENTS.md`: reglas cortas e imperativas para actuar dentro del repo.
- `ai_workflow.md`: memoria metodológica y continuidad entre conversaciones.
- Los contratos técnicos viven en docs específicas. Evitar duplicar detalles largos.

**Orden actual del proyecto:**
1. Blindar.
2. Trazabilidad e idempotencia.
3. Modularizar.
4. Automatizar con compuertas (Fase Actual).
5. Curar identidad de atletas.
6. Exponer producto de datos.

## Flujo obligatorio por cambio

1. **Diagnóstico**:
   - Revisar `git status`.
   - Leer archivos relevantes antes de proponer cambios.
   - Identificar cambios locales del usuario y no sobrescribirlos.
2. **Propuesta corta**:
   - Explicar el patch antes de editar.
   - Mantener cambios mínimos y localizados.
3. **Implementación**:
   - Seguir patrones existentes.
   - Si cambia regex o parseo, agregar comentario breve.
4. **Verificación**:
   - Ejecutar tests relevantes.
   - Ejecutar `py_compile` si se modifican scripts Python.
5. **Cierre**:
   - Revisar `git status`.
   - Resumir diagnóstico, cambios y verificación.
   - Proponer mensaje de commit.

## Regla de sincronización documental

Si cambia el comportamiento o el contrato:
1. Actualizar tests.
2. Actualizar el contrato técnico correspondiente.
3. Actualizar `README.md` si afecta uso humano.
4. Actualizar `AGENTS.md` solo si cambia una regla operativa.
5. Registrar cambios de gran envergadura o decisiones arquitectónicas en `CHANGELOG.md`.

## Continuidad operativa

Este documento no debe guardar estado puntual de cargas, manifests o próximos pasos,
porque esa información cambia rápido y tiende a quedar desactualizada. Para retomar
trabajo operativo:

- Usar `AGENTS.md` como reglas imperativas de ejecución.
- Usar `docs/plans/implementation_plan.md` como hoja de ruta por fases.
- Usar `backend/docs/fchmn_results_validation.md` como evidencia vigente de
  manifests, parser y validaciones.
- Usar `backend/docs/pre_load_checklist.md` antes de cualquier carga o recarga.
- Usar `backend/docs/CHANGELOG.md` para hitos históricos y decisiones relevantes.

Si una conversación necesita contexto inicial, debe pedir explícitamente leer esas
fuentes y revisar `git status` antes de proponer cambios.
