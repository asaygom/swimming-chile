# Changelog Histórico (Natación Chile)

Este documento condensa los hitos y auditorías relevantes durante el desarrollo y carga de datos históricos (Fase 4 y Fase 5). La evidencia detallada original fue consolidada para mantener la documentación operativa limpia.

## Abril - Mayo 2026 (Consolidación de Pipeline FCHMN 2022-2026)

### Parser Updates
- **0.1.12**: Soporte para layout brasileño "Swim It Up" (ej. Sudamericano Recife).
- **0.1.13**: Soporte para Quadathlon (conversión a 4 pruebas de 50m), sufijos de récords, encabezados OCR "E vento" y layouts HY-TEK multi-columna.
- **0.1.15**: Corrección de bugs en layouts HY-TEK de dos columnas y fragmentación OCR. Reparación conservadora de textos como `Rojas, 2 20 Escuela...` sin aplicar aliases.
- **0.1.16**: Correcciones menores en nombres de atletas y relevos, validado sobre 97k filas de atletas con sobreescribimientos específicos.
- **0.1.17**: Corrección de tiempos imposibles en HY-TEK (menores a 10s) que leían los puntos como tiempo o no traían seed real. Bloqueos estrictos para `result.csv` y `relay_team.csv`.

### Curaduría de Atletas y Alias de Clubes
- Se automatizó la detección pre-load de errores OCR conocidos en nombres de atletas. 
- Canonización de orden de nombres (`Nombre Apellido` -> `Apellido, Nombre`).
- Implementación de alias transitivos en `run_pipeline_results.py` (ej. `A -> B -> C` resuelve directo a `C`).
- La curaduría de identidades ahora requiere comprobaciones rígidas: mismo `birth_year`, mismo club y mismo género antes de proponer alias automático. Otras variaciones cruzan hacia revisión manual (ej. nombres extendidos).

### Auditoría Histórica
- **Foco 2022-2026:** El proyecto operativizó exitosamente 61 documentos del circuito local de la FCHMN para el periodo 2022-2026. Los años anteriores quedaron catalogados como backlog (legacy).
- **Competencias Internacionales:** Se separaron explícitamente eventos como el Sudamericano en un flujo de manifest diferente, asegurando que no se mezclen sin validación estricta del `competition_scope`.
- Copa Cordillera / Dual Internacional fue catalogada como circuito local (FCHMN).

*(Fin de los registros históricos exportados)*
