# Artefactos de datos y fuente de verdad

Este documento fija la politica operativa de archivos del proyecto. La regla general es: versionamos codigo, contratos y fixtures pequenos; no versionamos datos raw ni salidas completas generadas.

## Fuente final de verdad

PostgreSQL es la fuente final de verdad para datos consultables. Los CSVs, Excels y archivos debug son artefactos de trabajo para ingesta, validacion y auditoria, pero no reemplazan las tablas core.

## Que se versiona

- Codigo fuente de scripts y modulos.
- SQL de esquema, indices, constraints y consultas versionadas.
- Documentacion de arquitectura, contratos y decisiones.
- Fixtures pequenos de test en `backend/tests/fixtures/`.
- Archivos `.gitkeep` cuando una carpeta vacia define una estructura del proyecto.
- Archivos de referencia curados y pequenos, como aliases manuales.

## Que no se versiona

- PDFs descargados desde FCHMN u otras fuentes.
- CSVs completos generados desde PDFs reales.
- Excels consolidados generados por el parser.
- Dumps de base de datos.
- Archivos `.env`, secretos, credenciales o configuraciones locales.
- Caches de Python, pytest, virtualenvs y artefactos temporales.

## Carpetas principales

- `backend/data/raw/`: fuentes externas y salidas completas generadas localmente. Esta carpeta no se versiona.
- `backend/data/staging/csv/`: zona de trabajo para CSVs generados o preparados para carga. Solo se versiona `.gitkeep`.
- `backend/data/reference/`: datos curados pequenos que ayudan a normalizar o resolver entidades.
- `backend/tests/fixtures/`: entradas minimas y esperados pequenos para prevenir regresiones.
- `backend/docs/`: contratos, decisiones y documentacion del modelo vigente.

## Politica para CSVs

Los CSVs operativos son efimeros. Sirven para conectar parser, revision manual y pipeline, pero deben poder regenerarse desde fuentes raw o desde fixtures controlados. Si un CSV se usa en tests, debe ser minimo y representar un caso especifico, no una competencia completa.

## Politica para fixtures

Los fixtures deben ser chicos, legibles y orientados a comportamiento. Si vienen de un PDF real, se recorta solo la linea, fila o salida minima necesaria para cubrir el caso. La intencion es prevenir regresiones sin convertir el repositorio en un almacen historico de datos.
