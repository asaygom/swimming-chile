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
- Excels, CSVs o previews con datos personales de clubes, incluyendo RUT,
  fecha de nacimiento, correos, telefonos, membresias o candidatos de vinculo
  persona-atleta.
- Manifests, summaries y reportes reales generados durante discovery, descarga,
  validacion o carga.
- Dumps de base de datos.
- Archivos `.env`, secretos, credenciales o configuraciones locales.
- Caches de Python, pytest, virtualenvs y artefactos temporales.

## Carpetas principales

- `backend/data/raw/`: fuentes externas y salidas completas generadas localmente. Esta carpeta no se versiona.
- `backend/data/raw/results_pdf/fchmn/<año>/`: PDFs FCHMN descargados, agrupados por año de competencia o fuente.
- `backend/data/raw/results_csv/fchmn/<año>/`: CSVs/debug generados por el parser, agrupados igual que el PDF de origen.
- `backend/data/staging/csv/`: zona de trabajo para CSVs generados o preparados para carga. Solo se versiona `.gitkeep`.
- `backend/data/staging/*.xlsx`: planillas operativas locales. Si contienen datos personales, deben permanecer ignoradas por Git.
- `backend/data/staging/nunoa_master_identity_preview/`: salidas locales del preview de identidad/membresia. Contienen PII y no se versionan.
- `backend/data/reference/`: datos curados pequenos que ayudan a normalizar o resolver entidades.
- `backend/tests/fixtures/`: entradas minimas y esperados pequenos para prevenir regresiones.
- `backend/docs/`: contratos, decisiones y documentacion del modelo vigente.


## Politica para datos personales de clubes

Los datos personales de clubes son informacion privada, no artefactos de
pipeline publico. Esto incluye RUT, fecha de nacimiento civil, correo, telefono,
membresia y cualquier preview que permita asociar una persona real a un club o
atleta.

Reglas:

- Versionar solo el codigo que procesa esos datos, nunca los archivos fuente ni
  las salidas con PII.
- Mantener las planillas fuente y previews bajo rutas ignoradas por Git.
- No copiar PII a documentacion, logs, summaries versionables ni fixtures.
- Tratar RUT como evidencia fuerte de identidad, no como requisito para que una
  persona exista como miembro activo.
- El vinculo entre `identity.person` y `core.athlete` debe quedar en bandeja de
  revision humana antes de persistirse.

Script vigente:

```powershell
.\backend\.venv\Scripts\python.exe backend\scripts\preview_nunoa_master_identity_import.py
```

El script lee la planilla local de Nunoa Master, normaliza datos para
`identity.person`, `identity.contact_point` y `club_ops.membership`, y genera
previews locales ignorados. No escribe en la base de datos.

## Politica para CSVs

Los CSVs operativos son efimeros. Sirven para conectar parser, revision manual y pipeline, pero deben poder regenerarse desde fuentes raw o desde fixtures controlados. Si un CSV se usa en tests, debe ser minimo y representar un caso especifico, no una competencia completa.

## Politica para manifests y summaries

Los manifests y summaries reales son artefactos locales auditables. Conviene
mantenerlos durante Fase 4 porque documentan que fue descubierto, descargado,
parseado, validado o cargado, pero no todos tienen el mismo estatus operativo.

Clasificar mentalmente cada artefacto antes de usarlo:

- Canonico: evidencia estable para auditoria o backfill historico, como
  discoveries historicos deduplicados, summaries de descarga con checksums,
  summaries de batch validation revisados, regresiones amplias del parser y
  manifests congelados curados.
- Exploratorio: corridas `scratch`, probes, smoke tests con `--limit`, rechecks
  puntuales y manifests/summaries producidos mientras se ajustaba el flujo.
- Carga: summaries de `load` previos son evidencia historica de carga, pero no
  autorizan nuevas cargas ni reemplazan un manifest congelado con
  `competition_scope` curado.

No borrar artefactos exploratorios durante una auditoria activa. Primero cerrar
una foto canonica con discovery, download summary, batch summary y brechas por
documento; despues se puede archivar o limpiar scratch/probes si ya no aportan
trazabilidad.

Los nombres deben dejar claro el rol del artefacto. Preferir prefijos como
`fchmn_historical_*`, `fchmn_results_validation_*`, `regression_*`,
`scratch_*` o `*_probe` antes que nombres ambiguos.

## Politica para fixtures

Los fixtures deben ser chicos, legibles y orientados a comportamiento. Si vienen de un PDF real, se recorta solo la linea, fila o salida minima necesaria para cubrir el caso. La intencion es prevenir regresiones sin convertir el repositorio en un almacen historico de datos.
