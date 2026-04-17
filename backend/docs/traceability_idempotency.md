# Trazabilidad e idempotencia

Este documento define el contrato operativo de Fase 2. La meta es que cada ingesta deje evidencia auditable y que reprocesar el mismo insumo no duplique datos core.

## Tablas operativas

- `source_document`: representa el documento o lote fuente. Para PDFs parseados guarda nombre, checksum SHA-256, version del parser, ruta/URL y metadata.
- `load_run`: representa una ejecucion del pipeline. Guarda documento fuente, competencia, estado, version del parser, ruta de entrada y conteos de filas leidas.
- `validation_issue`: guarda issues detectados por las validaciones del pipeline cuando su conteo es mayor que cero.

La definicion canonical esta en `backend/sql/schema.sql`. Para una base existente, aplicar `backend/sql/migrations/001_traceability_idempotency.sql`.

## Identidad del documento fuente

El identificador preferido es `pdf_sha256`, porque prueba que el contenido del PDF es exactamente el mismo. Si no existe checksum, el pipeline usa `source_url` cuando esta disponible. Si tampoco existe, crea un `source_document` nuevo para la corrida manual.

## Estados de carga

- `started`: el pipeline creo la corrida y va a cargar staging/core.
- `completed`: la carga termino y las validaciones fueron registradas.
- `failed`: ocurrio un error; se guarda el mensaje para auditoria.

## Reglas de recarga

- Reprocesar el mismo PDF debe reutilizar `source_document` por checksum.
- Las tablas core evitan duplicados con checks previos y constraints unicos minimos.
- Si una fila ya existe, el pipeline debe ignorarla, no crear otra observacion equivalente.
- Las validaciones no bloquean la carga todavia; quedan persistidas para revision. Las compuertas duras pertenecen a la fase de batch runner.

## Validacion operativa

Validacion realizada contra PostgreSQL local (`natacion_chile`, schema `core`) el 2026-04-17 usando:

- PDF: `backend/data/raw/results_pdf/FCHMN_2026/resultados-coppa-italia-master-2026.pdf`
- CSVs regenerados: `backend/data/raw/results_csv/2026/coppa_italia_2026`
- `competition_id`: `2`
- `parser_version`: `0.1.5`
- `pdf_sha256`: `07e70c3202dd89d631bb312a76022f3adbd84c1e2787939c027c0b5d93d08256`

Resultado:

- Dos corridas consecutivas quedaron en `load_run.status = completed`.
- Ambas corridas reutilizaron el mismo `source_document_id` por `pdf_sha256`.
- Los conteos core se mantuvieron estables al recargar:
  - `event`: 232
  - `result`: 1238
  - `relay_result`: 80
  - `relay_result_member`: 320

Nota: carpetas CSV generadas antes de que el parser escribiera `pdf_sha256` y `parser_version` en `metadata.json` no tienen identidad documental estable. En ese caso el pipeline crea un `source_document` nuevo para la corrida manual, salvo que exista `source_url`.

## Constraints minimos

- `event`: unico por `competition_id` y `event_name` normalizado.
- `result`: unico por evento, atleta, club observado, posicion, tiempo y status.
- `relay_result`: unico por evento, club observado, nombre de equipo, posicion, tiempo y status.
- `relay_result_member`: unico por relevo y orden de posta.

## Fuera de alcance por ahora

- No se implementa scraper.
- No se bloquea la carga por alertas de calidad.
- No se resuelve identidad probabilistica de atletas.
- No se integra memoria persistente tipo Engram ni orquestacion multiagente.
