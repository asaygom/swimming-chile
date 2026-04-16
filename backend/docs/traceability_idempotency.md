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
