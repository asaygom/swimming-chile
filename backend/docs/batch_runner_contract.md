# Contrato del batch runner y compuertas

Este documento inicia la Fase 4. Define el contrato minimo antes de implementar
scraper, descarga automatica o compuertas duras.

La meta es automatizar con control: descubrir o recibir PDFs, parsearlos,
validarlos y solo cargar a core cuando el lote cumple condiciones minimas de
calidad.

## Alcance inicial

El batch runner debe orquestar componentes existentes:

- `backend/scripts/parse_results_pdf.py` para transformar PDF a CSVs.
- `backend/scripts/run_pipeline_results.py` para cargar CSVs ya validados.
- `source_document`, `load_run` y `validation_issue` para trazabilidad en DB.

El scraper de FCHMN es una pieza separada. Puede alimentar el batch runner con
URLs y rutas locales, pero no debe mezclarse con la logica de parseo, validacion
o carga.

## Entradas

Una unidad de trabajo representa un PDF de resultados o una carpeta ya parseada.

Campos minimos esperados:

- `source_id`: origen del documento.
- `source_url`: URL original cuando exista.
- `pdf_path`: ruta local del PDF cuando se vaya a parsear.
- `input_dir`: carpeta de CSVs cuando el parseo ya exista.
- `competition_id` o metadata suficiente para resolver/crear competencia.
- `default_source_id`: source por defecto para filas generadas.

Reglas:

- Si existe PDF, la identidad preferida es `pdf_sha256`.
- Si no existe PDF, se usa `source_url` cuando este disponible.
- Si no hay checksum ni URL, el lote se considera manual y no idempotente a nivel documental.

### Manifest JSONL local

Antes del scraper, el formato estable para lotes locales es JSONL: una unidad de
trabajo por linea, sin envolver el archivo en un arreglo JSON. Las lineas vacias
y las lineas que empiezan con `#` se ignoran.

Cada entrada debe usar exactamente una de estas formas:

```json
{"input_dir": "backend/data/raw/results_csv/competencia_x", "competition_id": 1, "default_source_id": 1}
```

```json
{"pdf": "backend/data/raw/results_pdf/competencia_x.pdf", "out_dir": "backend/data/raw/results_csv/competencia_x", "competition_id": 1, "default_source_id": 1}
```

Campos por entrada:

- `input_dir`: carpeta ya parseada con CSVs operativos.
- `pdf`: PDF local a parsear antes de validar. `pdf_path` se acepta como alias
  compatible con el nombre conceptual del contrato.
- `out_dir`: carpeta donde el parser escribira CSVs; requerido con `pdf` o
  `pdf_path`.
- `competition_id`: opcional si viene por CLI; el valor de la entrada tiene
  prioridad sobre el valor global.
- `default_source_id`: opcional; hereda el valor global cuando no se declara.
- `excel_name`: opcional; hereda el valor global cuando no se declara.

Cada documento se procesa de forma aislada. Un documento en `requires_review`
debe quedar reportado en el resumen del manifest, pero no debe impedir que los
otros documentos del mismo manifest se validen con su propio estado.

## Salidas esperadas

Por cada unidad procesada:

- PDF almacenado localmente, si aplica.
- CSVs operativos del parser en una carpeta estable.
- `metadata.json` con `pdf_name`, `pdf_sha256` y `parser_version` cuando exista PDF.
- Estado final del batch.
- Evidencia de issues de validacion.
- Carga a core solo si las compuertas lo permiten.

Los PDFs, CSVs completos y Excels generados siguen sin versionarse.

## Estados del batch

Estados canonicos propuestos:

- `discovered`: el documento fue encontrado por scraper o manifest, pero aun no se descargo.
- `downloaded`: el PDF esta disponible localmente y tiene checksum calculable.
- `parsed`: el parser genero salidas operativas.
- `validated`: las salidas pasaron compuertas minimas y pueden cargarse.
- `requires_review`: existen alertas bloqueantes o ambiguas; no se carga a core.
- `loaded`: la carga a staging/core termino correctamente.
- `failed`: hubo error tecnico que impidio completar la etapa.

Relacion con `load_run`:

- `load_run` sigue describiendo la ejecucion del pipeline de carga.
- El estado del batch describe la orquestacion previa y posterior a la carga.
- No se debe ampliar `load_run.status` sin una migracion explicita.

## Compuertas minimas antes de cargar

Las compuertas duras ocurren antes de ejecutar `run_pipeline_results.py`.

Bloquean la carga y dejan el lote en `requires_review`:

- Falta un CSV operativo obligatorio: `club.csv`, `event.csv`, `athlete.csv` o `result.csv`.
- Falta `metadata.json` para un PDF parseado.
- Falta `pdf_sha256` en metadata cuando la entrada fue un PDF.
- El parser no encontro eventos.
- El parser no encontro resultados individuales ni relevos.
- `debug_unparsed_lines.csv` supera el umbral permitido.
- Hay valores fuera del canon documentado para genero, estilo o status.
- Hay filas de resultado sin `event_name` o sin identidad observable de atleta/equipo.

Umbral inicial:

- `debug_unparsed_lines / lineas_relevantes_parseadas > 0.20` requiere revision.

Este umbral es conservador y debe validarse con fixtures antes de automatizar
cargas masivas.

## Validaciones no bloqueantes iniciales

Estas alertas se registran, pero no bloquean por defecto:

- Puntos ausentes.
- `seed_time` ausente.
- Club inferido para relevos.
- `birth_year_estimated` ausente cuando no hay edad.
- Diferencias menores de nombres de clubes cubiertas por aliases manuales.

## Separacion de responsabilidades

Scraper:

- Descubre URLs.
- Descarga PDFs.
- Calcula checksum.
- No parsea ni carga a DB.

Batch runner:

- Decide si un documento se debe procesar o saltar.
- Ejecuta parser.
- Evalua compuertas.
- Ejecuta pipeline solo si el lote esta validado.
- Produce resumen auditable.

Parser:

- Contiene heuristicas PDF.
- Genera CSVs y debug.
- No decide si se carga a core.

Pipeline:

- Hace limpieza generica y carga.
- Registra `source_document`, `load_run` y `validation_issue`.
- No debe implementar heuristicas agresivas del PDF.

## Contrato de idempotencia

- Si el mismo `pdf_sha256` ya fue procesado y cargado, el batch runner puede saltar la carga.
- Si el mismo checksum aparece con nueva URL, se actualiza trazabilidad del documento, no se duplica core.
- Si cambia el checksum para una URL conocida, se procesa como nueva version del documento.
- Si el parser cambia de version, se permite reprocesar, pero la carga a core debe seguir siendo idempotente.

## Fuera de alcance de este primer paso

- Implementar scraper real contra FCHMN.
- Crear tablas nuevas de batch state.
- Cambiar estados de `load_run`.
- Bloquear el pipeline manual existente.
- Resolver identidad probabilistica de atletas.
- Curar aliases automaticamente.

## Siguiente implementacion sugerida

Extender `backend/scripts/run_results_batch.py` para que:

1. Soporte entradas `pdf` en manifest con fixtures controlados.
2. Escriba resumen agregado y por documento en formato estable.
3. Mantenga scraper FCHMN separado del parseo, validacion y carga.
4. Use estados persistentes cuando exista una tabla operativa de batch.
