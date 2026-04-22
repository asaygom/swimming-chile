# Contratos del parser PDF

Este documento fija los contratos minimos de entrada y salida del parser antes de modularizarlo. La meta es que los tests protejan el comportamiento actual y que cualquier cambio futuro pueda explicarse contra estos acuerdos.

## Entrada

- Fuente principal: PDF de resultados FCHMN/HY-TEK.
- El parser recibe un archivo PDF y parametros operativos como `--out-dir`, `--competition-id` y `--default-source-id`.
- Los layouts soportados incluyen encabezados de evento en ingles y espanol, cursos `LC/SC Meter` y `CL/CP/CC Metro`, resultados individuales y relevos.
- Desde parser `0.1.12`, tambien se soporta el layout brasileno "Swim It Up" detectado por watermark `Sistemas de Natacao Swim It Up`, con headers de evento en portugues, franjas etarias `FAIXA`, fechas tipo `13 a 17/04/2026`, individuales y relevos por columnas.
- Desde parser `0.1.13`, tambien se soportan PDFs HY-TEK con resultados en multiples columnas (`#1 Women...`) y planillas `Quadathlon`; estas ultimas se normalizan como cuatro pruebas canonicas 50m (`butterfly`, `backstroke`, `breaststroke`, `freestyle`) y no introducen un stroke nuevo.
- Desde parser `0.1.16`, los nombres de atletas y nadadores de relevo corrigen
  artefactos OCR acotados solo cuando hay evidencia de layout o respaldo de
  pruebas individuales. No se reescriben sufijos fuente como `Rojas, 2`.

## Salidas operativas

El parser debe generar CSVs con nombres estables:

- `club.csv`
- `event.csv`
- `athlete.csv`
- `result.csv`
- `relay_team.csv`
- `relay_swimmer.csv`

Tambien puede generar archivos de trazabilidad/debug:

- `raw_result.csv`
- `raw_relay_team.csv`
- `raw_relay_swimmer.csv`
- `debug_unparsed_lines.csv`
- `metadata.json`
- Excel consolidado para revision manual

## Canon esperado

- `event.gender`: `women`, `men`, `mixed`.
- `athlete.gender` y nadadores de relevo: `female`, `male`.
- `event.stroke`: `freestyle`, `backstroke`, `breaststroke`, `butterfly`, `individual_medley`, `medley_relay`, `freestyle_relay`.
- `status`: `valid`, `dns`, `dnf`, `dsq`, `scratch`, `unknown`.

## Reglas de trazabilidad

- `metadata.json` debe incluir `pdf_name`, `pdf_sha256` y `parser_version` cuando el origen sea un PDF.
- `seed_time_text` y `result_time_text` conservan la forma normalizada del tiempo o status.
- `seed_time_ms` y `result_time_ms` se derivan cuando el tiempo es comparable.
- `age_at_event` pertenece al resultado observado.
- `birth_year_estimated = competition_year - age_at_event` cuando existe anio de competencia.
- `relay_team.csv` puede incluir `club_name`. Cuando existe, representa el club observado del equipo de relevo y debe preservarse hacia la carga; cuando falta o viene vacio, el pipeline puede inferir el club desde `club.csv` y `relay_team_name`.
- Las heuristicas propias del PDF viven en el parser; el pipeline solo debe hacer limpieza generica y carga.
- El parser normaliza sufijos de categorias de edad pegados al estilo en encabezados HY-TEK, por ejemplo `Breast 40 a 99 años` o `Medley 120 a 159 años Relay`, sin cambiar el canon de `event.stroke`.
- El parser puede omitir parciales/splits de carrera en `debug_unparsed_lines.csv` cuando no son filas de resultado; esto evita bloquear la validacion por lineas auxiliares de HY-TEK.
- Si una fila con resultado tipo status deja el tiempo de seed pegado al club, por ejemplo `Club Sparta A C 49.33 DQ DQ`, el parser debe separar `club_name = Club Sparta A C`, `seed_time_text = 49,33` y `result_time_text = DQ`.

## Fixtures de prueba

Los fixtures versionados deben ser pequenos y representativos. No se versionan PDFs completos ni CSVs historicos completos; solo lineas o archivos minimos necesarios para prevenir regresiones.
