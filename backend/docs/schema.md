# Schema v0.1 - Plataforma de datos de Natacion Chile

## 1. Proposito

Este documento describe el esquema logico actualmente operativo para cargar resultados de competencias de natacion en Chile, con foco inicial en resultados master publicados por FCHMN.

El flujo vigente extrae datos desde PDFs tipo HY-TEK, genera CSVs normalizados y carga una base PostgreSQL separando staging y core.

## 2. Estado operativo actual

Actualmente el proyecto soporta:

- parser PDF para layouts FCHMN tipo HY-TEK
- cursos `LC Meter` y `SC Meter`
- resultados individuales
- resultados de relevos e integrantes por posta
- tiempos de inscripcion (`seed_time_text`, `seed_time_ms`)
- tiempos finales (`result_time_text`, `result_time_ms`)
- edad contextual del resultado (`age_at_event`)
- estimacion de año de nacimiento (`birth_year_estimated`)
- puntos cuando existen en el PDF
- carga end-to-end a PostgreSQL para individuales y relevos

El pipeline carga:

- `club`
- `event`
- `athlete`
- `result`
- `relay_result`
- `relay_result_member`

## 3. Principios de modelado

### 3.1 Staging antes de core

Los CSVs se cargan primero en tablas `stg_*`. La limpieza generica, casts y cruces se hacen antes de insertar en tablas core.

### 3.2 Separar dato bruto y dato consultable

Los tiempos se conservan como texto para trazabilidad y tambien como milisegundos para ordenar, comparar y analizar.

### 3.3 No usar club como identidad rigida del atleta

El club ayuda a resolver el resultado observado, pero no debe ser la identidad permanente del nadador. Un atleta puede cambiar de club entre competencias.

### 3.4 La edad pertenece al resultado

`age_at_event` es contextual al evento. No reemplaza la identidad del atleta.

Cuando el parser tiene año de competencia y edad:

```text
birth_year_estimated = competition_year - age_at_event
```

### 3.5 Parser especializado, pipeline generico

El parser puede corregir problemas tipicos de extraccion PDF. El pipeline debe limitarse a limpieza generica, normalizacion de catalogos, casts y carga.

## 4. Tablas core

Las tablas core definidas en `backend/sql/schema.sql` son:

- `source`
- `club`
- `pool`
- `competition`
- `event`
- `athlete`
- `result`
- `relay_result`
- `relay_result_member`
- `record`

## 5. Tablas staging

Las tablas staging vigentes son:

- `stg_club`
- `stg_event`
- `stg_athlete`
- `stg_result`
- `stg_relay_result`
- `stg_relay_result_member`

## 6. Catalogos canonicos

### 6.1 `event.gender`

Valores esperados:

- `women`
- `men`
- `mixed`

### 6.2 `athlete.gender` y `relay_result_member.gender`

Valores esperados:

- `female`
- `male`

### 6.3 `event.stroke` y `record.stroke`

Valores esperados:

- `freestyle`
- `backstroke`
- `breaststroke`
- `butterfly`
- `individual_medley`
- `medley_relay`
- `freestyle_relay`

### 6.4 `result.status` y `relay_result.status`

Valores esperados:

- `valid`
- `dns`
- `dnf`
- `dsq`
- `scratch`
- `unknown`

### 6.5 `competition.course_type` y `record.course_type`

Valores esperados:

- `scm`
- `lcm`
- `unknown`

## 7. Resumen de entidades core

### 7.1 `source`

Registra origenes de informacion.

Campos principales:

- `id`
- `name`
- `source_type`
- `base_url`
- `notes`
- `last_checked_at`
- `created_at`

### 7.2 `club`

Registra clubes, equipos o instituciones.

Campos principales:

- `id`
- `name`
- `short_name`
- `city`
- `region`
- `association_name`
- `website`
- `instagram`
- `is_active`
- `source_id`
- `created_at`
- `updated_at`

### 7.3 `pool`

Registra piscinas o recintos.

Campos principales:

- `id`
- `name`
- `city`
- `region`
- `address`
- `latitude`
- `longitude`
- `pool_length_m`
- `lanes_count`
- `indoor_outdoor`
- `heated`
- `public_access_type`
- `website`
- `contact_info`
- `notes`
- `source_id`
- `last_verified_at`
- `created_at`
- `updated_at`

### 7.4 `competition`

Registra competencias.

Campos principales:

- `id`
- `name`
- `season_year`
- `start_date`
- `end_date`
- `city`
- `region`
- `venue_name`
- `pool_id`
- `organizer`
- `competition_type`
- `course_type`
- `status`
- `source_id`
- `source_url`
- `created_at`
- `updated_at`

### 7.5 `event`

Registra pruebas individuales y relevos dentro de una competencia.

Campos principales:

- `id`
- `competition_id`
- `event_name`
- `stroke`
- `distance_m`
- `gender`
- `age_group`
- `round_type`
- `event_order`
- `scheduled_date`
- `source_id`
- `created_at`

### 7.6 `athlete`

Registra nadadores.

Campos principales:

- `id`
- `full_name`
- `gender`
- `birth_year`
- `nationality`
- `club_id`
- `source_id`
- `created_at`
- `updated_at`

Observaciones:

- `club_id` refleja el club conocido para una carga o cruce, no una identidad historica permanente.
- `birth_year` puede provenir desde `stg_athlete.birth_year` o desde estimaciones de resultados/relevos cuando el atleta aun no lo tiene.

### 7.7 `result`

Registra resultados individuales.

Campos principales:

- `id`
- `event_id`
- `athlete_id`
- `club_id`
- `lane`
- `heat_number`
- `rank_position`
- `result_time_text`
- `result_time_ms`
- `seed_time_text`
- `seed_time_ms`
- `points`
- `age_at_event`
- `birth_year_estimated`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `created_at`

Observaciones:

- `result` no se usa para resultados de relevos.
- `age_at_event` y `birth_year_estimated` son datos del resultado observado.

### 7.8 `relay_result`

Registra resultados de equipos de relevo.

Campos principales:

- `id`
- `event_id`
- `club_id`
- `relay_team_name`
- `lane`
- `heat_number`
- `rank_position`
- `result_time_text`
- `result_time_ms`
- `seed_time_text`
- `seed_time_ms`
- `points`
- `reaction_time`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `created_at`

### 7.9 `relay_result_member`

Registra integrantes de un relevo y su orden de posta.

Campos principales:

- `id`
- `relay_result_id`
- `athlete_id`
- `leg_order`
- `athlete_name_raw`
- `gender`
- `age_at_event`
- `birth_year_estimated`
- `created_at`

Restricciones relevantes:

- `leg_order` debe estar entre 1 y 4.
- `UNIQUE (relay_result_id, leg_order)` evita duplicar la misma posta dentro de un relevo.

### 7.10 `record`

Registra records nacionales, master u otros tipos que se definan.

Campos principales:

- `id`
- `record_type`
- `stroke`
- `distance_m`
- `gender`
- `age_group`
- `course_type`
- `result_time_text`
- `result_time_ms`
- `athlete_name`
- `club_name`
- `record_date`
- `competition_name`
- `city`
- `source_id`
- `source_url`
- `is_current`
- `created_at`
- `updated_at`

## 8. CSVs generados por el parser PDF

`backend/scripts/parse_results_pdf.py` genera:

- `club.csv`
- `event.csv`
- `athlete.csv`
- `result.csv`
- `relay_team.csv`
- `relay_swimmer.csv`
- `raw_results.csv`
- `raw_relay_team.csv`
- `raw_relay_swimmer.csv`
- `debug_unparsed_lines.csv`
- `metadata.json`
- un Excel consolidado

Columnas principales por CSV operativo:

### 8.1 `club.csv`

- `name`
- `short_name`
- `city`
- `region`
- `source_id`

### 8.2 `event.csv`

- `competition_id`
- `event_name`
- `stroke`
- `distance_m`
- `gender`
- `age_group`
- `round_type`
- `source_id`

### 8.3 `athlete.csv`

- `full_name`
- `gender`
- `club_name`
- `birth_year`
- `source_id`

### 8.4 `result.csv`

- `event_name`
- `athlete_name`
- `club_name`
- `rank_position`
- `age_at_event`
- `birth_year_estimated`
- `seed_time_text`
- `seed_time_ms`
- `result_time_text`
- `result_time_ms`
- `points`
- `status`
- `source_id`

### 8.5 `relay_team.csv`

- `event_name`
- `relay_team_name`
- `rank_position`
- `seed_time_text`
- `seed_time_ms`
- `result_time_text`
- `result_time_ms`
- `points`
- `status`
- `source_id`
- `page_number`
- `line_number`

### 8.6 `relay_swimmer.csv`

- `event_name`
- `relay_team_name`
- `leg_order`
- `swimmer_name`
- `gender`
- `age_at_event`
- `birth_year_estimated`
- `page_number`
- `line_number`

## 9. Definicion resumida de staging

### 9.1 `stg_club`

- `name`
- `short_name`
- `city`
- `region`
- `source_id`

### 9.2 `stg_event`

- `competition_id`
- `event_name`
- `stroke`
- `distance_m`
- `gender`
- `age_group`
- `round_type`
- `source_id`

### 9.3 `stg_athlete`

- `full_name`
- `gender`
- `club_name`
- `birth_year`
- `source_id`

### 9.4 `stg_result`

- `event_name`
- `athlete_name`
- `club_name`
- `rank_position`
- `result_time_text`
- `result_time_ms`
- `age_at_event`
- `birth_year_estimated`
- `points`
- `seed_time_text`
- `seed_time_ms`
- `status`
- `source_id`

### 9.5 `stg_relay_result`

- `event_name`
- `club_name`
- `relay_team_name`
- `lane`
- `heat_number`
- `rank_position`
- `result_time_text`
- `result_time_ms`
- `points`
- `reaction_time`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `seed_time_text`
- `seed_time_ms`

### 9.6 `stg_relay_result_member`

- `event_name`
- `club_name`
- `relay_team_name`
- `leg_order`
- `athlete_name`
- `gender`
- `age_at_event`
- `birth_year_estimated`

## 10. Flujo actual de carga

### 10.1 Parseo PDF

1. `parse_results_pdf.py` lee el PDF.
2. Detecta metadata de competencia cuando esta disponible.
3. Extrae eventos, individuales, equipos de relevo e integrantes.
4. Normaliza catalogos canonicos.
5. Genera CSVs y archivos raw/debug.

### 10.2 Carga a PostgreSQL

1. `run_pipeline_results.py` lee `club.csv`, `event.csv`, `athlete.csv`, `result.csv`.
2. Si existen `relay_team.csv` y `relay_swimmer.csv`, los transforma a `stg_relay_result` y `stg_relay_result_member`.
3. Carga todas las tablas staging con `COPY`.
4. Inserta o reutiliza clubes, eventos y atletas.
5. Inserta resultados individuales en `result`.
6. Inserta resultados de relevos en `relay_result`.
7. Inserta integrantes de relevos en `relay_result_member`.
8. Ejecuta chequeos de diagnostico para detectar filas sin match.

## 11. Decisiones relevantes

### 11.1 Relevos separados de individuales

Un relevo es un resultado de equipo. Por eso se modela en `relay_result`, con integrantes en `relay_result_member`, y no como cuatro filas en `result`.

### 11.2 Identidad de atletas

El pipeline cruza atletas principalmente por nombre normalizado, genero y año de nacimiento cuando existe. El club ayuda, pero no debe convertirse en identidad historica rigida.

### 11.3 Semantica de tiempos

- `seed_time_*`: tiempo de inscripcion o marca previa reportada por la fuente.
- `result_time_*`: tiempo final obtenido en la prueba.

### 11.4 Semantica de anio de nacimiento

- `athlete.birth_year`: atributo del atleta cuando se conoce o se infiere de forma consistente.
- `result.birth_year_estimated`: estimacion contextual del resultado.
- `relay_result_member.birth_year_estimated`: estimacion contextual del integrante en ese relevo.

## 12. Propuestas de proximos cambios

Estas propuestas no son obligatorias para el flujo actual, pero quedaron visibles al actualizar la documentacion:

- agregar constraints de unicidad para evitar duplicados en cargas repetidas de `result` y `relay_result`
- documentar una estrategia explicita de merge de atletas cuando aparezca mejor informacion historica
- agregar tests con fixtures chicos de CSV para verificar la carga end-to-end de individuales y relevos
- crear un documento corto de comandos operativos con ejemplos reales de parseo y carga
- revisar si `record.gender` debe alinearse con `event.gender` (`women`, `men`, `mixed`) o mantenerse como catalogo propio (`female`, `male`, `mixed`, `unknown`)
