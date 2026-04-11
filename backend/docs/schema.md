# Schema v0.1 - Plataforma de datos de natación Chile

## 1. Propósito del documento

Este documento define el esquema lógico `v0.1` actualmente operativo para la plataforma de datos de natación en Chile.

El objetivo de esta versión es dejar una base clara, simple y extensible para:

- registrar fuentes de información
- registrar clubes
- registrar piscinas
- registrar competencias
- registrar pruebas dentro de cada competencia
- registrar nadadores
- registrar resultados individuales
- registrar resultados de relevos y sus integrantes
- registrar récords
- soportar cargas staging desde Excel, CSV y parser PDF

---

## 2. Estado actual del proyecto

A la fecha, el proyecto ya tiene funcionando:

- parser PDF estable para resultados individuales y relevos
- carga a `core` estable para clubes, eventos, atletas y resultados individuales
- separación de relevos en entidades propias
- staging tables para cargas controladas antes de insertar en tablas core

Actualmente:

- `event` puede contener pruebas individuales y relevos
- `result` contiene resultados individuales
- `relay_result` contiene resultados de equipos de relevo
- `relay_result_member` contiene los integrantes de cada relevo

---

## 3. Principios del diseño

### 3.1 No sobrediseñar
El modelo debe resolver el flujo actual sin bloquear extensiones futuras.

### 3.2 Mantener trazabilidad
Cada dato importante debe poder asociarse a una fuente.

### 3.3 Separar dato bruto y dato normalizado
Ejemplo: guardar tiempos como texto y también como valor numérico en milisegundos.

### 3.4 Permitir incompletitud
La base debe tolerar datos faltantes, porque muchas fuentes públicas no estarán completas.

### 3.5 Separar staging de core
Las cargas pasan primero por tablas staging para facilitar validación, normalización y deduplicación.

---

## 4. Alcance del esquema v0.1

Las tablas core definidas para esta versión son:

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

Las tablas staging definidas para esta versión son:

- `stg_club`
- `stg_event`
- `stg_athlete`
- `stg_result`
- `stg_relay_result`
- `stg_relay_result_member`

---

## 5. Catálogos canónicos actuales

### 5.1 `event.gender`
Valores esperados:

- `women`
- `men`
- `mixed`

### 5.2 `athlete.gender`
Valores esperados:

- `female`
- `male`

### 5.3 `event.stroke` y `record.stroke`
Valores esperados:

- `freestyle`
- `backstroke`
- `breaststroke`
- `butterfly`
- `individual_medley`
- `medley_relay`
- `freestyle_relay`

### 5.4 `result.status` y `relay_result.status`
Valores esperados:

- `valid`
- `dns`
- `dnf`
- `dsq`
- `scratch`
- `unknown`

---

## 6. Descripción general de entidades

### 6.1 `source`
Registra de dónde proviene la información.

Ejemplos:
- FECHIDA
- FCHMN
- World Aquatics
- carga manual
- sitio web municipal

### 6.2 `club`
Registra clubes, equipos o instituciones vinculadas a la natación.

### 6.3 `pool`
Registra piscinas o recintos disponibles para entrenar o competir.

### 6.4 `competition`
Registra competencias, campeonatos, torneos o controles.

### 6.5 `event`
Registra cada prueba o evento dentro de una competencia, incluyendo individuales y relevos.

### 6.6 `athlete`
Registra nadadores.

### 6.7 `result`
Registra resultados individuales por prueba y atleta.

### 6.8 `relay_result`
Registra resultados por equipo en pruebas de relevo.

### 6.9 `relay_result_member`
Registra los integrantes de cada relevo y su orden de posta.

### 6.10 `record`
Registra récords nacionales, máster u otros tipos que se definan.

---

## 7. Definición resumida de tablas core

### 7.1 `source`
Campos principales:
- `id`
- `name`
- `source_type`
- `base_url`
- `notes`
- `last_checked_at`
- `created_at`

### 7.2 `club`
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

Observación:
- `event` almacena tanto pruebas individuales como relevos.

### 7.6 `athlete`
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

### 7.7 `result`
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
- `points`
- `age`
- `birth_year_estimated`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `created_at`

Observación:
- `result` se usa solo para resultados individuales.

### 7.8 `relay_result`
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
- `points`
- `reaction_time`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `created_at`

### 7.9 `relay_result_member`
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

Observación:
- `relay_result_member` usa `UNIQUE (relay_result_id, leg_order)` para impedir duplicidad de integrantes en la misma posta.

### 7.10 `record`
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

---

## 8. Definición resumida de tablas staging

### 8.1 `stg_club`
Propósito:
- carga preliminar de clubes antes de normalizar e insertar en `club`

Campos:
- `name`
- `short_name`
- `city`
- `region`
- `source_id`

### 8.2 `stg_event`
Propósito:
- carga preliminar de eventos antes de normalizar e insertar en `event`

Campos:
- `competition_id`
- `event_name`
- `stroke`
- `distance_m`
- `gender`
- `age_group`
- `round_type`
- `source_id`

### 8.3 `stg_athlete`
Propósito:
- carga preliminar de atletas antes de normalizar e insertar en `athlete`

Campos:
- `full_name`
- `gender`
- `club_name`
- `source_id`

### 8.4 `stg_result`
Propósito:
- carga preliminar de resultados individuales antes de insertar en `result`

Campos:
- `event_name`
- `athlete_name`
- `club_name`
- `rank_position`
- `result_time_text`
- `result_time_ms`
- `age_at_event`
- `birth_year_estimated`
- `status`
- `source_id`

### 8.5 `stg_relay_result`
Propósito:
- carga preliminar de resultados de relevo antes de insertar en `relay_result`

Campos:
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

### 8.6 `stg_relay_result_member`
Propósito:
- carga preliminar de integrantes de relevo antes de insertar en `relay_result_member`

Campos:
- `event_name`
- `club_name`
- `relay_team_name`
- `leg_order`
- `athlete_name`
- `gender`
- `age_at_event`
- `birth_year_estimated`

---

## 9. Flujo actual de carga

### 9.1 Resultados individuales
Flujo operativo actual:

1. parser PDF o carga manual genera CSV/Excel
2. datos pasan por `stg_club`, `stg_event`, `stg_athlete`, `stg_result`
3. pipeline normaliza y deduplica
4. inserción en `club`, `event`, `athlete`, `result`

### 9.2 Relevos
Flujo parcialmente implementado:

1. parser PDF ya genera `relay_team.csv` y `relay_swimmer.csv`
2. el esquema ya tiene `stg_relay_result`, `stg_relay_result_member`, `relay_result` y `relay_result_member`
3. falta completar el pipeline para la carga end-to-end de relevos a `core`

---

## 10. Decisiones de modelado relevantes

### 10.1 Relevos separados de resultados individuales
No se modelan relevos dentro de `result`.

Razón:
- un relevo es un resultado de equipo, no un resultado individual repetido cuatro veces
- además requiere integrantes y orden de posta

### 10.2 Tiempo textual y tiempo numérico
Se conserva:
- `result_time_text` para trazabilidad y presentación
- `result_time_ms` para orden, ranking y análisis

### 10.3 Diferencia entre género de evento y género de atleta
Se mantiene separada:
- `event.gender`: `women | men | mixed`
- `athlete.gender`: `female | male`

Esto evita mezclar la denominación competitiva del evento con el sexo/género del atleta dentro del modelo.

---

## 11. Próximos pasos esperables

Los siguientes pasos razonables del proyecto son:

- completar la carga a core de `relay_result` y `relay_result_member`
- probar deduplicación de relevos en cargas repetidas
- definir si se agregará unicidad adicional en resultados individuales y relevos
- documentar scripts estables y comandos operativos del proceso

