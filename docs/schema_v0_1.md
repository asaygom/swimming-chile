# Schema v0.1 - Plataforma de datos de natación Chile

## 1. Propósito del documento

Este documento define el esquema lógico inicial `v0.1` para la plataforma de datos de natación en Chile.

El objetivo de esta versión es dejar una base clara, simple y extensible para:

- registrar fuentes de información
- registrar clubes
- registrar piscinas
- registrar competencias
- registrar pruebas dentro de cada competencia
- registrar nadadores
- registrar resultados
- registrar récords

Este esquema está pensado para soportar el MVP del proyecto y servir como base para la futura implementación en PostgreSQL.

---

## 2. Objetivo del MVP

La primera versión del proyecto debe permitir:

- mostrar competencias nacionales y máster
- mostrar pruebas por competencia
- mostrar resultados
- mostrar récords
- mostrar clubes o equipos
- mostrar piscinas o recintos

Todavía no se considera en esta versión:

- sistema de usuarios
- login
- entrenadores
- asociaciones como entidad separada
- afiliación histórica atleta-club
- splits o parciales por prueba
- disponibilidad horaria en tiempo real de piscinas
- rankings materializados avanzados

---

## 3. Principios del diseño

### 3.1 No sobrediseñar
El modelo inicial debe resolver el MVP, no todos los posibles casos futuros del deporte.

### 3.2 Mantener trazabilidad
Cada dato importante debe poder asociarse a una fuente.

### 3.3 Separar dato bruto y dato normalizado
Ejemplo: guardar tiempos como texto y también como valor numérico en milisegundos.

### 3.4 Permitir incompletitud
La base debe tolerar datos faltantes, porque muchas fuentes públicas no estarán completas.

### 3.5 Diseñar para crecer
El modelo debe permitir extenderse a futuro sin obligar a rehacer todo desde cero.

---

## 4. Alcance del esquema v0.1

Las tablas definidas para esta versión son:

- `sources`
- `clubs`
- `pools`
- `competitions`
- `events`
- `athletes`
- `results`
- `records`

---

## 5. Descripción general de entidades

### 5.1 sources
Registra de dónde proviene la información.

Ejemplos:
- FECHIDA
- FCHMN
- World Aquatics
- carga manual
- sitio web municipal

### 5.2 clubs
Registra clubes, equipos o instituciones vinculadas a la natación.

### 5.3 pools
Registra piscinas o recintos disponibles para entrenar o competir.

### 5.4 competitions
Registra competencias, campeonatos, torneos o controles.

### 5.5 events
Registra cada prueba o evento dentro de una competencia.

### 5.6 athletes
Registra nadadores.

### 5.7 results
Registra resultados por prueba y atleta.

### 5.8 records
Registra récords nacionales, máster u otros tipos que se definan.

---

## 6. Definición de tablas

## 6.1 Tabla `source`

### Propósito
Registrar la procedencia de la información.

### Campos
- `id`
- `name`
- `source_type`
- `base_url`
- `notes`
- `last_checked_at`
- `created_at`

### Campos obligatorios
- `name`
- `source_type`

### Observaciones
Esta tabla es clave para mantener trazabilidad y validar la confianza de la información.

---

## 6.2 Tabla `club`

### Propósito
Registrar clubes, equipos o instituciones.

### Campos
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

### Campos obligatorios
- `name`

### Observaciones
- `association_name` se almacena como texto en esta versión.
- Más adelante podría existir una tabla separada de asociaciones si realmente aporta valor.
- `source_id` permite identificar la fuente del club.

---

## 6.3 Tabla `pool`

### Propósito
Registrar piscinas o recintos.

### Campos
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

### Campos obligatorios
- `name`

### Observaciones
- `pool_length_m` es clave para distinguir 25m y 50m.
- `latitude` y `longitude` permitirán construir el mapa.
- Esta tabla debe tolerar carga manual e información incompleta.
- `public_access_type` permite indicar si la piscina es pública, municipal, privada, de club, escolar, universitaria u otra.

---

## 6.4 Tabla `competition`

### Propósito
Registrar competencias, torneos o campeonatos.

### Campos
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

### Campos obligatorios
- `name`

### Observaciones
- `season_year` ayuda a filtrar por temporada.
- `venue_name` se guarda aunque no exista una relación consolidada con `pools`.
- `pool_id` será opcional.
- `competition_type` servirá para distinguir competencias nacionales, regionales, máster, open u otras.
- `course_type` debe distinguir entre 25m, 50m u otro tipo no determinado.
- `status` permitirá registrar si la competencia está planificada, finalizada, cancelada o postergada.

---

## 6.5 Tabla `event`

### Propósito
Registrar pruebas o eventos dentro de una competencia.

### Campos
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

### Campos obligatorios
- `competition_id`
- `event_name`

### Observaciones
- `event_name` debe conservar el nombre lo más cercano posible a la fuente original.
- `stroke`, `distance_m`, `gender`, `age_group` y `round_type` representan una versión parcialmente normalizada del nombre del evento.
- `age_group` se deja como texto en esta versión para no sobrediseñar categorías.

---

## 6.6 Tabla `athlete`

### Propósito
Registrar nadadores.

### Campos
- `id`
- `full_name`
- `gender`
- `birth_year`
- `nationality`
- `club_id`
- `source_id`
- `created_at`
- `updated_at`

### Campos obligatorios
- `full_name`

### Observaciones
- Puede haber duplicados iniciales por diferencias en escritura del nombre.
- `club_id` se deja opcional para tolerar fuentes incompletas.
- La deduplicación y afiliación histórica atleta-club se resolverán en versiones futuras.

---

## 6.7 Tabla `result`

### Propósito
Registrar resultados de atletas en pruebas específicas.

### Campos
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
- `reaction_time`
- `record_flag`
- `status`
- `source_id`
- `source_url`
- `created_at`

### Campos obligatorios
- `event_id`
- `athlete_id`

### Observaciones
- `club_id` también se guarda aquí, aunque exista en `athletes`, para congelar el club histórico asociado a ese resultado.
- `result_time_text` debe guardar el valor original tal como aparece en la fuente.
- `result_time_ms` permitirá análisis, comparación y ordenamiento.
- `status` permitirá registrar casos como DNS, DNF, DSQ o válido.
- `record_flag` podrá almacenar marcas como NR, CR, MR u otras equivalentes.

---

## 6.8 Tabla `record`

### Propósito
Registrar récords vigentes o históricos.

### Campos
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

### Campos obligatorios
- `record_type`
- `distance_m`
- `gender`
- `course_type`
- `result_time_text`

### Observaciones
- En esta versión se privilegia flexibilidad.
- `athlete_name` y `club_name` se guardan como texto para tolerar fuentes no totalmente consolidadas.
- Más adelante esta tabla podrá vincularse más estrictamente con `athletes`, `clubs` y `events`.

---

## 7. Relaciones principales

Las relaciones principales del modelo son:

- un `source` puede estar asociado a múltiples registros en distintas tablas
- un `competition` tiene muchos `events`
- un `event` tiene muchos `results`
- un `athlete` puede tener muchos `results`
- un `club` puede estar asociado a muchos `athletes`
- un `club` puede aparecer en muchos `results`
- un `pool` puede estar asociado a muchas `competitions`

### Vista conceptual

```text
source
 ├── club
 ├── pool
 ├── competition
 ├── event
 ├── athlete
 ├── result
 └── record

competition
 └── event
      └── result
           ├── athlete
           └── club

pool
 └── competition
```

---

## 8. Campos obligatorios por tabla

### `source`
- `name`
- `source_type`

### `club`
- `name`

### `pool`
- `name`

### `competition`
- `name`

### `event`
- `competition_id`
- `event_name`

### `athlete`
- `full_name`

### `result`
- `event_id`
- `athlete_id`

### `record`
- `record_type`
- `distance_m`
- `gender`
- `course_type`
- `result_time_text`

---

## 9. Valores controlados sugeridos

En esta versión no se crearán tablas catálogo separadas. Se utilizarán valores controlados mediante la aplicación o mediante restricciones simples en PostgreSQL.

### 9.1 `competition_type`
Valores sugeridos:
- `national`
- `regional`
- `master`
- `open`
- `school`
- `other`

### 9.2 `course_type`
Valores sugeridos:
- `scm`
- `lcm`
- `unknown`

### 9.3 `status` en `competition`
Valores sugeridos:
- `planned`
- `finished`
- `cancelled`
- `postponed`

### 9.4 `gender`
Valores sugeridos:
- `male`
- `female`
- `mixed`
- `unknown`

### 9.5 `stroke`
Valores sugeridos:
- `freestyle`
- `backstroke`
- `breaststroke`
- `butterfly`
- `medley`
- `relay`
- `mixed`
- `unknown`

### 9.6 `round_type`
Valores sugeridos:
- `heats`
- `final`
- `timed_final`
- `semifinal`
- `unknown`

### 9.7 `indoor_outdoor`
Valores sugeridos:
- `indoor`
- `outdoor`
- `mixed`
- `unknown`

### 9.8 `public_access_type`
Valores sugeridos:
- `public`
- `municipal`
- `club`
- `private`
- `school`
- `university`
- `unknown`

### 9.9 `status` en `result`
Valores sugeridos:
- `valid`
- `dns`
- `dnf`
- `dsq`
- `scratch`
- `unknown`

---

## 10. Validaciones mínimas sugeridas

Cuando este esquema se traduzca a SQL, se recomienda considerar al menos las siguientes validaciones:

- `pool_length_m > 0`
- `lanes_count > 0`
- `distance_m > 0`
- `result_time_ms >= 0`
- `season_year >= 1900`
- `birth_year >= 1900`
- `latitude` entre -90 y 90
- `longitude` entre -180 y 180

---

## 11. Decisiones intencionales de esta versión

### Incluido
- trazabilidad por fuente
- flexibilidad para datos incompletos
- separación entre dato bruto y dato normalizado
- soporte para calendario, resultados, récords, clubes y piscinas

### Excluido
- asociaciones como tabla propia
- entrenadores
- usuarios
- rankings materializados
- splits por prueba
- disponibilidad detallada por horario en piscinas
- historial formal atleta-club
- lógica avanzada de deduplicación

---

## 12. Riesgos y observaciones

- La tabla `athlete` probablemente requerirá deduplicación futura.
- La tabla `record` está pensada para flexibilidad, no para máxima pureza relacional.
- Piscinas y clubes probablemente necesitarán una mezcla de automatización y carga manual.
- Un exceso de restricciones en SQL podría dificultar la carga inicial de datos reales.
- Un exceso de texto libre podría generar inconsistencias si no se controlan los valores.

---