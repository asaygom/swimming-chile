# Identidad visual

La identidad de SwimStats Chile combina superficies de marca oscuras con superficies claras para lectura intensiva de datos. Los tokens canónicos viven en `src/index.css` mediante `@theme` de Tailwind CSS v4; no existe un `tailwind.config` para colores.

## Paleta canónica

### Marca

| Token | Hex | Uso |
| --- | --- | --- |
| `brand-night` | `#020B1F` | Fondo oscuro principal |
| `brand-navy` | `#061A33` | Fondo oscuro secundario |
| `brand-panel` | `#0B2748` | Paneles sobre fondos oscuros |
| `brand-steel` | `#16476F` | Bordes y elementos secundarios oscuros |
| `brand-cyan` | `#00D8F5` | Acento principal de marca |
| `brand-turquoise` | `#21F2E7` | Acento complementario |
| `brand-pool` | `#0096FF` | Azul acuático de marca |
| `brand-white` | `#F4F8FF` | Texto principal sobre fondos oscuros |
| `brand-muted` | `#B8C7DA` | Texto secundario sobre fondos oscuros |
| `brand-subtle` | `#7F94AE` | Texto de baja jerarquía |

### Superficies claras y acción

| Token | Hex | Uso |
| --- | --- | --- |
| `canvas` | `#F8FAFC` | Lienzo de páginas de datos |
| `surface` | `#FFFFFF` | Tarjetas, tablas y controles |
| `ink` | `#0F172A` | Texto principal sobre superficies claras |
| `content-muted` | `#475569` | Texto secundario sobre superficies claras |
| `content-subtle` | `#64748B` | Texto de baja jerarquía sobre superficies claras |
| `line` | `#E2E8F0` | Divisores y bordes claros |
| `action` | `#0074C8` | Enlaces y acciones sobre blanco |

### Estados semánticos

| Significado | Base | Fuerte |
| --- | --- | --- |
| Éxito | `success` `#35F2A5` | `success-strong` `#047857` |
| Advertencia | `warning` `#FFD166` | `warning-strong` `#92400E` |
| Peligro | `danger` `#FF5C7A` | `danger-strong` `#BE123C` |

Las variantes base sirven como acentos o fondos suaves. Las variantes `strong` se usan para texto y controles cuando hace falta mayor contraste.

### Datos y dominio

| Token | Hex |
| --- | --- |
| `chart-axis` | `#CBD5E1` |
| `chart-grid` | `#E2E8F0` |
| `chart-primary` | `#0096FF` |
| `trend-improve` | `#35F2A5` |
| `trend-regress` | `#FF5C7A` |
| `course-scm` | `#0096FF` |
| `course-lcm` | `#7C3AED` |
| `course-open` | `#059669` |
| `medal-gold` | `#D97706` |
| `medal-silver` | `#64748B` |
| `medal-bronze` | `#C2410C` |

Los colores de tendencia, curso y medalla expresan significado de dominio: no deben sustituirse por colores de marca genéricos.

## Uso claro y oscuro

- Home y shell de aplicación usan la familia `brand-night` a `brand-steel`, con texto `brand-white` o `brand-muted`.
- Las páginas con tablas, rankings y alta densidad de datos usan `canvas`, `surface`, `ink` y `line`.
- Los paneles claros pueden convivir dentro del shell oscuro; la elección depende de la función de la superficie, no de una preferencia global del usuario.
- No hay modo oscuro global ni selector de tema. No agregues variantes `dark:` como sustituto de esta estrategia por superficie.

## Contraste y accesibilidad

- Un botón con fondo `brand-cyan` debe usar texto oscuro (`brand-night` o `ink`), nunca blanco.
- Enlaces sobre `surface` blanco deben usar `action` `#0074C8`; `brand-pool` y `brand-cyan` se reservan para marca, gráficos o fondos donde el contraste esté verificado.
- Texto sobre fondos oscuros: usa `brand-white` para contenido principal y `brand-muted` para contenido secundario. `brand-subtle` solo corresponde a contenido no crítico con contraste suficiente.
- No dependas únicamente del color para comunicar estado, tendencia, curso o posición: conserva texto, iconos, etiquetas o estructura asociada.
- Los estados de foco deben permanecer visibles y usar un token coherente con la acción o el estado semántico.
