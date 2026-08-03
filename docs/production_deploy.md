# Despliegue a producción del módulo en vivo

Runbook para publicar el llamador público, el control de voluntario, los
comunicados y la carga de sembrado.

Este documento registra **decisiones y trampas**, no inventarios. La lista
autoritativa de variables vive en `backend/.env.example` y
`frontend/.env.example`; la de migraciones, en `backend/sql/migrations/`. Aquí
solo está lo que esos archivos no pueden decir.

Supuestos al escribirlo: producción tiene el histórico y las migraciones hasta
la 014; backend en Railway, frontend en Vercel, ambos bajo subdominios de un
mismo dominio propio.

## 0. Por qué el dominio no es un detalle cosmético

Las cookies del módulo (`swimstats_admin_session` y `swimstats_live_operator`)
son `SameSite=Lax`. El navegador solo las envía si el frontend y la API
comparten **sitio registrable**.

- `swimstats.cl` + `api.swimstats.cl` → mismo sitio, funciona.
- `algo.vercel.app` + `algo.railway.app` → sitios distintos, el navegador
  descarta la cookie y fallan control de voluntario, comunicados y carga de
  sembrado.

La pantalla pública es anónima y funcionaría igual, así que **este fallo no se
nota hasta que alguien intenta operar el llamador**. Verificarlo es el paso 5.

Las cookies además son `Secure`: sobre HTTP no viajan. Esperar a que los
certificados TLS de ambos subdominios estén emitidos antes de probar nada.

## 1. Migraciones

Aplicar todo lo posterior a la 014, en orden. Ninguna es destructiva: solo
agregan tablas.

Desde una máquina fuera de Railway hay que usar la **URL pública** de la base.
La `DATABASE_URL` que consume la API apunta al host interno
(`*.railway.internal`), que solo resuelve dentro de la red privada de Railway:
con esa cadena el `psql` local falla por DNS, no por permisos.

```bash
export MIGRATION_URL='<DATABASE_PUBLIC_URL de Railway>'
for f in $(ls backend/sql/migrations/ | sort | sed -n '/^015_/,$p'); do
  psql "$MIGRATION_URL" -v ON_ERROR_STOP=1 -f "backend/sql/migrations/$f"
done
```

Verificación:

```sql
SELECT table_schema || '.' || table_name
FROM information_schema.tables
WHERE table_name IN ('live_heat_state', 'live_announcement', 'live_heat_movement',
                     'live_announcement_event', 'competition_live_branding',
                     'admin_session', 'user_competition_role')
ORDER BY 1;
```

Deben aparecer las siete.

## 2. Configuración que difiere de desarrollo

Las variables están en los `.env.example`. Lo que no se deduce de ahí:

- Van en el servicio que **ejecuta la API**, no en el de la base: PostgreSQL es
  un servicio gestionado y no lee la configuración de la aplicación.
- `DATABASE_URL` conviene declararla como referencia al servicio de base
  (`${{Postgres.DATABASE_URL}}`) y no como valor pegado, para que sobreviva a
  una rotación de credenciales.
- Si `DATABASE_URL` está definida, las variables `DB_HOST`, `DB_PORT`,
  `DB_NAME`, `DB_USER` y `DB_PASSWORD` no se leen nunca. Declararlas solo deja
  un secreto sin uso dando vueltas.
- `ALLOWED_ORIGINS` debe ser el origen público del frontend, con `https`, e
  incluir `www` si el sitio también responde ahí.
- `LIVE_HEAT_OPERATOR_COMPETITION_ID` es el id de la competencia **en
  producción**, que puede no coincidir con el local. Ver paso 3.
- `LIVE_HEAT_SESSION_SECRET` y el código de operador deben ser distintos a los
  de desarrollo. El código se guarda solo hasheado:
  `printf '%s' 'CODIGO' | sha256sum`.
- Las `VITE_*` son variables de build: cambiarlas exige **redesplegar** el
  frontend, no basta con guardarlas.
- En Supabase, *Authentication → URL Configuration*: el Site URL apunta al
  frontend y hay que agregar `https://<dominio>/admin/password` a Redirect URLs,
  o el enlace de recuperación de contraseña no vuelve al formulario.

**`LIVE_HEAT_TRUSTED_PROXY_CIDRS` merece su propio párrafo.** El acceso del
voluntario se limita a 5 intentos fallidos por 15 minutos, contados por IP de
cliente, y la API solo cree en `X-Forwarded-For` si quien conecta está dentro de
esa lista. Si queda vacía detrás del proxy de Railway, la API ve la IP del proxy
en todas las peticiones y **todos los voluntarios comparten un único contador**:
cinco tecleos equivocados de una persona dejan fuera a todo el mundo por 15
minutos. Es un fallo que solo aparece el día del evento y en el peor momento.

## 3. Datos de la competencia en producción

Nada de esto viaja con el código.

```sql
-- 1. Confirmar el id real antes que nada: de aquí sale la variable del operador.
SELECT id, name, start_date, competition_scope FROM core.competition
WHERE name ILIKE '%ñuñoa%';

-- 2. Rol global, para publicar sembrado desde la app.
INSERT INTO auth.user_role (user_id, club_id, role)
VALUES ((SELECT id FROM auth.user_account WHERE email = 'TU-CORREO'), NULL, 'platform_admin');

-- 3. Rol por competencia, para administrar comunicados y logo.
INSERT INTO auth.user_competition_role (user_id, competition_id, role)
VALUES ((SELECT id FROM auth.user_account WHERE email = 'TU-CORREO'), <ID>, 'competition_admin');
```

Después: ajustar `LIVE_HEAT_OPERATOR_COMPETITION_ID` a ese id y reiniciar la API,
publicar el sembrado desde `/competitions/<ID>/live/program` validando antes de
confirmar, y subir el logo desde `/competitions/<ID>/live/admin`.

Publicar sembrado exige que el nombre y las fechas del archivo coincidan con una
competencia existente. Si la competencia todavía no está en producción, este
paso se bloquea.

## 4. Despliegue

`master` corre tests, lint y build en CI antes de cualquier despliegue. Backend
y frontend se despliegan desde sus respectivos proveedores sin pasos manuales
adicionales.

## 5. Verificación posterior

En este orden, porque cada paso depende del anterior:

1. `/api/ready` responde `ready`.
2. `/competitions/<ID>/live` muestra el board con el heat y el logo. Es anónimo:
   **si esto funciona no prueba nada sobre las cookies**.
3. **La prueba que importa**: entrar a `/competitions/<ID>/live/control` desde
   otro dispositivo, ingresar el código y mover un heat. Si el código se acepta
   pero la siguiente acción devuelve 401, el problema es la cookie cross-site
   del paso 0, no el código.
4. `/competitions/<ID>/live/admin`: crear y activar un comunicado, verlo
   aparecer en la pantalla pública.
5. `/competitions/<ID>/live/program`: validar un sembrado sin publicarlo.
6. Recuperación de contraseña en `/admin/password`.

## 6. Riesgos conocidos el día del evento

- **El código de operador sirve para una sola competencia.**
  `LIVE_HEAT_OPERATOR_COMPETITION_ID` es una variable única. Cambiar de
  competencia exige editarla y reiniciar la API.
- **Carga del board público.** Cada pantalla encendida consulta heat y
  comunicados cada 2,5 s y el logo cada 60 s: unas 49 consultas por minuto por
  pantalla. Con varias pantallas y un plan chico conviene mirar el consumo
  antes del evento.
- **El filesystem del deploy es efímero.** Los artefactos del sembrado no
  sobreviven a un redespliegue. La publicación y sus entradas viven en la base;
  el resumen de validación solo existe en la respuesta HTTP.
- **Republicar durante el evento** deja la publicación anterior en `superseded`
  y el board cae a "sin estado" hasta que el voluntario adopte la nueva
  publicación desde el control.
- **Límite de tamaño del body** en la subida de sembrado: la API acepta hasta
  16 MiB, pero el proveedor puede cortar antes. Un CSV de un par de cientos de
  KB no es problema; si alguna vez llega un PDF grande, probarlo antes y no ese
  día.

## 7. Reversa

- Frontend y backend: redesplegar la versión anterior desde su proveedor.
- Migraciones: las posteriores a la 014 solo agregan tablas nuevas, así que un
  rollback de código no exige revertirlas. Volver atrás el esquema implicaría
  perder la historia de auditoría ya registrada.
