# Checklist pre-carga y full reload

Este checklist prepara una carga controlada a core. No reemplaza las compuertas
del batch runner: solo ordena las decisiones humanas antes de ejecutar `--load`.

## Principios

- No cargar desde manifests exploratorios.
- No cargar documentos `failed` ni `requires_review`.
- No cargar documentos sin `competition_scope` curado.
- No guardar passwords en archivos, summaries ni comandos versionados.
- Hacer backup antes de cualquier wipe o full reload.
- Registrar evidencia auditable antes y despues de la carga.

## 1. Evidencia de entrada

- Confirmar manifest de discovery usado.
- Confirmar summary de descarga con checksums.
- Confirmar summary de batch validation.
- Confirmar que los documentos candidatos estan `validated`.
- Revisar que el batch summary no tenga issues bloqueantes.
- Revisar que el scope local no dependa de keywords automaticas.

## 2. Manifest congelado

- Crear una lista curada de `source_url` locales permitidas.
- Ejecutar `freeze_validated_manifest.py` con `--competition-scope fchmn_local`.
- Verificar que el manifest congelado contiene solo documentos locales.
- Verificar que documentos `failed` y `requires_review` quedaron excluidos.
- Verificar que cada entrada tiene `competition_scope`.
- Guardar el manifest congelado como artefacto local auditable.

## 3. Backup

- Crear backup de PostgreSQL antes de cargar.
- Registrar fecha, base, schema y nombre del archivo de backup.
- Guardar el backup fuera del repo o en una ruta ignorada por git.
- Confirmar que el backup puede restaurarse antes de hacer wipe.

Plantilla de comando:

```powershell
pg_dump --format=custom --dbname natacion_chile --schema core --file <ruta_backup_ignorada>
```

## 4. Wipe controlado

Ejecutar wipe solo si se busca una recarga completa y el backup ya fue verificado.

- Confirmar explicitamente el alcance: schema completo, tablas core especificas o staging.
- No borrar tablas operativas si se necesita conservar auditoria historica.
- Documentar el SQL exacto antes de ejecutarlo.
- Preferir transaccion manual con conteos antes/despues.

Orden orientativo si se hace wipe de datos core cargables:

1. `relay_result_member`
2. `relay_result`
3. `result`
4. `athlete`
5. `event`
6. `club`
7. `competition`

Revisar dependencias reales en `backend/sql/schema.sql` antes de ejecutar.

## 5. Carga

- Ejecutar `run_results_batch.py --manifest <manifest_congelado> --load`.
- Usar credenciales solo en la terminal o variables locales no versionadas.
- Escribir `--summary-json` en `backend/data/raw/batch_summaries/`.
- Confirmar que el summary de carga queda `loaded`.
- Confirmar que los comandos registrados tienen password redactado.

## 6. Validacion post-load

Validar conteos agregados:

- documentos cargados esperados vs `load_run completed`
- competencias esperadas en `core.competition`
- eventos por competencia
- resultados individuales
- resultados de relevos
- integrantes de relevos
- issues persistidos en `validation_issue`

Validar trazabilidad:

- `source_document.checksum_sha256` presente cuando hubo PDF.
- `source_document.source_url` presente cuando venia desde manifest.
- `load_run.input_dir` apunta a la carpeta parseada esperada.
- `load_run.parser_version` coincide con `metadata.json`.

Validar idempotencia minima:

- Reejecutar una carga controlada solo si el manifest congelado y el backup lo permiten.
- Confirmar que los conteos core no se duplican.
- Confirmar que el mismo `pdf_sha256` reutiliza `source_document`.

## 7. Rollback

Aplicar rollback si:

- el batch no queda `loaded`,
- aparecen conteos imposibles,
- faltan documentos esperados,
- hay duplicacion evidente,
- se cargo un scope equivocado.

Acciones:

- Guardar el summary fallido.
- No intentar corregir datos manualmente en core.
- Restaurar desde backup si la carga contamino core.
- Registrar el hallazgo en el handoff antes de reintentar.

## Cierre

- Revisar `git status`.
- No versionar PDFs, CSVs completos, Excels, summaries reales ni backups.
- Actualizar `backend/docs/ai_workflow.md` solo si cambia el siguiente paso o una decision vigente.
