from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROUTER = ROOT / "frontend/src/app/router.tsx"
SERVICE = ROOT / "frontend/src/features/competitions/api/competitionService.ts"
SCHEMA = ROOT / "frontend/src/lib/schemas/competition.ts"
PAGE = ROOT / "frontend/src/features/competitions/pages/CompetitionLiveHeatPage.tsx"
STYLES = ROOT / "frontend/src/index.css"
CONTROL_PAGE = ROOT / "frontend/src/features/competitions/pages/CompetitionLiveHeatControlPage.tsx"
ADMIN_PAGE = ROOT / "frontend/src/features/competitions/pages/CompetitionLiveAnnouncementAdminPage.tsx"
AUTH_SCHEMA = ROOT / "frontend/src/lib/schemas/auth.ts"
FRONTEND_ENV = ROOT / "frontend/.env.example"


def test_public_live_heat_route_uses_validated_read_only_api_contract():
    router = ROUTER.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")
    schema = SCHEMA.read_text(encoding="utf-8")

    assert "path: '/competitions/:id/live'" in router
    assert "<CompetitionLiveHeatPage />" in router
    assert "async getLiveHeat(id: string)" in service
    assert "`${API_BASE_URL}/api/competitions/${id}/live-heat`" in service
    assert "LiveHeatResponseSchema.parse(data)" in service
    assert "status: z.enum(['not_started', 'active', 'paused', 'finished'])" in schema
    assert "pool_role: z.enum(['main', 'competition', 'training'])" in schema


def test_public_live_heat_page_covers_operational_read_states_accessibly():
    source = PAGE.read_text(encoding="utf-8")

    assert "refetchInterval: LIVE_HEAT_POLL_INTERVAL_MS" in source
    assert "const LIVE_HEAT_POLL_INTERVAL_MS = 2_500" in source
    assert 'aria-live="polite"' in source
    assert "Llamador aún no iniciado" in source
    assert "Pista" in source
    assert "to={`/competitions/${id}?tab=series`}" in source
    assert ".post(" not in source.lower()
    assert ".put(" not in source.lower()


def test_live_routes_bypass_the_ordinary_site_shell():
    router = ROUTER.read_text(encoding="utf-8")
    live_route = "path: '/competitions/:id/live'"
    control_route = "path: '/competitions/:id/live/control'"

    assert live_route in router
    assert control_route in router
    assert router.index(live_route) < router.index("element: <MainLayout />")
    assert router.index(control_route) < router.index("element: <MainLayout />")


def test_public_live_heat_preserves_caller_board_hierarchy_and_projects_next_heat():
    source = PAGE.read_text(encoding="utf-8")

    assert 'data-live-layout="caller-board"' in source
    assert "getMeetProgram" in source
    assert "nextHeat" in source
    assert "PRÓXIMA SERIE" in source
    assert "HEAT" in source
    assert "Pista" in source


def test_public_live_heat_is_a_viewport_locked_responsive_tv_board():
    source = PAGE.read_text(encoding="utf-8")

    assert 'className="flex h-dvh max-h-dvh flex-col overflow-hidden' in source
    assert 'className="mx-auto flex w-full min-h-0 flex-1' in source
    assert "md:grid-cols-[minmax(0,1fr)_minmax(260px,0.48fr)]" in source
    assert "min-h-[55dvh]" not in source
    assert "min-h-[35dvh]" not in source
    assert 'data-live-section="heat"' in source
    assert 'data-live-section="event"' in source
    assert 'data-live-section="tournament"' in source
    assert 'data-live-entry="current"' in source
    assert 'data-live-entry-club="inline"' in source
    assert "[@media(max-height:700px)_and_(max-width:767px)]:grid-cols-3" in source
    assert "[@media(max-height:700px)_and_(max-width:767px)]:grid-rows-[minmax(0,1fr)_minmax(0,1fr)]" in source
    assert "[@media(max-height:700px)_and_(max-width:767px)]:hidden" in source


def test_next_heat_projection_can_cross_sessions_in_the_same_pool_partition():
    source = PAGE.read_text(encoding="utf-8")

    assert "sessionNumber: session.session_number" in source
    assert "heat.sessionNumber === state.session_number" in source
    assert ".filter((item) =>" in source
    assert "item.pool_role === state.pool_role" in source


def test_public_board_distinguishes_initial_failure_from_degraded_cached_data():
    source = PAGE.read_text(encoding="utf-8")

    assert "!programQuery.data || !liveHeatQuery.data" in source
    assert "No pudimos cargar el estado del llamador" in source
    assert "Datos potencialmente desactualizados" in source
    assert "programQuery.refetch()" in source
    assert "liveHeatQuery.refetch()" in source


def test_control_preserves_dedicated_controller_interactions():
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert 'data-live-layout="heat-controller"' in source
    assert 'htmlFor="event-selector"' in source
    assert 'htmlFor="heat-selector"' in source
    assert "Heat llamado" in source
    assert "Anterior" in source
    assert "Siguiente" in source


def test_operator_control_publishes_event_and_heat_selection_immediately_as_active():
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "const statusLabels" not in source
    assert "Por comenzar" not in source
    assert "En curso" not in source
    assert "Pausado" not in source
    assert "Finalizado" not in source
    assert "<fieldset" not in source
    assert "async (target: HeatOption)" in source
    assert "status: 'active'" in source
    assert "update(heats[selectedIndex - 1])" in source
    assert "update(heats[selectedIndex + 1])" in source
    assert "if (first) void update(first);" in source
    assert "const target = heats.find((heat) => heatKey(heat) === event.target.value);" in source
    assert "if (target) void update(target);" in source
    assert source.count("disabled={saving}") >= 2
    assert "update(selected)" not in source
    assert "Aplicar selecci" not in source
    assert "Inicializar llamador" not in source


def test_operator_control_serializes_updates_and_auto_publishes_once():
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "const updateInFlightRef = useRef(false)" in source
    assert "if (updateInFlightRef.current) return;" in source
    assert "updateInFlightRef.current = true;" in source
    assert "updateInFlightRef.current = false;" in source
    assert source.index("await liveQuery.refetch()") < source.index(
        "updateInFlightRef.current = false;"
    )
    assert "const autoPublishedHeatKeyRef = useRef('')" in source
    assert "const firstHeatKey = heatKey(heats[0]);" in source
    assert "autoPublishedHeatKeyRef.current === firstHeatKey" in source
    assert "autoPublishedHeatKeyRef.current = firstHeatKey;" in source
    assert "void update(heats[0]);" in source
    assert "autoPublishedHeatKeyRef.current = '';" in source
    assert "Reintentar" in source
    assert "autoPublishedHeatKeyRef.current = heatKey(heats[0]);" in source
    assert source.index("await competitionService.updateLiveHeat") < source.index(
        "setSelectedKey(heatKey(target))"
    )


def test_operator_control_polls_and_adopts_remote_revision_outside_local_publish():
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "const LIVE_CONTROL_POLL_INTERVAL_MS = 2_500" in source
    assert "refetchInterval: LIVE_CONTROL_POLL_INTERVAL_MS" in source
    assert "const stateVersionKey" in source
    assert "const observedStateVersionRef = useRef('')" in source
    assert "const locallyPublishedStateVersionRef = useRef('')" in source
    assert "saving || updateInFlightRef.current" in source
    assert "locallyPublishedStateVersionRef.current = stateVersionKey(updated.state)" in source
    assert "observedStateVersionRef.current === version" in source
    assert "setSelectedKey('')" in source
    assert "Otro voluntario" in source


def test_operator_control_exposes_explicit_cookie_logout():
    service = SERVICE.read_text(encoding="utf-8")
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "async deleteLiveHeatSession(id: string)" in service
    assert "/live-heat/session/logout`" in service
    assert "method: 'POST', credentials: 'include'" in service
    assert "await competitionService.deleteLiveHeatSession(id!)" in source
    assert "setAuthenticated(false)" in source
    assert "Cerrar sesión" in source


def test_operator_update_response_schema_matches_put_contract_without_get_derivations():
    schema = SCHEMA.read_text(encoding="utf-8")
    update_schema = schema.split("export const LiveHeatUpdateStateSchema", 1)[1].split(
        "export type LiveHeatUpdate", 1
    )[0]

    assert "event_name" not in update_schema
    assert "heat_total" not in update_schema
    assert "state: LiveHeatUpdateStateSchema" in update_schema


def test_operator_control_reads_typed_private_recent_movement_history():
    schema = SCHEMA.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "LiveHeatMovementSchema" in schema
    assert "LiveHeatHistoryResponseSchema" in schema
    assert "async getLiveHeatHistory(id: string, limit: number = 8)" in service
    assert "/live-heat/history?limit=${limit}`" in service
    assert "credentials: 'include'" in service
    assert "queryKey: ['competition-live-heat-history', id]" in source
    assert source.count("refetchInterval: LIVE_CONTROL_POLL_INTERVAL_MS") >= 2
    assert "Movimientos recientes" in source
    assert "movement.is_current_session ? 'Esta sesi\\u00f3n' : 'Otra sesi\\u00f3n'" in source
    assert "movement.previous_event_number" in source


def test_live_heat_spanish_copy_is_valid_utf8_without_mojibake():
    sources = (
        PAGE.read_text(encoding="utf-8"),
        CONTROL_PAGE.read_text(encoding="utf-8"),
        SERVICE.read_text(encoding="utf-8"),
    )

    for source in sources:
        assert not any(marker in source for marker in ("\u00c3", "\u00c2", "\ufffd", "\u00e2\u20ac"))
    control = sources[1]
    assert "Código temporal" in control
    assert "La sesión expiró" in control
    assert "Pantalla pública" in control
    assert "Validando…" in control
    assert " · Piscina" in control
    assert "No se pudo iniciar la sesión" in sources[2]


def test_operator_control_uses_cookie_session_and_optimistic_concurrency():
    router = ROUTER.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")
    schema = SCHEMA.read_text(encoding="utf-8")
    source = CONTROL_PAGE.read_text(encoding="utf-8")

    assert "path: '/competitions/:id/live/control'" in router
    assert "<CompetitionLiveHeatControlPage />" in router
    assert "/live-heat/session`" in service
    assert "credentials: 'include'" in service
    assert "body: JSON.stringify(update)" in service
    assert "expected_revision: number" in schema
    assert "error.status === 409" in source
    assert "getMeetProgram" in source
    assert "getLiveHeat" in source
    assert "localStorage" not in source
    assert "sessionStorage" not in source
    assert "URLSearchParams" not in source
    assert 'type="password"' in source
    assert "Aplicar selecci" not in source
    assert 'aria-live="polite"' in source
    assert "target.stage_number === liveState.stage_number" in source
    assert "target.pool_role === liveState.pool_role" in source


def test_public_announcement_api_is_typed_validated_and_independent_from_live_heat():
    service = SERVICE.read_text(encoding="utf-8")
    schema = SCHEMA.read_text(encoding="utf-8")

    assert "LiveAnnouncementSchema" in schema
    assert "display_mode: z.enum(['fullscreen', 'ticker'])" in schema
    assert "announcement: LiveAnnouncementSchema.nullable()" in schema
    assert "async getActiveLiveAnnouncement(id: string)" in service
    assert "/live-announcements/active`" in service
    assert "LiveAnnouncementResponseSchema.parse" in service


def test_public_board_polls_announcements_independently_and_renders_both_modes():
    source = PAGE.read_text(encoding="utf-8")

    assert "const LIVE_ANNOUNCEMENT_POLL_INTERVAL_MS = 2_500" in source
    assert "queryKey: ['competition-live-announcement', id]" in source
    assert "competitionService.getActiveLiveAnnouncement(id!)" in source
    assert "refetchInterval: LIVE_ANNOUNCEMENT_POLL_INTERVAL_MS" in source
    assert "announcement?.display_mode === 'fullscreen'" in source
    assert 'data-live-layout="announcement-fullscreen"' in source
    assert source.index("announcement?.display_mode === 'fullscreen'") < source.index(
        "competitionQuery.isLoading || programQuery.isLoading || liveHeatQuery.isLoading"
    )
    assert "announcement?.display_mode === 'ticker'" in source
    assert 'data-live-announcement="ticker"' in source


def test_fullscreen_announcement_auto_fits_without_internal_scrolling():
    source = PAGE.read_text(encoding="utf-8")
    fullscreen = source.split('data-live-layout="announcement-fullscreen"', 1)[1].split(
        "competitionQuery.isLoading", 1
    )[0]

    assert "useLayoutEffect" in source and "ResizeObserver" in source
    assert "document.fonts.ready" in source
    assert "scrollHeight <= frame.clientHeight" in source
    assert "scrollWidth <= frame.clientWidth" in source
    assert "overflow-y-auto" not in fullscreen
    assert "lg:text-6xl" not in fullscreen
    assert "[overflow-wrap:anywhere]" in source


def test_ticker_is_accessible_fixed_and_reserves_board_space():
    source = PAGE.read_text(encoding="utf-8")
    styles = STYLES.read_text(encoding="utf-8")
    ticker = source.split('data-live-announcement="ticker"', 1)[1]

    assert 'role="status"' in source
    assert 'aria-live="polite"' in source
    assert "fixed inset-x-0 bottom-0" in source
    assert "data-live-has-ticker={tickerAnnouncement ? 'true' : undefined}" in source
    assert "announcementQuery.refetch()" in source
    assert "const announcement = announcementQuery.isError ? null" in source
    assert "tickerAnnouncement.message.toUpperCase()" in source
    assert "Math.max(5, Math.ceil(100 / Math.max(tickerText.length, 1)))" in source
    assert "Math.max(15, tickerTotalChars * 0.225)" in source
    assert "[0, 1].map" in ticker and 'aria-hidden="true"' in ticker
    assert "truncate" not in ticker and "text-overflow" not in styles
    assert "@keyframes live-announcement-ticker" in styles
    assert "transform: translateX(-50%)" in styles
    assert "linear infinite" in styles
    assert "@media (prefers-reduced-motion: reduce)" in styles
    assert ".live-announcement-ticker-copy[aria-hidden='true']" in styles


def test_announcement_admin_route_is_standalone_and_separate_from_operator_control():
    router = ROUTER.read_text(encoding="utf-8")

    assert "path: '/competitions/:id/live/admin'" in router
    assert "<CompetitionLiveAnnouncementAdminPage />" in router
    assert router.index("path: '/competitions/:id/live/admin'") < router.index(
        "element: <MainLayout />"
    )
    assert "path: '/competitions/:id/live/control'" in router


def test_admin_login_exchanges_ephemeral_supabase_token_for_http_only_session():
    service = SERVICE.read_text(encoding="utf-8")
    auth_schema = AUTH_SCHEMA.read_text(encoding="utf-8")

    assert "VITE_SUPABASE_URL" in service and "VITE_SUPABASE_ANON_KEY" in service
    assert "/auth/v1/token?grant_type=password" in service
    assert "SupabasePasswordResponseSchema.parse" in service
    assert "Authorization: `Bearer ${ephemeral.accessToken}`" in service
    assert "credentials: 'include'" in service
    assert "ephemeral.accessToken = ''" in service
    assert "localStorage" not in service and "sessionStorage" not in service
    assert "AdminSessionResponseSchema" in auth_schema
    env_example = FRONTEND_ENV.read_text(encoding="utf-8")
    assert "VITE_SUPABASE_URL=" in env_example
    assert "VITE_SUPABASE_ANON_KEY=" in env_example


def test_admin_foundation_probes_access_handles_errors_and_logs_out():
    service = SERVICE.read_text(encoding="utf-8")
    source = ADMIN_PAGE.read_text(encoding="utf-8")

    assert "getLiveAnnouncements" in service and "/live-announcements`" in service
    assert "deleteLiveAnnouncementAdminSession" in service
    assert "queryKey: ['competition-live-announcements-admin', id]" in source
    assert 'type="email"' in source and 'type="password"' in source
    assert "createLiveAnnouncementAdminSession(email, password)" in source
    assert "setPassword('')" in source
    assert "Cerrar sesión" in source
    assert "Autenticación administrativa no configurada" in source
    assert "No tienes permisos para administrar esta competencia" in source
    assert "No pudimos conectar con la administración" in source
    assert "createLiveHeatSession" not in source
    assert "swimstats_live_operator" not in source
    assert "localStorage" not in source and "sessionStorage" not in source


def test_announcement_admin_service_mutations_are_typed_scoped_and_revisioned():
    service = SERVICE.read_text(encoding="utf-8")
    schema = SCHEMA.read_text(encoding="utf-8")

    for method in [
        "createLiveAnnouncement", "updateLiveAnnouncement",
        "setLiveAnnouncementActivation", "deleteLiveAnnouncement",
    ]:
        assert f"async {method}" in service
    assert "LiveAnnouncementResponseSchema.parse" in service
    assert "LiveAnnouncementCreate" in schema and "expected_revision: 0" in schema
    assert "LiveAnnouncementUpdate" in schema and "LiveAnnouncementActivation" in schema
    assert "expected_revision" in service
    assert "credentials: 'include'" in service
    assert "encodeURIComponent(id)" in service


def test_announcement_history_is_typed_admin_only_and_refreshed_after_mutations():
    schema = SCHEMA.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")
    source = ADMIN_PAGE.read_text(encoding="utf-8")
    public_source = PAGE.read_text(encoding="utf-8")

    assert "LiveAnnouncementEventSchema" in schema
    assert "LiveAnnouncementHistoryResponseSchema" in schema
    assert "async getLiveAnnouncementHistory(id: string, limit: number = 20)" in service
    assert "/live-announcements/history?limit=${limit}`" in service
    assert "credentials: 'include'" in service
    assert "queryKey: ['competition-live-announcement-history-admin', id]" in source
    assert source.count("historyQuery.refetch()") >= 3
    assert "Historial de comunicados" in source
    assert "event.actor_user_id" in source
    assert "getLiveAnnouncementHistory" not in public_source


def test_announcement_admin_page_provides_accessible_crud_and_status():
    source = ADMIN_PAGE.read_text(encoding="utf-8")

    assert 'htmlFor="announcement-message"' in source
    assert 'htmlFor="announcement-mode"' in source
    assert "createLiveAnnouncement(id!" in source
    assert "updateLiveAnnouncement(id!" in source
    assert "setLiveAnnouncementActivation(id!" in source
    assert "deleteLiveAnnouncement(id!" in source
    assert "announcement.revision" in source
    assert "announcement.is_active" in source
    assert "Pantalla completa" in source and "Cinta inferior" in source
    assert "Activar" in source and "Desactivar" in source
    assert "Editar" in source and "Eliminar" in source


def test_announcement_admin_recovers_from_stale_mutations():
    source = ADMIN_PAGE.read_text(encoding="utf-8")

    assert "error instanceof ApiError && error.status === 409" in source
    assert "await announcementsQuery.refetch()" in source
    assert "Otro administrador actualizó los comunicados" in source
    assert "expected_revision: announcement.revision" in source
    assert "createLiveHeatSession" not in source
    assert "swimstats_live_operator" not in source
    assert "setMessage(successMessage);\n      void announcementsQuery.refetch();" in source
    assert "draftMode === 'ticker' ? 240 : 1000" in source


def test_live_branding_service_is_typed_revisioned_and_never_persists_images_locally():
    schema = SCHEMA.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")

    assert "LiveBrandingResponseSchema" in schema and "has_logo: z.boolean()" in schema
    assert "async getLiveBranding(id: string)" in service
    assert "async uploadLiveBranding(id: string, file: File, expectedRevision: number)" in service
    assert "async deleteLiveBranding(id: string, expectedRevision: number)" in service
    assert "body: file" in service and "expected_revision=${expectedRevision}" in service
    assert "localStorage" not in service and "data:" not in service


def test_public_board_polls_branding_and_replaces_only_tournament_content():
    source = PAGE.read_text(encoding="utf-8")

    # El logo no cambia durante la competencia: se consulta mucho mas lento que
    # el heat y los comunicados, que si cambian en vivo.
    assert "const LIVE_BRANDING_POLL_INTERVAL_MS = 60_000" in source
    assert "queryKey: ['competition-live-branding', id]" in source
    assert "refetchInterval: LIVE_BRANDING_POLL_INTERVAL_MS" in source
    assert "getLiveBrandingLogoUrl(id!, branding.revision)" in source
    assert "logoFailedRevision !== branding.revision" in source
    assert 'alt={`Logo de ${competition.name}`}' in source
    tournament = source.split('data-live-section="tournament"', 1)[1].split('</header>', 1)[0]
    # El logo se acota en mobile para no desplazar a los nadadores llamados.
    assert 'className="max-h-24 max-w-full object-contain sm:max-h-32 lg:max-h-full"' in tournament
    # La marca dejo de vivir dentro de la tarjeta del torneo: es un pie propio,
    # al final del board y solo en mobile.
    assert "SwimStats.cl" not in tournament and "Ver sembrado" not in tournament
    footer = source.split('data-live-section="board-footer"', 1)[1].split('</footer>', 1)[0]
    assert "SwimStats.cl" in footer and "Ver sembrado" in footer
    assert "lg:hidden" in footer and "shrink-0" in footer


def test_admin_page_validates_previews_and_recovers_live_branding_mutations():
    source = ADMIN_PAGE.read_text(encoding="utf-8")

    assert 'accept="image/png,image/jpeg,image/webp"' in source
    assert "file.size > 2 * 1024 * 1024" in source
    assert "URL.createObjectURL" in source and "URL.revokeObjectURL" in source
    assert "uploadLiveBranding(id!, selectedLogo!, branding.revision)" in source
    assert "deleteLiveBranding(id!, branding.revision)" in source
    assert "await brandingQuery.refetch()" in source
    assert "Otro administrador actualizó el logo" in source
    assert "localStorage" not in source and "FileReader" not in source
