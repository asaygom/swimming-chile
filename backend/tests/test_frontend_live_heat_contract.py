from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROUTER = ROOT / "frontend/src/app/router.tsx"
SERVICE = ROOT / "frontend/src/features/competitions/api/competitionService.ts"
SCHEMA = ROOT / "frontend/src/lib/schemas/competition.ts"
PAGE = ROOT / "frontend/src/features/competitions/pages/CompetitionLiveHeatPage.tsx"
CONTROL_PAGE = ROOT / "frontend/src/features/competitions/pages/CompetitionLiveHeatControlPage.tsx"


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
    assert "const LIVE_HEAT_POLL_INTERVAL_MS = 10_000" in source
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
    assert "PRÓXIMO HEAT" in source
    assert "HEAT" in source
    assert "Pista" in source


def test_public_live_heat_is_a_viewport_locked_responsive_tv_board():
    source = PAGE.read_text(encoding="utf-8")

    assert 'className="h-dvh max-h-dvh overflow-hidden' in source
    assert 'className="mx-auto flex h-full min-h-0' in source
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


def test_operator_update_response_schema_matches_put_contract_without_get_derivations():
    schema = SCHEMA.read_text(encoding="utf-8")
    update_schema = schema.split("export const LiveHeatUpdateStateSchema", 1)[1].split(
        "export type LiveHeatUpdate", 1
    )[0]

    assert "event_name" not in update_schema
    assert "heat_total" not in update_schema
    assert "state: LiveHeatUpdateStateSchema" in update_schema


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
