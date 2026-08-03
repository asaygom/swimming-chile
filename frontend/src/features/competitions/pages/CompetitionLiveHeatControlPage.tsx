import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { PasswordField } from '../../../components/ui/PasswordField';
import { ApiError } from '../../../lib/api/fetcher';
import type { LiveHeatUpdate, MeetProgramSession } from '../../../lib/schemas/competition';
import { competitionService } from '../api/competitionService';

type HeatOption = Omit<LiveHeatUpdate, 'status' | 'expected_revision'> & {
  event_name: string;
  heat_total: number | null;
};

const LIVE_CONTROL_POLL_INTERVAL_MS = 2_500;

const heatKey = (heat: HeatOption) => [heat.publication_id, heat.stage_number, heat.pool_role, heat.session_number, heat.event_number, heat.heat_number].join(':');
const eventKey = (heat: HeatOption) => [heat.publication_id, heat.stage_number, heat.pool_role, heat.session_number, heat.event_number].join(':');
const stateVersionKey = (state: { publication_id: number; stage_number: number; pool_role: string; revision: number }) => [state.publication_id, state.stage_number, state.pool_role, state.revision].join(':');

const flattenHeats = (sessions: MeetProgramSession[]): HeatOption[] => sessions.flatMap((session) =>
  session.events.flatMap((event) => event.heats.map((heat) => ({
    publication_id: session.publication_id,
    stage_number: session.stage_number,
    pool_role: session.pool_role,
    session_number: session.session_number,
    event_number: event.event_number,
    event_name: event.event_name,
    heat_number: heat.heat_number,
    heat_total: heat.heat_total,
  }))),
);

export const CompetitionLiveHeatControlPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const [code, setCode] = useState('');
  const [authenticated, setAuthenticated] = useState(false);
  const [authError, setAuthError] = useState('');
  const [authenticating, setAuthenticating] = useState(false);
  const [message, setMessage] = useState('');
  const [selectedKey, setSelectedKey] = useState('');
  const [saving, setSaving] = useState(false);
  const [autoPublishFailed, setAutoPublishFailed] = useState(false);
  const updateInFlightRef = useRef(false);
  const autoPublishedHeatKeyRef = useRef('');
  const observedStateVersionRef = useRef('');
  const locallyPublishedStateVersionRef = useRef('');

  const competitionQuery = useQuery({ queryKey: ['competition', id], queryFn: () => competitionService.getCompetitionDetail(id!), enabled: Boolean(id) && authenticated });
  const programQuery = useQuery({ queryKey: ['competition-meet-program', id], queryFn: () => competitionService.getMeetProgram(id!), enabled: Boolean(id) && authenticated });
  const liveQuery = useQuery({
    queryKey: ['competition-live-heat', id],
    queryFn: () => competitionService.getLiveHeat(id!),
    enabled: Boolean(id) && authenticated,
    refetchInterval: LIVE_CONTROL_POLL_INTERVAL_MS,
  });
  const historyQuery = useQuery({
    queryKey: ['competition-live-heat-history', id],
    queryFn: () => competitionService.getLiveHeatHistory(id!),
    enabled: Boolean(id) && authenticated,
    refetchInterval: LIVE_CONTROL_POLL_INTERVAL_MS,
  });

  const heats = useMemo(() => flattenHeats(programQuery.data?.sessions ?? []), [programQuery.data]);
  const liveState = liveQuery.data?.state;
  const defaultSelectedKey = useMemo(() => {
    const current = liveState && heats.find((heat) =>
      heat.publication_id === liveState.publication_id
      && heat.stage_number === liveState.stage_number
      && heat.pool_role === liveState.pool_role
      && heat.session_number === liveState.session_number
      && heat.event_number === liveState.event_number
      && heat.heat_number === liveState.heat_number);
    return heats.length ? heatKey(current ?? heats[0]) : '';
  }, [heats, liveState]);
  const effectiveSelectedKey = selectedKey || defaultSelectedKey;
  const selectedIndex = heats.findIndex((heat) => heatKey(heat) === effectiveSelectedKey);
  const selected = heats[selectedIndex];
  const events = useMemo(() => heats.filter((heat, index) => heats.findIndex((candidate) => eventKey(candidate) === eventKey(heat)) === index), [heats]);
  const eventHeats = selected ? heats.filter((heat) => eventKey(heat) === eventKey(selected)) : [];

  const authenticate = async (event: React.FormEvent) => {
    event.preventDefault();
    setAuthError('');
    setAuthenticating(true);
    try {
      await competitionService.createLiveHeatSession(id!, code);
      setCode('');
      setAuthenticated(true);
    } catch (error) {
      setCode('');
      setAuthError(error instanceof ApiError && error.status === 429
        ? 'Demasiados intentos. Espera antes de volver a probar.'
        : 'El código temporal no es válido o el control no está configurado.');
    } finally {
      setAuthenticating(false);
    }
  };

  const update = useCallback(async (target: HeatOption) => {
    if (updateInFlightRef.current) return;
    updateInFlightRef.current = true;
    const autoPublish = !liveState && autoPublishedHeatKeyRef.current === heatKey(target);
    if (autoPublish) setAutoPublishFailed(false);
    setSaving(true);
    setMessage('');
    const expectedRevision = liveState
      && target.stage_number === liveState.stage_number
      && target.pool_role === liveState.pool_role
      ? liveState.revision : 0;
    try {
      const updated = await competitionService.updateLiveHeat(id!, {
        publication_id: target.publication_id,
        stage_number: target.stage_number,
        pool_role: target.pool_role,
        session_number: target.session_number,
        event_number: target.event_number,
        heat_number: target.heat_number,
        status: 'active',
        expected_revision: expectedRevision,
      });
      locallyPublishedStateVersionRef.current = stateVersionKey(updated.state);
      setSelectedKey(heatKey(target));
      setMessage('Llamador actualizado correctamente.');
    } catch (error) {
      if (autoPublish) { autoPublishedHeatKeyRef.current = ''; setAutoPublishFailed(true); }
      if (error instanceof ApiError && error.status === 409) {
        setSelectedKey('');
        setMessage('Otro voluntario actualizó el llamador. Revisa el estado y confirma nuevamente.');
      } else if (error instanceof ApiError && error.status === 401) {
        setAuthenticated(false);
        setAuthError('La sesión expiró. Ingresa nuevamente el código temporal.');
      } else {
        setMessage('No pudimos guardar el cambio. Intenta nuevamente.');
      }
    } finally {
      try {
        await liveQuery.refetch();
        await historyQuery.refetch();
      } finally {
        updateInFlightRef.current = false;
        setSaving(false);
      }
    }
  }, [historyQuery, id, liveQuery, liveState]);

  useEffect(() => {
    if (!authenticated || !liveState || saving || updateInFlightRef.current) return;
    const version = stateVersionKey(liveState);
    if (!observedStateVersionRef.current) {
      observedStateVersionRef.current = version;
      return;
    }
    if (observedStateVersionRef.current === version) return;
    observedStateVersionRef.current = version;
    setSelectedKey('');
    if (locallyPublishedStateVersionRef.current === version) {
      locallyPublishedStateVersionRef.current = '';
      return;
    }
    setMessage('Otro voluntario actualiz\u00f3 el llamador. Mostramos el estado vigente.');
  }, [authenticated, liveState, saving]);

  const logout = async () => {
    setMessage('');
    try {
      await competitionService.deleteLiveHeatSession(id!);
      observedStateVersionRef.current = '';
      locallyPublishedStateVersionRef.current = '';
      setSelectedKey('');
      setAuthenticated(false);
    } catch {
      setMessage('No pudimos cerrar la sesi\u00f3n. Intenta nuevamente.');
    }
  };

  useEffect(() => {
    if (!authenticated || liveQuery.isLoading || liveQuery.isError || liveState || !heats.length || autoPublishFailed) return;
    const firstHeatKey = heatKey(heats[0]);
    if (autoPublishedHeatKeyRef.current === firstHeatKey) return;
    autoPublishedHeatKeyRef.current = firstHeatKey;
    void update(heats[0]);
  }, [authenticated, autoPublishFailed, heats, liveQuery.isError, liveQuery.isLoading, liveState, update]);

  if (!authenticated) {
    return (
      <main data-live-layout="heat-controller" className="grid min-h-dvh place-items-center bg-slate-100 p-4 font-sans">
        <section className="w-full max-w-md overflow-hidden rounded-3xl bg-white shadow-xl">
          <div className="bg-brand-live p-7 text-white">
            <div className="flex items-center gap-3"><div><p className="text-xs font-black uppercase tracking-widest text-white/80">{competitionQuery.data?.competition.name}</p><h1 className="text-2xl font-black">Controlador de heats</h1></div></div>
          </div>
          <form onSubmit={authenticate} className="space-y-4 p-7">
            <p className="text-sm text-slate-500">Ingresa el código temporal asignado a esta competencia.</p>
            <PasswordField
              id="operator-code" label="Código temporal" autoComplete="one-time-code" required
              value={code} onChange={setCode}
              inputClassName="bg-slate-50 text-slate-800 focus:ring-2 focus:ring-brand-live"
            />
            {authError && <p role="alert" className="text-sm font-semibold text-danger">{authError}</p>}
            <button disabled={authenticating} className="w-full rounded-xl bg-brand-live px-4 py-3 font-black text-white disabled:opacity-50" type="submit">{authenticating ? 'Validando…' : 'Ingresar al control'}</button>
            <Link className="block text-center text-sm font-bold text-brand-live hover:underline" to={`/competitions/${id}/live`}>Ver pantalla pública</Link>
          </form>
        </section>
      </main>
    );
  }

  if (competitionQuery.isLoading || programQuery.isLoading || liveQuery.isLoading) return <main className="grid min-h-dvh place-items-center bg-slate-100">Cargando controlador…</main>;
  if (competitionQuery.isError || programQuery.isError || liveQuery.isError || !selected) return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 text-center">No pudimos cargar el programa y el estado del llamador.</main>;

  return (
    // El controlador se bloquea al viewport: el voluntario opera de pie y con
    // prisa, asi que los botones de avance no pueden quedar detras de un scroll.
    <main data-live-layout="heat-controller" className="flex h-dvh flex-col overflow-hidden bg-slate-100 font-sans text-slate-800">
      <div className="mx-auto flex h-full w-full min-h-0 max-w-4xl flex-col gap-3 p-3 [@media(max-height:500px)]:gap-2 [@media(max-height:500px)]:p-2 sm:gap-5 sm:p-6">
        <header className="flex shrink-0 items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm [@media(max-height:500px)]:p-2">
          <div className="min-w-0"><p className="text-xs font-black uppercase tracking-widest text-brand-live [@media(max-height:500px)]:hidden">Controlador de heats</p><h1 className="truncate text-lg font-black [@media(max-height:500px)]:text-sm">{competitionQuery.data?.competition.name}</h1></div>
          <nav className="flex shrink-0 items-center gap-3 text-xs font-bold"><Link className="text-brand-live" to={`/competitions/${id}/live`}>Pantalla pública</Link><button type="button" disabled={saving} onClick={() => { void logout(); }} className="text-slate-600 disabled:opacity-40">Cerrar sesión</button></nav>
        </header>

        {/* En horizontal la altura es el recurso escaso: los dos selectores se
            reparten en columnas para no empujar al heat y a los botones. */}
        <section className="grid shrink-0 gap-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm [@media(max-height:500px)]:grid-cols-2 [@media(max-height:500px)]:gap-3 [@media(max-height:500px)]:p-3 sm:p-5">
          <div>
            <label htmlFor="event-selector" className="mb-1 block text-xs font-black uppercase tracking-wider text-slate-500">Selección de evento</label>
            <select id="event-selector" value={eventKey(selected)} disabled={saving} onChange={(event) => { const first = heats.find((heat) => eventKey(heat) === event.target.value); if (first) void update(first); }} className="w-full rounded-xl border border-slate-300 bg-slate-50 px-4 py-3 text-base font-bold disabled:opacity-50">
              {events.map((event) => <option key={eventKey(event)} value={eventKey(event)}>Evento #{event.event_number} — {event.event_name}</option>)}
            </select>
          </div>
          <div className="flex items-center justify-between gap-4 border-t border-slate-100 pt-4 [@media(max-height:500px)]:flex-col [@media(max-height:500px)]:items-stretch [@media(max-height:500px)]:gap-1 [@media(max-height:500px)]:border-t-0 [@media(max-height:500px)]:pt-0">
            <label htmlFor="heat-selector" className="text-xs font-black uppercase tracking-wider text-slate-500">Ir al heat</label>
            <select id="heat-selector" value={heatKey(selected)} disabled={saving} onChange={(event) => { const target = heats.find((heat) => heatKey(heat) === event.target.value); if (target) void update(target); }} className="rounded-xl border border-slate-300 bg-slate-50 px-4 py-2 font-bold disabled:opacity-50 [@media(max-height:500px)]:w-full">
              {eventHeats.map((heat) => <option key={heatKey(heat)} value={heatKey(heat)}>Heat {heat.heat_number} de {heat.heat_total ?? eventHeats.length}</option>)}
            </select>
          </div>
        </section>

        {/* Region flexible: aqui vive todo lo que puede crecer. Si excede el
            alto disponible scrollea solo esta zona, de modo que los botones de
            avance nunca se desplazan fuera de la pantalla. */}
        <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto sm:gap-5">
        <section className="flex min-h-40 flex-1 flex-col justify-between overflow-hidden rounded-2xl bg-brand-live p-5 text-white shadow-md [@media(max-height:500px)]:min-h-24 [@media(max-height:500px)]:p-3 sm:min-h-48 sm:p-7">
          <div className="flex items-center justify-between gap-3"><span className="rounded-lg bg-white/15 px-3 py-1 text-xs font-black uppercase tracking-widest">Evento #{selected.event_number}</span><span className="flex items-center gap-2 rounded-lg border border-emerald-300/40 bg-emerald-400/20 px-3 py-1 text-xs font-black uppercase"><span className="h-2 w-2 rounded-full bg-emerald-300" />Heat llamado</span></div>
          <div><p className="mb-2 font-semibold text-cyan-100">{selected.event_name}</p><div className="flex items-baseline justify-between gap-4"><h2 className="text-5xl font-black italic tracking-tight [@media(max-height:500px)]:text-3xl sm:text-6xl">HEAT {String(selected.heat_number).padStart(2, '0')}</h2><span className="text-xl font-bold text-cyan-100">de {selected.heat_total ?? eventHeats.length}</span></div><p className="mt-3 text-xs font-bold text-cyan-100">Etapa {selected.stage_number} · Piscina {selected.pool_role}</p></div>
        </section>

        <div aria-live="polite" className="min-h-5 shrink-0 text-center text-sm font-semibold text-slate-600">{message}</div>
        {autoPublishFailed && !liveState && <button type="button" disabled={saving} onClick={() => { autoPublishedHeatKeyRef.current = heatKey(heats[0]); void update(heats[0]); }} className="shrink-0 rounded-xl bg-slate-800 px-4 py-3 font-black text-white disabled:opacity-40">Reintentar</button>}
        {/* El historial crece durante la jornada, asi que nace colapsado: es
            material de consulta, no la accion principal del voluntario. */}
        <details className="shrink-0 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <summary className="text-xs font-black uppercase tracking-wider text-slate-500">Movimientos recientes</summary>
          {historyQuery.isError ? <p className="mt-2 text-sm text-slate-500">No pudimos actualizar el historial.</p> : (
            <ol className="mt-2 space-y-1 text-sm">
              {(historyQuery.data?.movements ?? []).map((movement) => (
                <li key={movement.id} className="flex flex-wrap items-center justify-between gap-x-3 rounded-lg bg-slate-50 px-3 py-2">
                  <span className="font-bold text-slate-700">{movement.is_current_session ? 'Esta sesi\u00f3n' : 'Otra sesi\u00f3n'}</span>
                  <span className="text-slate-600">{movement.previous_event_number ? `E${movement.previous_event_number} / H${movement.previous_heat_number} -> ` : 'Inicio -> '}E{movement.resulting_event_number} / H{movement.resulting_heat_number}</span>
                  <time className="text-xs text-slate-400" dateTime={movement.occurred_at}>{new Date(movement.occurred_at).toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit' })}</time>
                </li>
              ))}
              {historyQuery.data?.movements.length === 0 && <li className="py-2 text-slate-400">{'A\u00fan no hay movimientos.'}</li>}
            </ol>
          )}
        </details>
        </div>
        <div className="grid shrink-0 grid-cols-2 gap-3">
          <button type="button" disabled={saving || !liveState || selectedIndex <= 0} onClick={() => { void update(heats[selectedIndex - 1]); }} className="rounded-2xl border border-slate-300 bg-white px-5 py-4 text-xl font-black disabled:opacity-40 [@media(max-height:500px)]:py-2 [@media(max-height:500px)]:text-base">← Anterior</button>
          <button type="button" disabled={saving || !liveState || selectedIndex >= heats.length - 1} onClick={() => { void update(heats[selectedIndex + 1]); }} className="rounded-2xl bg-brand-live px-5 py-4 text-xl font-black text-white disabled:opacity-40 [@media(max-height:500px)]:py-2 [@media(max-height:500px)]:text-base">Siguiente →</button>
        </div>
      </div>
    </main>
  );
};
