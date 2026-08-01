import React, { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { ApiError } from '../../../lib/api/fetcher';
import type { LiveHeatUpdate, MeetProgramSession } from '../../../lib/schemas/competition';
import { competitionService } from '../api/competitionService';

type HeatOption = Omit<LiveHeatUpdate, 'status' | 'expected_revision'> & {
  event_name: string;
  heat_total: number | null;
};

const heatKey = (heat: HeatOption) => [heat.publication_id, heat.stage_number, heat.pool_role, heat.session_number, heat.event_number, heat.heat_number].join(':');
const eventKey = (heat: HeatOption) => [heat.publication_id, heat.stage_number, heat.pool_role, heat.session_number, heat.event_number].join(':');

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

const statusLabels = {
  not_started: 'Por comenzar',
  active: 'En curso',
  paused: 'Pausado',
  finished: 'Finalizado',
} as const;

export const CompetitionLiveHeatControlPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const [code, setCode] = useState('');
  const [authenticated, setAuthenticated] = useState(false);
  const [authError, setAuthError] = useState('');
  const [authenticating, setAuthenticating] = useState(false);
  const [message, setMessage] = useState('');
  const [selectedKey, setSelectedKey] = useState('');
  const [saving, setSaving] = useState(false);

  const competitionQuery = useQuery({ queryKey: ['competition', id], queryFn: () => competitionService.getCompetitionDetail(id!), enabled: Boolean(id) && authenticated });
  const programQuery = useQuery({ queryKey: ['competition-meet-program', id], queryFn: () => competitionService.getMeetProgram(id!), enabled: Boolean(id) && authenticated });
  const liveQuery = useQuery({ queryKey: ['competition-live-heat', id], queryFn: () => competitionService.getLiveHeat(id!), enabled: Boolean(id) && authenticated });

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

  const update = async (target: HeatOption, status: LiveHeatUpdate['status']) => {
    setSaving(true);
    setMessage('');
    const expectedRevision = liveState
      && target.stage_number === liveState.stage_number
      && target.pool_role === liveState.pool_role
      ? liveState.revision : 0;
    try {
      await competitionService.updateLiveHeat(id!, {
        publication_id: target.publication_id,
        stage_number: target.stage_number,
        pool_role: target.pool_role,
        session_number: target.session_number,
        event_number: target.event_number,
        heat_number: target.heat_number,
        status,
        expected_revision: expectedRevision,
      });
      setSelectedKey(heatKey(target));
      await liveQuery.refetch();
      setMessage('Llamador actualizado correctamente.');
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        setSelectedKey('');
        await liveQuery.refetch();
        setMessage('Otro voluntario actualizó el llamador. Revisa el estado y confirma nuevamente.');
      } else if (error instanceof ApiError && error.status === 401) {
        setAuthenticated(false);
        setAuthError('La sesión expiró. Ingresa nuevamente el código temporal.');
      } else {
        setMessage('No pudimos guardar el cambio. Intenta nuevamente.');
      }
    } finally {
      setSaving(false);
    }
  };

  if (!authenticated) {
    return (
      <main data-live-layout="heat-controller" className="grid min-h-dvh place-items-center bg-slate-100 p-4 font-sans">
        <section className="w-full max-w-md overflow-hidden rounded-3xl bg-white shadow-xl">
          <div className="bg-brand-pool p-7 text-white">
            <div className="flex items-center gap-3"><img src="/web-app-manifest-192x192.png" alt="" className="h-12 w-12 rounded-xl" /><div><p className="text-xs font-black uppercase tracking-widest text-white/80">SwimStats Chile</p><h1 className="text-2xl font-black">Controlador de heats</h1></div></div>
          </div>
          <form onSubmit={authenticate} className="space-y-4 p-7">
            <p className="text-sm text-slate-500">Ingresa el código temporal asignado a esta competencia.</p>
            <label className="block text-sm font-bold text-slate-700" htmlFor="operator-code">Código temporal</label>
            <input id="operator-code" type="password" autoComplete="one-time-code" required value={code} onChange={(event) => setCode(event.target.value)} className="w-full rounded-xl border border-slate-300 bg-slate-50 px-4 py-3 text-slate-800 focus:ring-2 focus:ring-brand-pool" />
            {authError && <p role="alert" className="text-sm font-semibold text-danger">{authError}</p>}
            <button disabled={authenticating} className="w-full rounded-xl bg-brand-pool px-4 py-3 font-black text-white disabled:opacity-50" type="submit">{authenticating ? 'Validando…' : 'Ingresar al control'}</button>
            <Link className="block text-center text-sm font-bold text-brand-pool hover:underline" to={`/competitions/${id}/live`}>Ver pantalla pública</Link>
          </form>
        </section>
      </main>
    );
  }

  if (competitionQuery.isLoading || programQuery.isLoading || liveQuery.isLoading) return <main className="grid min-h-dvh place-items-center bg-slate-100">Cargando controlador…</main>;
  if (competitionQuery.isError || programQuery.isError || liveQuery.isError || !selected) return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 text-center">No pudimos cargar el programa y el estado del llamador.</main>;

  return (
    <main data-live-layout="heat-controller" className="min-h-dvh bg-slate-100 font-sans text-slate-800">
      <div className="mx-auto flex min-h-dvh max-w-4xl flex-col gap-3 p-3 sm:gap-5 sm:p-6">
        <header className="flex items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="min-w-0"><p className="text-xs font-black uppercase tracking-widest text-brand-pool">Controlador de heats</p><h1 className="truncate text-lg font-black">{competitionQuery.data?.competition.name}</h1></div>
          <nav className="flex shrink-0 gap-3 text-xs font-bold"><Link className="text-brand-pool" to={`/competitions/${id}/live`}>Pantalla pública</Link><Link className="text-slate-500" to={`/competitions/${id}?tab=series`}>Sembrado</Link></nav>
        </header>

        <section className="space-y-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
          <div>
            <label htmlFor="event-selector" className="mb-1 block text-xs font-black uppercase tracking-wider text-slate-500">Selección de evento</label>
            <select id="event-selector" value={eventKey(selected)} onChange={(event) => { const first = heats.find((heat) => eventKey(heat) === event.target.value); if (first) setSelectedKey(heatKey(first)); }} className="w-full rounded-xl border border-slate-300 bg-slate-50 px-4 py-3 text-base font-bold">
              {events.map((event) => <option key={eventKey(event)} value={eventKey(event)}>Evento #{event.event_number} — {event.event_name}</option>)}
            </select>
          </div>
          <div className="flex items-center justify-between gap-4 border-t border-slate-100 pt-4">
            <label htmlFor="heat-selector" className="text-xs font-black uppercase tracking-wider text-slate-500">Ir al heat</label>
            <select id="heat-selector" value={heatKey(selected)} onChange={(event) => setSelectedKey(event.target.value)} className="rounded-xl border border-slate-300 bg-slate-50 px-4 py-2 font-bold">
              {eventHeats.map((heat) => <option key={heatKey(heat)} value={heatKey(heat)}>Heat {heat.heat_number} de {heat.heat_total ?? eventHeats.length}</option>)}
            </select>
          </div>
        </section>

        <section className="flex min-h-48 flex-1 flex-col justify-between rounded-2xl bg-brand-pool p-5 text-white shadow-md sm:p-7">
          <div className="flex items-center justify-between gap-3"><span className="rounded-lg bg-white/15 px-3 py-1 text-xs font-black uppercase tracking-widest">Evento #{selected.event_number}</span><span className="flex items-center gap-2 rounded-lg border border-emerald-300/40 bg-emerald-400/20 px-3 py-1 text-xs font-black uppercase"><span className="h-2 w-2 rounded-full bg-emerald-300" />Heat llamado</span></div>
          <div><p className="mb-2 font-semibold text-cyan-100">{selected.event_name}</p><div className="flex items-baseline justify-between gap-4"><h2 className="text-5xl font-black italic tracking-tight sm:text-6xl">HEAT {String(selected.heat_number).padStart(2, '0')}</h2><span className="text-xl font-bold text-cyan-100">de {selected.heat_total ?? eventHeats.length}</span></div><p className="mt-3 text-xs font-bold text-cyan-100">Etapa {selected.stage_number} · Piscina {selected.pool_role}</p></div>
        </section>

        <div aria-live="polite" className="min-h-5 text-center text-sm font-semibold text-slate-600">{message}</div>
        <div className="grid gap-3 sm:grid-cols-2">
          <button type="button" disabled={saving || !liveState || selectedIndex <= 0} onClick={() => update(heats[selectedIndex - 1], liveState!.status)} className="rounded-2xl border border-slate-300 bg-white px-5 py-4 text-xl font-black disabled:opacity-40">← Anterior</button>
          <button type="button" disabled={saving || !liveState || selectedIndex >= heats.length - 1} onClick={() => update(heats[selectedIndex + 1], liveState!.status)} className="rounded-2xl bg-brand-pool px-5 py-4 text-xl font-black text-white disabled:opacity-40">Siguiente →</button>
        </div>
        <div className="grid gap-3 rounded-2xl bg-white p-4 sm:grid-cols-[1.4fr_2fr]">
          <button type="button" disabled={saving} onClick={() => update(selected, liveState?.status ?? 'not_started')} className="rounded-xl bg-slate-800 px-4 py-3 font-black text-white disabled:opacity-40">{liveState ? 'Aplicar selección' : 'Inicializar llamador'}</button>
          <fieldset disabled={saving || !liveState} className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            <legend className="sr-only">Estado del heat</legend>
            {Object.entries(statusLabels).map(([status, label]) => <button key={status} type="button" onClick={() => update(selected, status as LiveHeatUpdate['status'])} aria-pressed={liveState?.status === status} className="rounded-lg border border-slate-200 px-2 py-3 text-sm font-bold aria-pressed:border-brand-pool aria-pressed:bg-brand-pool aria-pressed:text-white disabled:opacity-40">{label}</button>)}
          </fieldset>
        </div>
      </div>
    </main>
  );
};
