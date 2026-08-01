import React, { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import type { MeetProgramSession } from '../../../lib/schemas/competition';
import { competitionService } from '../api/competitionService';

const LIVE_HEAT_POLL_INTERVAL_MS = 10_000;

const statusLabels = {
  not_started: 'Por comenzar',
  active: 'En curso',
  paused: 'Pausado',
  finished: 'Finalizado',
} as const;

type ProgramHeat = {
  sessionNumber: number;
  eventNumber: number;
  eventName: string;
  heatNumber: number;
  heatTotal: number | null;
  entries: MeetProgramSession['events'][number]['heats'][number]['entries'];
};

const partitionHeats = (sessions: MeetProgramSession[], state: NonNullable<Awaited<ReturnType<typeof competitionService.getLiveHeat>>['state']>) => {
  const partition = sessions.filter((item) =>
    item.publication_id === state.publication_id
    && item.stage_number === state.stage_number
    && item.pool_role === state.pool_role);
  return partition.flatMap((session) => session.events.flatMap((event) =>
    event.heats.map((heat): ProgramHeat => ({
      sessionNumber: session.session_number,
      eventNumber: event.event_number,
      eventName: event.event_name,
      heatNumber: heat.heat_number,
      heatTotal: heat.heat_total,
      entries: heat.entries,
    }))));
};

export const CompetitionLiveHeatPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const competitionQuery = useQuery({
    queryKey: ['competition', id],
    queryFn: () => competitionService.getCompetitionDetail(id!),
    enabled: Boolean(id),
  });
  const programQuery = useQuery({
    queryKey: ['competition-meet-program', id],
    queryFn: () => competitionService.getMeetProgram(id!),
    enabled: Boolean(id),
  });
  const liveHeatQuery = useQuery({
    queryKey: ['competition-live-heat', id],
    queryFn: () => competitionService.getLiveHeat(id!),
    enabled: Boolean(id),
    refetchInterval: LIVE_HEAT_POLL_INTERVAL_MS,
  });

  const state = liveHeatQuery.data?.state;
  const nextHeat = useMemo(() => {
    if (!state) return null;
    const heats = partitionHeats(programQuery.data?.sessions ?? [], state);
    const currentIndex = heats.findIndex((heat) =>
      heat.sessionNumber === state.session_number
      && heat.eventNumber === state.event_number
      && heat.heatNumber === state.heat_number);
    return currentIndex >= 0 ? heats[currentIndex + 1] ?? null : null;
  }, [programQuery.data, state]);

  if (competitionQuery.isLoading || programQuery.isLoading || liveHeatQuery.isLoading) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 font-sans text-slate-600">Cargando llamador…</main>;
  }
  if (competitionQuery.isError || !competitionQuery.data) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 text-center font-sans text-slate-700">No pudimos cargar la competencia.</main>;
  }
  if ((programQuery.isError || liveHeatQuery.isError) && (!programQuery.data || !liveHeatQuery.data)) {
    return (
      <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 text-center font-sans text-slate-700">
        <div><p className="font-bold">No pudimos cargar el estado del llamador.</p><button type="button" onClick={() => { programQuery.refetch(); liveHeatQuery.refetch(); }} className="mt-4 rounded-xl bg-brand-pool px-4 py-3 font-bold text-white">Reintentar</button></div>
      </main>
    );
  }

  const competition = competitionQuery.data.competition;
  const entries = liveHeatQuery.data?.entries ?? [];

  return (
    <main data-live-layout="caller-board" className="min-h-dvh bg-slate-100 p-3 font-sans text-slate-800 sm:p-5">
      <div className="mx-auto flex min-h-[calc(100dvh-1.5rem)] max-w-[1920px] flex-col gap-3 sm:min-h-[calc(100dvh-2.5rem)] lg:flex-row lg:gap-5">
        <aside className="flex shrink-0 flex-col gap-3 lg:w-[340px] xl:w-[380px]">
          <section className="overflow-hidden rounded-3xl bg-white">
            <div className="bg-brand-pool px-6 py-5 text-white">
              <p className="text-xs font-extrabold uppercase tracking-[0.22em] text-white/80">Heat actual</p>
              <h1 className="mt-1 text-5xl font-black italic tracking-tight">HEAT {state ? String(state.heat_number).padStart(2, '0') : '--'}</h1>
              {state?.heat_total && <p className="mt-1 font-bold text-white/80">de {state.heat_total} heats</p>}
            </div>
            <div className="px-6 py-5">
              <p className="text-xs font-extrabold uppercase tracking-widest text-brand-pool">Evento {state?.event_number ?? '—'}</p>
              <h2 className="mt-2 text-2xl font-black leading-tight text-[#434343]">{state?.event_name ?? 'Llamador aún no iniciado'}</h2>
            </div>
          </section>

          <header className="flex flex-1 flex-col justify-between rounded-3xl bg-white p-6">
            <div className="flex items-start gap-3">
              <img src="/web-app-manifest-192x192.png" alt="" className="h-12 w-12 rounded-xl" />
              <div>
                <p className="text-xs font-black uppercase tracking-widest text-brand-pool">SwimStats Chile</p>
                <h2 className="mt-1 text-xl font-black leading-tight text-[#434343]">{competition.name}</h2>
                <p className="mt-1 text-sm font-medium text-slate-500">{competition.location || 'Sede por confirmar'}</p>
              </div>
            </div>
            <div className="mt-6 flex items-center justify-between gap-3 border-t border-slate-100 pt-4 text-xs font-bold">
              <span className="rounded-lg bg-slate-100 px-3 py-2 uppercase tracking-wider text-slate-600">{state ? statusLabels[state.status] : 'Sin iniciar'}</span>
              <Link to={`/competitions/${id}?tab=series`} className="text-brand-pool hover:underline">Ver sembrado</Link>
            </div>
          </header>
        </aside>

        <section aria-live="polite" aria-atomic="true" className="grid min-h-0 flex-1 gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(260px,0.48fr)]">
          {(programQuery.isError || liveHeatQuery.isError) && (
            <p role="status" className="rounded-xl border border-amber-300 bg-amber-50 px-4 py-3 text-sm font-semibold text-amber-900 lg:col-span-2">
              Datos potencialmente desactualizados. <button type="button" onClick={() => { programQuery.refetch(); liveHeatQuery.refetch(); }} className="font-black underline">Reintentar</button>
            </p>
          )}
          <div className="flex min-h-[55dvh] flex-col rounded-3xl bg-white p-3 sm:p-5">
            <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
              <h2 className="text-sm font-black uppercase tracking-widest text-[#434343]">Nadadores llamados</h2>
              <span className="text-xs font-bold text-slate-400">Etapa {state?.stage_number ?? '—'}</span>
            </div>
            {!state ? (
              <div className="grid flex-1 place-items-center p-8 text-center font-semibold text-slate-400">La pantalla se actualizará cuando el voluntario inicie el llamador.</div>
            ) : entries.length === 0 ? (
              <div className="grid flex-1 place-items-center p-8 text-center font-semibold text-slate-400">Este heat no tiene carriles asignados.</div>
            ) : (
              <ul className="flex flex-1 flex-col justify-evenly gap-2 py-2" aria-label="Asignaciones de carril">
                {entries.map((entry) => (
                  <li key={`${entry.lane}-${entry.display_name}`} className="flex min-h-14 items-center justify-between gap-4 rounded-xl bg-slate-50 px-4 py-2">
                    <div className="min-w-0">
                      <p className="truncate text-lg font-black text-[#434343] sm:text-xl 2xl:text-3xl">{entry.display_name}</p>
                      <p className="truncate text-xs font-bold text-slate-500 sm:text-sm">{entry.club_name || 'Club no informado'}</p>
                    </div>
                    <div className="shrink-0 text-brand-pool"><span className="mr-2 text-sm font-semibold">Pista</span><span className="text-3xl font-black sm:text-4xl">{entry.lane}</span></div>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <aside className="flex min-h-[35dvh] flex-col rounded-3xl bg-white p-3 sm:p-5">
            <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
              <h2 className="text-xs font-black uppercase tracking-widest text-[#434343]"><span className="mr-2 inline-block h-2 w-2 animate-pulse rounded-full bg-amber-500" />PRÓXIMO HEAT {nextHeat ? String(nextHeat.heatNumber).padStart(2, '0') : '—'}</h2>
            </div>
            {nextHeat ? (
              <>
                <p className="px-2 pt-3 text-xs font-bold text-brand-pool">Evento {nextHeat.eventNumber} · {nextHeat.eventName}</p>
                <ul className="flex flex-1 flex-col justify-evenly gap-1 py-2">
                  {nextHeat.entries.map((entry) => (
                    <li key={`${entry.lane}-${entry.display_name}`} className="flex items-center justify-between gap-2 rounded-xl bg-slate-50 px-3 py-2">
                      <span className="truncate font-bold text-[#434343]">{entry.display_name}</span>
                      <span className="max-w-28 truncate rounded bg-slate-200 px-2 py-1 text-[10px] font-bold text-slate-600">{entry.club_name || '--'}</span>
                    </li>
                  ))}
                </ul>
              </>
            ) : <div className="grid flex-1 place-items-center p-6 text-center text-sm font-semibold text-slate-400">Último heat alcanzado</div>}
          </aside>
        </section>
      </div>
    </main>
  );
};
