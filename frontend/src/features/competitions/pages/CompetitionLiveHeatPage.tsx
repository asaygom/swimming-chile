import React, { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import type { MeetProgramSession } from '../../../lib/schemas/competition';
import { competitionService } from '../api/competitionService';

const LIVE_HEAT_POLL_INTERVAL_MS = 10_000;
const LIVE_ANNOUNCEMENT_POLL_INTERVAL_MS = 10_000;

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
  const announcementQuery = useQuery({
    queryKey: ['competition-live-announcement', id],
    queryFn: () => competitionService.getActiveLiveAnnouncement(id!),
    enabled: Boolean(id),
    refetchInterval: LIVE_ANNOUNCEMENT_POLL_INTERVAL_MS,
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

  const announcement = announcementQuery.isError ? null : announcementQuery.data?.announcement;
  if (competitionQuery.data && announcement?.display_mode === 'fullscreen') {
    const competition = competitionQuery.data.competition;
    return (
      <main data-live-layout="announcement-fullscreen" aria-live="polite" aria-atomic="true" className="grid h-dvh max-h-dvh overflow-hidden bg-brand-pool p-6 font-sans text-white sm:p-10">
        <div className="mx-auto flex min-h-0 w-full max-w-6xl flex-col justify-between rounded-3xl border border-white/20 bg-white/10 p-6 shadow-2xl sm:p-10">
          <header className="flex items-center gap-3 border-b border-white/20 pb-5"><img src="/web-app-manifest-192x192.png" alt="" className="h-12 w-12 rounded-xl" /><div><p className="text-xs font-black uppercase tracking-[0.22em] text-white/75">Comunicado oficial</p><h1 className="text-xl font-black sm:text-2xl">{competition.name}</h1></div></header>
          <p className="my-6 overflow-y-auto text-center text-3xl font-black leading-tight sm:text-5xl lg:text-6xl">{announcement.message}</p>
          <footer className="flex items-center justify-between gap-4 border-t border-white/20 pt-5 text-sm font-bold text-white/75"><span>SwimStats Chile</span>{announcementQuery.isError && <button type="button" className="underline" onClick={() => announcementQuery.refetch()}>Actualizar comunicado</button>}</footer>
        </div>
      </main>
    );
  }

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
  const tickerAnnouncement = announcement?.display_mode === 'ticker' ? announcement : null;

  return (
    <main data-live-layout="caller-board" style={{ paddingBottom: tickerAnnouncement ? '4.5rem' : undefined }} className="h-dvh max-h-dvh overflow-hidden bg-slate-100 p-2 font-sans text-slate-800 sm:p-4">
      <div className="mx-auto flex h-full min-h-0 max-w-[1920px] flex-col gap-2 sm:gap-3 lg:flex-row lg:gap-5">
        <aside className="grid shrink-0 grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)] gap-2 sm:gap-3 [@media(max-height:700px)_and_(max-width:767px)]:grid-cols-3 lg:h-full lg:min-h-0 lg:w-[340px] lg:grid-cols-1 lg:grid-rows-[auto_auto_minmax(0,1fr)] xl:w-[380px]">
          <section data-live-section="heat" className="overflow-hidden rounded-2xl bg-brand-pool text-white sm:rounded-3xl">
            <div className="flex h-full flex-col justify-center px-4 py-3 sm:px-6 sm:py-4 lg:py-5">
              <p className="text-xs font-extrabold uppercase tracking-[0.22em] text-white/80 [@media(max-height:700px)_and_(max-width:767px)]:hidden">Heat actual</p>
              <h1 className="mt-1 text-3xl font-black italic leading-none tracking-tight sm:text-4xl lg:text-5xl">HEAT {state ? String(state.heat_number).padStart(2, '0') : '--'}</h1>
              {state?.heat_total && <p className="mt-1 font-bold text-white/80">de {state.heat_total} heats</p>}
            </div>
          </section>
          <section data-live-section="event" className="flex min-w-0 flex-col justify-center overflow-hidden rounded-2xl bg-white px-4 py-3 text-slate-800 sm:rounded-3xl sm:px-6 sm:py-4 lg:py-5">
            <p className="text-xs font-extrabold uppercase tracking-widest text-brand-pool">Evento {state?.event_number ?? '—'}</p>
            <h2 className="mt-1 line-clamp-2 text-lg font-black leading-tight text-[#434343] sm:text-xl [@media(max-height:700px)_and_(max-width:767px)]:line-clamp-1 lg:mt-2 lg:text-2xl">{state?.event_name ?? 'Llamador aún no iniciado'}</h2>
          </section>

          <header data-live-section="tournament" className="col-span-2 flex min-h-0 flex-col justify-between overflow-hidden rounded-2xl bg-white p-3 sm:rounded-3xl sm:p-4 [@media(max-height:700px)_and_(max-width:767px)]:col-span-1 lg:col-span-1 lg:p-6">
            <div className="flex items-start gap-3">
              <div className="min-w-0">
                <h2 className="mt-1 text-xl font-black leading-tight text-[#434343] [@media(max-height:700px)_and_(max-width:767px)]:truncate">{competition.name}</h2>
                <p className="mt-1 text-sm font-medium text-slate-500 [@media(max-height:700px)_and_(max-width:767px)]:hidden">{competition.location || 'Sede por confirmar'}</p>
              </div>
            </div>
            <div className="mt-2 flex items-center justify-between gap-3 border-t border-slate-100 pt-2 text-xs font-bold [@media(max-height:700px)_and_(max-width:767px)]:hidden lg:mt-6 lg:pt-4">
              <div className="flex items-center gap-2">
              <img
                src="/web-app-manifest-192x192.png"
                alt="SwimStats Chile"
                className="h-8 w-8"
              />
              <span className="bg-clip-text text-xl font-bold tracking-tight">
                SwimStats.cl
              </span>
            </div>
              <Link to={`/competitions/${id}?tab=series`} className="text-brand-pool hover:underline">Ver sembrado</Link>
            </div>
          </header>
        </aside>

        <section aria-live="polite" aria-atomic="true" className="flex min-h-0 flex-1 flex-col gap-2 sm:gap-3">
          {(programQuery.isError || liveHeatQuery.isError || announcementQuery.isError) && (
            <p role="status" className="shrink-0 rounded-xl border border-amber-300 bg-amber-50 px-4 py-2 text-sm font-semibold text-amber-900">
              Datos potencialmente desactualizados. <button type="button" onClick={() => { programQuery.refetch(); liveHeatQuery.refetch(); announcementQuery.refetch(); }} className="font-black underline">Reintentar</button>
            </p>
          )}
          <div className="grid min-h-0 flex-1 grid-rows-[minmax(0,1.35fr)_minmax(0,0.65fr)] gap-2 sm:gap-3 [@media(max-height:700px)_and_(max-width:767px)]:grid-rows-[minmax(0,1fr)_minmax(0,1fr)] md:grid-cols-[minmax(0,1fr)_minmax(260px,0.48fr)] md:grid-rows-1">
            <div className="flex min-h-0 flex-col overflow-hidden rounded-2xl bg-white p-3 sm:rounded-3xl sm:p-4 2xl:p-5">
            <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
              <h2 className="text-sm font-black uppercase tracking-widest text-[#434343]">Nadadores llamados</h2>
              <span className="text-xs font-bold text-slate-400">Etapa {state?.stage_number ?? '—'}</span>
            </div>
            {!state ? (
              <div className="grid flex-1 place-items-center p-8 text-center font-semibold text-slate-400">La pantalla se actualizará cuando el voluntario inicie el llamador.</div>
            ) : entries.length === 0 ? (
              <div className="grid flex-1 place-items-center p-8 text-center font-semibold text-slate-400">Este heat no tiene carriles asignados.</div>
            ) : (
              <ul className="flex min-h-0 flex-1 flex-col justify-evenly gap-1 overflow-y-auto py-1 sm:gap-1.5" aria-label="Asignaciones de carril">
                {entries.map((entry) => (
                  <li data-live-entry="current" key={`${entry.lane}-${entry.display_name}`} className="flex min-h-9 flex-1 items-center justify-between gap-3 rounded-xl bg-slate-50/70 px-3 py-1 sm:px-4 sm:py-1.5 2xl:py-2">
                    <div className="flex min-w-0 items-center gap-2 sm:gap-3">
                      <p className="truncate text-base font-black text-[#434343] sm:text-xl 2xl:text-3xl">{entry.display_name}</p>
                      <span data-live-entry-club="inline" className="max-w-28 shrink-0 truncate rounded-md bg-slate-200/70 px-2 py-0.5 text-[10px] font-bold text-slate-600 sm:max-w-36 sm:text-xs 2xl:text-sm">{entry.club_name || 'Club no informado'}</span>
                    </div>
                    <div className="shrink-0 text-brand-pool"><span className="mr-2 text-xs font-semibold sm:text-sm">Pista</span><span className="text-2xl font-black sm:text-3xl 2xl:text-4xl">{entry.lane}</span></div>
                  </li>
                ))}
              </ul>
            )}
            </div>

            <aside className="flex min-h-0 flex-col overflow-hidden rounded-2xl bg-white p-3 sm:rounded-3xl sm:p-4 2xl:p-5">
            <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
              <h2 className="text-xs font-black uppercase tracking-widest text-[#434343]"><span className="mr-2 inline-block h-2 w-2 animate-pulse rounded-full bg-amber-500" />PRÓXIMO HEAT {nextHeat ? String(nextHeat.heatNumber).padStart(2, '0') : '—'}</h2>
            </div>
            {nextHeat ? (
              <>
                <p className="px-2 pt-3 text-xs font-bold text-brand-pool">Evento {nextHeat.eventNumber} · {nextHeat.eventName}</p>
                <ul className="flex min-h-0 flex-1 flex-col justify-evenly gap-1 overflow-y-auto py-1 sm:py-2">
                  {nextHeat.entries.map((entry) => (
                    <li key={`${entry.lane}-${entry.display_name}`} className="flex min-h-8 flex-1 items-center justify-between gap-2 rounded-xl bg-slate-50 px-3 py-1 sm:py-1.5 2xl:py-2">
                      <span className="truncate font-bold text-[#434343]">{entry.display_name}</span>
                      <span className="max-w-28 truncate rounded bg-slate-200 px-2 py-1 text-[10px] font-bold text-slate-600">{entry.club_name || '--'}</span>
                    </li>
                  ))}
                </ul>
              </>
            ) : <div className="grid flex-1 place-items-center p-6 text-center text-sm font-semibold text-slate-400">Último heat alcanzado</div>}
            </aside>
          </div>
        </section>
      </div>
      {tickerAnnouncement && <aside data-live-announcement="ticker" role="status" aria-live="polite" aria-atomic="true" className="fixed inset-x-0 bottom-0 z-50 flex h-16 items-center gap-4 bg-slate-950 px-5 text-white shadow-2xl sm:px-8"><span className="shrink-0 rounded bg-amber-400 px-2 py-1 text-xs font-black uppercase tracking-wider text-slate-950">Comunicado</span><p className="truncate text-lg font-bold" title={tickerAnnouncement.message}>{tickerAnnouncement.message}</p></aside>}
    </main>
  );
};
