import React, { useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import type { MeetProgramSession } from '../../../lib/schemas/competition';
import { competitionService } from '../api/competitionService';

const LIVE_HEAT_POLL_INTERVAL_MS = 2_500;
const LIVE_ANNOUNCEMENT_POLL_INTERVAL_MS = 2_500;
// El logo se define antes de la competencia y no cambia durante el evento, a
// diferencia del heat y los comunicados. Se consulta lento para no triplicar el
// trafico del board por un dato practicamente estatico.
const LIVE_BRANDING_POLL_INTERVAL_MS = 60_000;

type ProgramHeat = {
  sessionNumber: number;
  eventNumber: number;
  eventName: string;
  heatNumber: number;
  heatTotal: number | null;
  entries: MeetProgramSession['events'][number]['heats'][number]['entries'];
};

const AutoFitAnnouncementText: React.FC<{ message: string }> = ({ message }) => {
  const frameRef = useRef<HTMLDivElement>(null);
  const textRef = useRef<HTMLParagraphElement>(null);

  useLayoutEffect(() => {
    const frame = frameRef.current;
    const text = textRef.current;
    if (!frame || !text) return undefined;

    let active = true;
    const fit = () => {
      if (!active || frame.clientWidth === 0 || frame.clientHeight === 0) return;
      let lower = 1;
      // Sin tope arbitrario: el limite es el alto del marco, porque una linea no
      // puede ser mas alta que el. El tope anterior de 112px frenaba al texto
      // antes de llenar la pantalla en un televisor.
      let upper = frame.clientHeight;
      while (upper - lower > 0.25) {
        const candidate = (lower + upper) / 2;
        text.style.fontSize = `${candidate}px`;
        if (text.scrollHeight <= frame.clientHeight && text.scrollWidth <= frame.clientWidth) lower = candidate;
        else upper = candidate;
      }
      text.style.fontSize = `${Math.floor(lower * 10) / 10}px`;
    };

    const observer = new ResizeObserver(fit);
    observer.observe(frame);
    fit();
    void document.fonts.ready.then(() => { if (active) fit(); });
    return () => { active = false; observer.disconnect(); };
  }, [message]);

  return (
    <div ref={frameRef} className="my-4 flex min-h-0 flex-1 items-center justify-center overflow-hidden sm:my-6">
      <p ref={textRef} className="max-h-full max-w-full whitespace-pre-wrap text-center font-black leading-[1.05] [overflow-wrap:anywhere]">{message}</p>
    </div>
  );
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
  const [logoFailedRevision, setLogoFailedRevision] = useState<number | null>(null);
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
  const brandingQuery = useQuery({
    queryKey: ['competition-live-branding', id],
    queryFn: () => competitionService.getLiveBranding(id!),
    enabled: Boolean(id),
    refetchInterval: LIVE_BRANDING_POLL_INTERVAL_MS,
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
      // Sin tarjeta ni ancho maximo: el comunicado ocupa la pantalla completa.
      // Solo queda el respiro minimo del borde, porque el texto pegado al canto
      // se pierde en televisores con overscan.
      <main data-live-layout="announcement-fullscreen" aria-live="polite" aria-atomic="true" className="grid h-dvh max-h-dvh overflow-hidden bg-brand-live font-sans text-white">
        <div className="flex min-h-0 w-full flex-col justify-between p-4 sm:p-6">
          <header className="flex items-center justify-between gap-3 border-b border-white/20 pb-4">
            <div><p className="text-xs font-black uppercase tracking-[0.22em] text-white/75">Comunicado oficial</p><h1 className="text-xl font-black sm:text-2xl">{competition.name}</h1></div>
            {/* Marca del torneo en version blanca, la unica legible sobre el
                fondo de la marca. Va sin texto alternativo porque el nombre de
                la competencia ya esta en el encabezado y se leeria dos veces. */}
            <img src="/nunoa-master-2026-white.png" alt="" className="h-12 w-auto shrink-0 object-contain sm:h-16" />
          </header>
          <AutoFitAnnouncementText message={announcement.message} />
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
        <div><p className="font-bold">No pudimos cargar el estado del llamador.</p><button type="button" onClick={() => { programQuery.refetch(); liveHeatQuery.refetch(); }} className="mt-4 rounded-xl bg-brand-live px-4 py-3 font-bold text-white">Reintentar</button></div>
      </main>
    );
  }

  const competition = competitionQuery.data.competition;
  const branding = brandingQuery.isError ? null : brandingQuery.data;
  const showLogo = branding?.has_logo && logoFailedRevision !== branding.revision;
  const entries = liveHeatQuery.data?.entries ?? [];
  const tickerAnnouncement = announcement?.display_mode === 'ticker' ? announcement : null;
  const tickerText = tickerAnnouncement ? tickerAnnouncement.message.toUpperCase() : '';
  const tickerRepeatCount = tickerAnnouncement
    ? Math.max(5, Math.ceil(100 / Math.max(tickerText.length, 1)))
    : 0;
  const tickerItems = Array.from({ length: tickerRepeatCount }, () => tickerText);
  const tickerTotalChars = tickerItems.join(' \u2022 ').length;
  const tickerDuration = Math.max(15, tickerTotalChars * 0.225);

  return (
    <main data-live-layout="caller-board" data-live-has-ticker={tickerAnnouncement ? 'true' : undefined} className="flex h-dvh max-h-dvh flex-col overflow-hidden bg-slate-100 p-2 font-sans text-slate-800 sm:p-4">
      <div className="mx-auto flex w-full min-h-0 flex-1 max-w-[1920px] flex-col gap-2 sm:gap-3 lg:flex-row lg:gap-5">
        <aside className="grid shrink-0 grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)] gap-2 sm:gap-3 [@media(max-height:700px)_and_(max-width:767px)]:grid-cols-3 lg:h-full lg:min-h-0 lg:w-[340px] lg:grid-cols-1 lg:grid-rows-[auto_auto_minmax(0,1fr)] xl:w-[380px]">
          <section data-live-section="heat" className="overflow-hidden rounded-2xl bg-brand-live text-white sm:rounded-3xl">
            <div className="flex h-full flex-col justify-center px-4 py-3 sm:px-6 sm:py-4 lg:py-5">
              <p className="text-xs font-extrabold uppercase tracking-[0.22em] text-white/80 [@media(max-height:700px)_and_(max-width:767px)]:hidden">Serie actual</p>
              <h1 className="mt-1 text-3xl font-black italic leading-none tracking-tight sm:text-4xl lg:text-5xl">HEAT {state ? String(state.heat_number).padStart(2, '0') : '--'}</h1>
              {state?.heat_total && <p className="mt-1 font-bold text-white/80">de {state.heat_total} heats</p>}
            </div>
          </section>
          <section data-live-section="event" className="flex min-w-0 flex-col justify-center overflow-hidden rounded-2xl bg-white px-4 py-3 text-slate-800 sm:rounded-3xl sm:px-6 sm:py-4 lg:py-5">
            <p className="text-xs font-extrabold uppercase tracking-widest text-brand-live">Evento {state?.event_number ?? '—'}</p>
            <h2 className="mt-1 line-clamp-2 text-lg font-black leading-tight text-[#434343] sm:text-xl [@media(max-height:700px)_and_(max-width:767px)]:line-clamp-1 lg:mt-2 lg:text-2xl">{state?.event_name ?? 'Llamador aún no iniciado'}</h2>
          </section>

          <header data-live-section="tournament" className="col-span-2 flex min-h-0 flex-col justify-center overflow-hidden rounded-2xl bg-white p-3 sm:rounded-3xl sm:p-4 [@media(max-height:700px)_and_(max-width:767px)]:col-span-1 lg:col-span-1 lg:p-6">
            <div className="flex min-h-0 flex-1 items-center justify-center">
              {/* En mobile la fila del logo es de alto automatico, asi que un
                  logo vertical se come la pantalla y desplaza a los nadadores.
                  Se acota por alto explicito y solo en lg se deja crecer. */}
              {showLogo ? <img
                src={competitionService.getLiveBrandingLogoUrl(id!, branding.revision)}
                alt={`Logo de ${competition.name}`}
                onError={() => setLogoFailedRevision(branding.revision)}
                className="max-h-24 max-w-full object-contain sm:max-h-32 lg:max-h-full"
              /> : <div className="min-w-0 self-start">
                <h2 className="mt-1 text-xl font-black leading-tight text-[#434343] [@media(max-height:700px)_and_(max-width:767px)]:truncate">{competition.name}</h2>
                <span className="text-xs font-bold text-slate-400">Etapa {state?.stage_number ?? 'única'}</span>
                <p className="mt-1 text-sm font-medium text-slate-500 [@media(max-height:700px)_and_(max-width:767px)]:hidden">{competition.location || 'Sede por confirmar'}</p>
              </div>}
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
            </div>
            {!state ? (
              <div className="grid flex-1 place-items-center p-8 text-center font-semibold text-slate-400">La pantalla se actualizará cuando el encargado inicie el llamador.</div>
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
                    <div className="shrink-0 text-brand-live"><span className="mr-2 text-xs font-semibold sm:text-sm">Pista</span><span className="text-2xl font-black sm:text-3xl 2xl:text-4xl">{entry.lane}</span></div>
                  </li>
                ))}
              </ul>
            )}
            </div>

            <aside className="flex min-h-0 flex-col overflow-hidden rounded-2xl bg-white p-3 sm:rounded-3xl sm:p-4 2xl:p-5">
            <div className="flex items-center justify-between border-b border-slate-200 px-2 pb-3">
              <h2 className="text-xs font-black uppercase tracking-widest text-[#434343]"><span className="mr-2 inline-block h-2 w-2 animate-pulse rounded-full bg-amber-500" />PRÓXIMA SERIE {nextHeat ? String(nextHeat.heatNumber).padStart(2, '0') : '—'}</h2>
            </div>
            {nextHeat ? (
              <>
                <p className="px-2 pt-3 text-xs font-bold text-brand-live">Evento {nextHeat.eventNumber} · {nextHeat.eventName}</p>
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
      {/* Pie de marca solo en mobile: en la pantalla de piscina el board es una
          TV sin interaccion, y el enlace ocupa alto que necesitan los nadadores.
          Queda en el flujo, por lo que el padding del ticker lo mantiene visible. */}
      <footer data-live-section="board-footer" className="mt-2 flex shrink-0 items-center justify-between gap-3 rounded-2xl bg-white px-4 py-2 sm:mt-3 sm:rounded-3xl lg:hidden">
        <div className="flex items-center gap-2">
          <img
            src="/web-app-manifest-192x192.png"
            alt="SwimStats Chile"
            className="h-7 w-7"
          />
          <span className="text-lg font-bold tracking-tight">
            SwimStats.cl
          </span>
        </div>
        <Link to={`/competitions/${id}?tab=series`} className="text-xs font-bold text-brand-live hover:underline">Ver sembrado</Link>
      </footer>
      {tickerAnnouncement && (
        <aside data-live-announcement="ticker" role="status" aria-live="polite" aria-atomic="true" aria-label={`Comunicado: ${tickerAnnouncement.message}`} className="fixed inset-x-0 bottom-0 z-50 flex h-9 items-center overflow-hidden border-t border-white/25 bg-brand-live text-white shadow-2xl motion-reduce:h-auto motion-reduce:min-h-11 motion-reduce:py-2 sm:h-10 md:h-11">
          <div className="w-full overflow-hidden" aria-hidden="true">
            <div className="live-announcement-ticker-track flex w-max shrink-0 items-center whitespace-nowrap" style={{ '--live-announcement-ticker-duration': `${tickerDuration}s` } as React.CSSProperties}>
              {[0, 1].map((copyIndex) => (
                <div key={copyIndex} data-live-ticker-copy className="live-announcement-ticker-copy flex shrink-0 items-center" aria-hidden={copyIndex === 1 ? 'true' : undefined}>
                  {tickerItems.map((text, itemIndex) => (
                    <span key={itemIndex} data-live-ticker-item className="flex shrink-0 items-center">
                      <span className="live-announcement-ticker-message px-5 text-xs font-black uppercase tracking-widest drop-shadow-xs sm:text-sm md:text-base">{text}</span>
                      <span className="live-announcement-ticker-separator text-xs font-bold text-white/60">&bull;</span>
                    </span>
                  ))}
                </div>
              ))}
            </div>
          </div>
        </aside>
      )}
    </main>
  );
};
