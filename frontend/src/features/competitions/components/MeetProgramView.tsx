import { useEffect, useMemo, useState } from 'react';
import { EmptyState } from '../../../components/ui/EmptyState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { LoadingState } from '../../../components/ui/LoadingState';
import type { MeetProgramResponse } from '../../../lib/schemas/competition';

type MeetProgramViewProps = {
  program?: MeetProgramResponse;
  competitionDate: string;
  isLoading: boolean;
  isError: boolean;
  onRetry: () => void;
};

type ScheduledHeat = {
  segmentKey: string;
  sessionKey: string;
  sessionName: string;
  stageNumber: number;
  scheduledDate: string;
  eventNumber: number;
  eventName: string;
  heatNumber: number;
  heatTotal: number | null;
  estimatedStartTime: string;
  startMinutes: number;
};

type EstimatedStatus = {
  label: string;
  heat: ScheduledHeat;
};

const SANTIAGO_TIME_ZONE = 'America/Santiago';

const timeToMinutes = (value: string) => {
  const match = /^(?<hour>[01]\d|2[0-3]):(?<minute>[0-5]\d)$/.exec(value);
  if (!match?.groups) return null;
  return Number(match.groups.hour) * 60 + Number(match.groups.minute);
};

const chileDateKey = (date: Date) => {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: SANTIAGO_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const value = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
};

const chileTimeMinutes = (date: Date) => {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: SANTIAGO_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const value = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return Number(value.hour) * 60 + Number(value.minute);
};

const getEstimatedStatus = (scheduledHeats: ScheduledHeat[], now: Date): EstimatedStatus | null => {
  if (scheduledHeats.length === 0) return null;
  const today = chileDateKey(now);
  const firstHeat = scheduledHeats[0];
  const lastHeat = scheduledHeats.at(-1)!;

  if (today < firstHeat.scheduledDate) {
    return { label: 'Próxima serie estimada', heat: firstHeat };
  }
  if (today > lastHeat.scheduledDate) {
    return { label: 'Programa estimado finalizado', heat: lastHeat };
  }

  const heatsToday = scheduledHeats.filter(heat => heat.scheduledDate === today);
  if (heatsToday.length === 0) {
    const nextHeat = scheduledHeats.find(heat => heat.scheduledDate > today);
    return nextHeat
      ? { label: 'Próxima serie estimada', heat: nextHeat }
      : { label: 'Última serie programada', heat: lastHeat };
  }
  const currentMinutes = chileTimeMinutes(now);
  const nextIndex = heatsToday.findIndex(heat => heat.startMinutes > currentMinutes);
  if (nextIndex === 0) {
    return { label: 'Próxima serie estimada', heat: heatsToday[0] };
  }
  if (nextIndex === -1) {
    return { label: 'Última serie programada', heat: heatsToday.at(-1)! };
  }
  return { label: 'Serie estimada actual', heat: heatsToday[nextIndex - 1] };
};

const normalizeSearch = (value: string) =>
  value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();

const matchesQuery = (value: string, tokens: string[]) => {
  const normalized = normalizeSearch(value);
  return tokens.every(token => normalized.includes(token));
};

const getMatchRanges = (value: string, tokens: string[]) => {
  const normalizedCharacters: string[] = [];
  const sourceIndexes: number[] = [];
  let sourceIndex = 0;

  for (const character of value) {
    const normalizedCharacter = normalizeSearch(character);
    for (const part of normalizedCharacter) {
      normalizedCharacters.push(part);
      sourceIndexes.push(sourceIndex);
    }
    sourceIndex += character.length;
  }

  const normalizedValue = normalizedCharacters.join('');
  const ranges: Array<[number, number]> = [];

  for (const token of tokens) {
    let matchIndex = normalizedValue.indexOf(token);
    while (matchIndex !== -1) {
      const start = sourceIndexes[matchIndex];
      const lastSourceIndex = sourceIndexes[matchIndex + token.length - 1];
      const end = lastSourceIndex + (Array.from(value.slice(lastSourceIndex))[0]?.length ?? 0);
      ranges.push([start, end]);
      matchIndex = normalizedValue.indexOf(token, matchIndex + token.length);
    }
  }

  return ranges
    .sort(([leftStart], [rightStart]) => leftStart - rightStart)
    .reduce<Array<[number, number]>>((merged, range) => {
      const previous = merged.at(-1);
      if (!previous || range[0] > previous[1]) {
        merged.push(range);
      } else {
        previous[1] = Math.max(previous[1], range[1]);
      }
      return merged;
    }, []);
};

const HighlightMatches = ({ value, tokens }: { value: string; tokens: string[] }) => {
  const ranges = getMatchRanges(value, tokens);
  if (ranges.length === 0) return value;

  const fragments = [];
  let cursor = 0;
  for (const [start, end] of ranges) {
    if (start > cursor) fragments.push(value.slice(cursor, start));
    fragments.push(
      <mark key={`${start}-${end}`} className="rounded-sm bg-yellow-200 px-0.5 text-inherit">
        {value.slice(start, end)}
      </mark>,
    );
    cursor = end;
  }
  if (cursor < value.length) fragments.push(value.slice(cursor));

  return <>{fragments}</>;
};

const toggleKey = (current: Set<string>, key: string) => {
  const next = new Set(current);
  if (next.has(key)) {
    next.delete(key);
  } else {
    next.add(key);
  }
  return next;
};

const sessionKey = (session: MeetProgramResponse['sessions'][number]) =>
  `${session.stage_number}:${session.pool_role}:${session.session_number}`;

export const MeetProgramView = ({
  program,
  competitionDate,
  isLoading,
  isError,
  onRetry,
}: MeetProgramViewProps) => {
  const [query, setQuery] = useState('');
  const [now, setNow] = useState(() => new Date());
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(new Set());
  const [expandedHeats, setExpandedHeats] = useState<Set<string>>(new Set());
  const tokens = useMemo(
    () => normalizeSearch(query).match(/[a-z0-9]+/g) ?? [],
    [query],
  );
  const isFiltering = tokens.length > 0;
  const scheduledHeats = useMemo(() => {
    if (!program) return [];
    return program.sessions
      .flatMap(session =>
        session.events.flatMap(event =>
          event.heats.flatMap<ScheduledHeat>(heat => {
            if (!heat.estimated_start_time) return [];
            const startMinutes = timeToMinutes(heat.estimated_start_time);
            if (startMinutes === null) return [];
            return [{
              segmentKey: `${session.scheduled_date ?? competitionDate.slice(0, 10)}:${session.stage_number}`,
              sessionKey: sessionKey(session),
              sessionName: session.session_name,
              stageNumber: session.stage_number,
              scheduledDate: session.scheduled_date ?? competitionDate.slice(0, 10),
              eventNumber: event.event_number,
              eventName: event.event_name,
              heatNumber: heat.heat_number,
              heatTotal: heat.heat_total,
              estimatedStartTime: heat.estimated_start_time,
              startMinutes,
            }];
          }),
        ),
      )
      .sort((left, right) =>
        left.scheduledDate.localeCompare(right.scheduledDate)
        || left.startMinutes - right.startMinutes
        || left.stageNumber - right.stageNumber,
      );
  }, [competitionDate, program]);
  const estimatedStatuses = useMemo(() => {
    const anchorStatus = getEstimatedStatus(scheduledHeats, now);
    if (!anchorStatus) return [];

    const segmentHeats = scheduledHeats.filter(
      heat => heat.segmentKey === anchorStatus.heat.segmentKey,
    );
    const sessionKeys = [...new Set(segmentHeats.map(heat => heat.sessionKey))];
    return sessionKeys.flatMap(session => {
      const status = getEstimatedStatus(
        segmentHeats.filter(heat => heat.sessionKey === session),
        now,
      );
      return status ? [status] : [];
    });
  }, [now, scheduledHeats]);
  const hasEstimatedSchedule = scheduledHeats.length > 0;

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 30_000);
    return () => window.clearInterval(timer);
  }, []);
  const eventTotals = useMemo(() => {
    const totals = new Map<string, { heats: number; entries: number }>();
    if (!program) return totals;

    for (const session of program.sessions) {
      for (const event of session.events) {
        const key = `${program.competition_id}:${sessionKey(session)}:${event.event_number}`;
        totals.set(key, {
          heats: event.heats.length,
          entries: event.heats.reduce((total, heat) => total + heat.entries.length, 0),
        });
      }
    }

    return totals;
  }, [program]);
  const sessions = useMemo(() => {
    if (!program) return [];
    if (tokens.length === 0) return program.sessions;

    return program.sessions.flatMap(session => {
      const events = session.events.flatMap(event => {
        const eventMatches = matchesQuery(
          `#${event.event_number} ${event.event_name}`,
          tokens,
        );
        const heats = eventMatches
          ? event.heats
          : event.heats.filter(heat =>
              heat.entries.some(entry =>
                matchesQuery(
                  [
                    entry.display_name,
                    entry.club_name ?? '',
                    ...entry.relay_members,
                  ].join(' '),
                  tokens,
                ),
              ),
            );
        return heats.length > 0 ? [{ ...event, heats }] : [];
      });
      return events.length > 0 ? [{ ...session, events }] : [];
    });
  }, [program, tokens]);

  const resetExpandedSections = () => {
    setExpandedEvents(new Set());
    setExpandedHeats(new Set());
  };

  const updateQuery = (value: string) => {
    setQuery(value);
    if (!value.trim()) resetExpandedSections();
  };

  const clearQuery = () => {
    setQuery('');
    resetExpandedSections();
  };

  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState onRetry={onRetry} />;
  if (!program?.publication) {
    return (
      <EmptyState
        title="Series aún no publicadas"
        description="El programa oficial todavía no está disponible."
      />
    );
  }

  return (
    <section className="space-y-5" aria-labelledby="meet-program-heading">
      {estimatedStatuses.length > 0 && (
        <div
          aria-live="polite"
          className={`sticky top-16 z-30 grid overflow-hidden rounded-xl border border-brand-cyan/40 bg-brand-night text-brand-white shadow-md ${
            estimatedStatuses.length > 1 ? 'h-32 sm:h-16 sm:grid-cols-2' : 'h-20 sm:h-16'
          }`}
        >
          {estimatedStatuses.map((status, index) => (
            <div
              key={status.heat.sessionKey}
              className={`flex min-w-0 items-center justify-between gap-3 px-4 ${
                index > 0 ? 'border-t border-brand-cyan/20 sm:border-l sm:border-t-0' : ''
              }`}
            >
              <div className="min-w-0">
                <p className="truncate text-[0.6rem] font-bold uppercase tracking-widest text-brand-cyan sm:text-[0.65rem]">
                  {status.label} · Según programa
                </p>
                <p className={`${estimatedStatuses.length > 1 ? 'line-clamp-2' : 'line-clamp-3'} text-xs font-bold leading-tight sm:line-clamp-1 sm:text-sm`}>
                  {program.sessions.length > 1 ? `${status.heat.sessionName} · ` : ''}
                  Prueba #{status.heat.eventNumber} · {status.heat.eventName}
                </p>
              </div>
              <div className="shrink-0 text-right">
                <p className="font-mono text-base font-black leading-none sm:text-lg">
                  {status.heat.estimatedStartTime}
                </p>
                <p className="mt-1 text-[0.65rem] font-semibold text-brand-muted sm:text-xs">
                  Serie {status.heat.heatNumber}
                  {status.heat.heatTotal ? ` de ${status.heat.heatTotal}` : ''}
                </p>
              </div>
            </div>
          ))}
        </div>
      )}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h2 id="meet-program-heading" className="text-2xl font-bold tracking-tight text-ink">
            Series
          </h2>
          <p className="mt-1 text-sm text-content-subtle">
            {program.publication.entry_count} inscripciones publicadas
            {program.publication.source_url && (
              <>
                {' · '}
                <a
                  href={program.publication.source_url}
                  target="_blank"
                  rel="noreferrer"
                  className="font-medium text-action hover:underline"
                >
                  Programa oficial
                </a>
              </>
            )}
          </p>
        </div>
        <div className="flex w-full gap-2 lg:w-96">
          <input
            type="search"
            value={query}
            onChange={event => updateQuery(event.target.value)}
            placeholder="Buscar prueba, nadador, club o relevo..."
            aria-label="Buscar en las series"
            className="min-w-0 flex-1 rounded-lg border border-line bg-surface px-4 py-2 text-sm text-ink outline-none transition-shadow focus:border-action focus:ring-2 focus:ring-action"
          />
          {query && (
            <button
              type="button"
              onClick={clearQuery}
              className="rounded-lg border border-line bg-surface px-4 py-2 text-sm font-medium text-content-muted hover:bg-canvas hover:text-ink"
            >
              Limpiar
            </button>
          )}
        </div>
      </div>


      {sessions.length === 0 ? (
        <EmptyState
          title="No se encontraron series"
          description="Intenta con otros términos de búsqueda."
        />
      ) : (
        sessions.map(session => (
          <section
            key={sessionKey(session)}
            className="space-y-4"
            aria-labelledby={`session-${sessionKey(session)}`}
          >
            <h3
              id={`session-${sessionKey(session)}`}
              className="text-xl font-black text-ink"
            >
              {session.session_name}
            </h3>
            {session.events.map(event => {
              const segmentKey = sessionKey(session);
              const eventKey = `${program.competition_id}:${segmentKey}:${event.event_number}`;
              const eventPanelId = `meet-program-event-${segmentKey}-${event.event_number}`;
              const eventIsExpanded = isFiltering || expandedEvents.has(eventKey);
              const eventTotal = eventTotals.get(eventKey);
              const eventHeatCount = eventTotal?.heats ?? event.heats.length;
              const eventEntryCount = eventTotal?.entries ??
                event.heats.reduce(
                  (total, heat) => total + heat.entries.length,
                  0,
                );

              return (
                <article
                  key={event.event_number}
                  className="rounded-xl border border-line bg-surface shadow-sm"
                >
                  <header
                    className={`sticky ${hasEstimatedSchedule ? estimatedStatuses.length > 1 ? 'top-48 sm:top-32' : 'top-36 sm:top-32' : 'top-16'} z-20 bg-canvas ${
                      eventIsExpanded
                        ? 'rounded-t-xl border-b border-line shadow-sm'
                        : 'rounded-xl'
                    }`}
                  >
                    <button
                      type="button"
                      onClick={() => setExpandedEvents(current => toggleKey(current, eventKey))}
                      aria-expanded={eventIsExpanded}
                      aria-controls={eventPanelId}
                      className="flex w-full items-center justify-between gap-4 px-4 py-3 text-left transition-colors hover:bg-line/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-action sm:px-6"
                    >
                      <span className="flex min-w-0 items-center gap-3">
                        <svg
                          className={`h-5 w-5 shrink-0 text-content-subtle transition-transform ${eventIsExpanded ? 'rotate-180' : ''}`}
                          fill="none"
                          viewBox="0 0 24 24"
                          stroke="currentColor"
                          aria-hidden="true"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                        </svg>
                        <span className="min-w-0">
                          <span className="block text-xs font-bold uppercase tracking-widest text-content-subtle">
                            Prueba #<HighlightMatches value={String(event.event_number)} tokens={tokens} />
                          </span>
                          <span className="block font-bold text-ink">
                            <HighlightMatches value={event.event_name} tokens={tokens} />
                          </span>
                        </span>
                      </span>
                      <span className="shrink-0 text-right text-xs font-medium text-content-subtle">
                        {eventHeatCount} {eventHeatCount === 1 ? 'serie' : 'series'}
                        <span className="hidden sm:inline"> · {eventEntryCount} inscripciones</span>
                      </span>
                    </button>
                  </header>
                  <div
                    id={eventPanelId}
                    hidden={!eventIsExpanded}
                    aria-hidden={!eventIsExpanded}
                    className="overflow-hidden rounded-b-xl divide-y divide-line"
                  >
                      {event.heats.map(heat => {
                        const heatKey = `${eventKey}:${heat.heat_number}`;
                        const heatPanelId = `${eventPanelId}-heat-${heat.heat_number}`;
                        const heatIsExpanded = isFiltering || expandedHeats.has(heatKey);

                        return (
                          <section key={heat.heat_number}>
                            <h5>
                              <button
                                type="button"
                                onClick={() => setExpandedHeats(current => toggleKey(current, heatKey))}
                                aria-expanded={heatIsExpanded}
                                aria-controls={heatPanelId}
                                className="flex w-full items-center justify-between gap-4 px-4 py-3 text-left font-bold text-content-muted transition-colors hover:bg-canvas/70 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-action sm:px-6"
                              >
                                <span className="flex items-center gap-2">
                                  <svg
                                    className={`h-4 w-4 text-content-subtle transition-transform ${heatIsExpanded ? 'rotate-180' : ''}`}
                                    fill="none"
                                    viewBox="0 0 24 24"
                                    stroke="currentColor"
                                    aria-hidden="true"
                                  >
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                  </svg>
                                  Serie {heat.heat_number}
                                  {heat.heat_total ? ` de ${heat.heat_total}` : ''}
                                </span>
                                <span className="shrink-0 text-right">
                                  {heat.estimated_start_time && (
                                    <span className="block font-mono text-sm font-bold text-ink">
                                      {heat.estimated_start_time}
                                    </span>
                                  )}
                                  <span className="block text-xs font-medium text-content-subtle">
                                    {heat.entries.length} inscripciones
                                  </span>
                                </span>
                              </button>
                            </h5>
                            <div
                              id={heatPanelId}
                              hidden={!heatIsExpanded}
                              aria-hidden={!heatIsExpanded}
                              className="grid gap-2 px-4 pb-4 sm:px-6 sm:pb-6"
                            >
                                {heat.entries.map(entry => (
                                  <div
                                    key={entry.lane}
                                    className="grid grid-cols-[3rem_1fr_auto] items-center gap-3 rounded-lg border border-line bg-canvas/50 p-3"
                                  >
                                    <span className="flex h-10 w-10 items-center justify-center rounded-full bg-brand-night text-sm font-black text-brand-white">
                                      {entry.lane}
                                    </span>
                                    <div className="min-w-0">
                                      <p className="font-semibold text-ink">
                                        <HighlightMatches value={entry.display_name} tokens={tokens} />
                                      </p>
                                      <p className="text-sm text-content-subtle">
                                        <HighlightMatches
                                          value={entry.club_name || 'Club no informado'}
                                          tokens={tokens}
                                        />
                                      </p>
                                      {entry.relay_members.length > 0 && (
                                        <p className="mt-1 text-xs text-content-muted">
                                          <HighlightMatches
                                            value={entry.relay_members.join(' · ')}
                                            tokens={tokens}
                                          />
                                        </p>
                                      )}
                                    </div>
                                    <span className="font-mono text-sm font-bold text-content-muted">
                                      {entry.seed_time_text || 'NT'}
                                    </span>
                                  </div>
                                ))}
                            </div>
                          </section>
                        );
                      })}
                  </div>
                </article>
              );
            })}
          </section>
        ))
      )}
    </section>
  );
};
