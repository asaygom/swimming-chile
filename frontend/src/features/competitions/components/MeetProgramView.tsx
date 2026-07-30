import { useMemo, useState } from 'react';
import { EmptyState } from '../../../components/ui/EmptyState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { LoadingState } from '../../../components/ui/LoadingState';
import type { MeetProgramResponse } from '../../../lib/schemas/competition';

type MeetProgramViewProps = {
  program?: MeetProgramResponse;
  isLoading: boolean;
  isError: boolean;
  onRetry: () => void;
};

const normalizeSearch = (value: string) =>
  value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();

const matchesQuery = (value: string, tokens: string[]) => {
  const normalized = normalizeSearch(value);
  return tokens.every(token => normalized.includes(token));
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

export const MeetProgramView = ({
  program,
  isLoading,
  isError,
  onRetry,
}: MeetProgramViewProps) => {
  const [query, setQuery] = useState('');
  const [expandedEvents, setExpandedEvents] = useState<Set<string>>(new Set());
  const [expandedHeats, setExpandedHeats] = useState<Set<string>>(new Set());
  const tokens = useMemo(
    () => normalizeSearch(query).match(/[a-z0-9]+/g) ?? [],
    [query],
  );
  const isFiltering = tokens.length > 0;
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
            key={session.session_number}
            className="space-y-4"
            aria-labelledby={`session-${session.session_number}`}
          >
            <h3
              id={`session-${session.session_number}`}
              className="text-xl font-black text-ink"
            >
              {session.session_name}
            </h3>
            {session.events.map(event => {
              const eventKey = `${program.competition_id}:${session.session_number}:${event.event_number}`;
              const eventPanelId = `meet-program-event-${session.session_number}-${event.event_number}`;
              const eventIsExpanded = isFiltering || expandedEvents.has(eventKey);
              const eventEntryCount = event.heats.reduce(
                (total, heat) => total + heat.entries.length,
                0,
              );

              return (
                <article
                  key={event.event_number}
                  className="overflow-hidden rounded-xl border border-line bg-surface shadow-sm"
                >
                  <header className={eventIsExpanded ? 'border-b border-line bg-canvas' : 'bg-canvas'}>
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
                            Prueba #{event.event_number}
                          </span>
                          <span className="block font-bold text-ink">{event.event_name}</span>
                        </span>
                      </span>
                      <span className="shrink-0 text-right text-xs font-medium text-content-subtle">
                        {event.heats.length} {event.heats.length === 1 ? 'serie' : 'series'}
                        <span className="hidden sm:inline"> · {eventEntryCount} inscripciones</span>
                      </span>
                    </button>
                  </header>
                  <div
                    id={eventPanelId}
                    hidden={!eventIsExpanded}
                    aria-hidden={!eventIsExpanded}
                    className="divide-y divide-line"
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
                                <span className="text-xs font-medium text-content-subtle">
                                  {heat.entries.length} inscripciones
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
                                      <p className="font-semibold text-ink">{entry.display_name}</p>
                                      <p className="text-sm text-content-subtle">
                                        {entry.club_name || 'Club no informado'}
                                      </p>
                                      {entry.relay_members.length > 0 && (
                                        <p className="mt-1 text-xs text-content-muted">
                                          {entry.relay_members.join(' · ')}
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
