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

export const MeetProgramView = ({
  program,
  isLoading,
  isError,
  onRetry,
}: MeetProgramViewProps) => {
  const [query, setQuery] = useState('');
  const tokens = useMemo(
    () => normalizeSearch(query).match(/[a-z0-9]+/g) ?? [],
    [query],
  );
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
            onChange={event => setQuery(event.target.value)}
            placeholder="Buscar prueba, nadador, club o relevo..."
            aria-label="Buscar en las series"
            className="min-w-0 flex-1 rounded-lg border border-line bg-surface px-4 py-2 text-sm text-ink outline-none transition-shadow focus:border-action focus:ring-2 focus:ring-action"
          />
          {query && (
            <button
              type="button"
              onClick={() => setQuery('')}
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
            {session.events.map(event => (
              <article
                key={event.event_number}
                className="overflow-hidden rounded-xl border border-line bg-surface shadow-sm"
              >
                <header className="border-b border-line bg-canvas px-4 py-3 sm:px-6">
                  <p className="text-xs font-bold uppercase tracking-widest text-content-subtle">
                    Prueba #{event.event_number}
                  </p>
                  <h4 className="font-bold text-ink">{event.event_name}</h4>
                </header>
                <div className="divide-y divide-line">
                  {event.heats.map(heat => (
                    <section key={heat.heat_number} className="p-4 sm:p-6">
                      <h5 className="mb-3 font-bold text-content-muted">
                        Serie {heat.heat_number}
                        {heat.heat_total ? ` de ${heat.heat_total}` : ''}
                      </h5>
                      <div className="grid gap-2">
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
                  ))}
                </div>
              </article>
            ))}
          </section>
        ))
      )}
    </section>
  );
};
