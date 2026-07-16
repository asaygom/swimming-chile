import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { rankingService } from '../api/rankingService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';

const strokeLabels: Record<string, string> = {
  freestyle: 'Libre',
  backstroke: 'Espalda',
  breaststroke: 'Pecho',
  butterfly: 'Mariposa',
  individual_medley: 'Combinado',
};

const genderLabels: Record<string, string> = {
  women: 'Damas',
  men: 'Varones',
  mixed: 'Mixto',
};

const courseLabels: Record<string, string> = {
  scm: 'Piscina corta',
  lcm: 'Piscina larga',
  unknown: 'Sin dato de piscina',
};

type AnalyticsView = 'swimmers' | 'clubs' | 'competitions';

const currentYear = new Date().getFullYear();

function formatCompetitionDate(date?: string | null) {
  if (!date) return 's/f';
  return new Date(`${date}T12:00:00`).toLocaleDateString('es-CL', {
    day: '2-digit',
    month: 'short',
  });
}

export const RankingsPage: React.FC = () => {
  const [activeView, setActiveView] = React.useState<AnalyticsView>('swimmers');
  const [distance, setDistance] = React.useState('50');
  const [stroke, setStroke] = React.useState('freestyle');
  const [gender, setGender] = React.useState('men');
  const [ageGroup, setAgeGroup] = React.useState('all');
  const [courseType, setCourseType] = React.useState('all');
  const [year, setYear] = React.useState('all');
  const [page, setPage] = React.useState(1);
  const [athleteSearchInput, setAthleteSearchInput] = React.useState('');
  const [athleteSearch, setAthleteSearch] = React.useState('');
  const [clubStatsYear, setClubStatsYear] = React.useState(String(currentYear));
  const [clubStatsGoverningBody, setClubStatsGoverningBody] = React.useState('all');

  const filtersQuery = useQuery({
    queryKey: ['ranking-filter-options'],
    queryFn: () => rankingService.getFilterOptions(),
  });

  const validEventOptions = filtersQuery.data?.event_options || [];
  const selectedEventIsValid = validEventOptions.length === 0 || validEventOptions.some(
    (option) => option.distance_m === Number(distance) && option.stroke === stroke
  );
  const normalizedDistance = selectedEventIsValid ? distance : String(validEventOptions[0]?.distance_m || distance);
  const normalizedStroke = selectedEventIsValid ? stroke : (validEventOptions[0]?.stroke || stroke);
  const strokeOptions = validEventOptions.length
    ? Array.from(new Set(validEventOptions.map((option) => option.stroke)))
    : (filtersQuery.data?.strokes.length ? filtersQuery.data.strokes : Object.keys(strokeLabels));
  const distanceOptions = validEventOptions.length
    ? Array.from(new Set(
        validEventOptions
          .filter((option) => option.stroke === normalizedStroke)
          .map((option) => option.distance_m)
      ))
    : (filtersQuery.data?.distances.length ? filtersQuery.data.distances : [50, 100, 200, 400]);

  const rankingsQuery = useQuery({
    queryKey: ['rankings', normalizedDistance, normalizedStroke, gender, ageGroup, courseType, year, athleteSearch, page],
    queryFn: () => rankingService.getRankings({
      distance_m: normalizedDistance,
      stroke: normalizedStroke,
      gender,
      age_group: ageGroup,
      course_type: courseType,
      year,
      athlete_search: athleteSearch,
      page,
    }),
    enabled: activeView === 'swimmers',
  });

  const clubParticipationQuery = useQuery({
    queryKey: ['club-participation', clubStatsYear, clubStatsGoverningBody],
    queryFn: () => rankingService.getClubParticipation(1, {
      year: clubStatsYear,
      governing_body: clubStatsGoverningBody,
    }),
    enabled: activeView === 'clubs',
  });

  const clubStatsFilterQuery = useQuery({
    queryKey: ['club-stats-filter-options'],
    queryFn: () => rankingService.getClubStatsFilterOptions(),
    enabled: activeView === 'clubs' || activeView === 'competitions',
  });

  const clubParticipationMatrixQuery = useQuery({
    queryKey: ['club-participation-matrix', clubStatsYear, clubStatsGoverningBody],
    queryFn: () => rankingService.getClubParticipationMatrix({
      year: clubStatsYear,
      governing_body: clubStatsGoverningBody,
    }),
    enabled: activeView === 'clubs',
  });

  const competitionStatsQuery = useQuery({
    queryKey: ['competition-stats-table', clubStatsYear, clubStatsGoverningBody],
    queryFn: () => rankingService.getCompetitionStatsTable({
      year: clubStatsYear,
      governing_body: clubStatsGoverningBody,
    }),
    enabled: activeView === 'competitions',
  });

  const resetPage = (setter: (value: string) => void) => (value: string) => {
    setter(value);
    setPage(1);
  };

  const updateDistance = (nextDistance: string) => {
    setDistance(nextDistance);
    setPage(1);
  };

  const updateStroke = (nextStroke: string) => {
    const compatibleDistances = validEventOptions
      .filter((option) => option.stroke === nextStroke)
      .map((option) => option.distance_m);
    setStroke(nextStroke);
    if (compatibleDistances.length > 0 && !compatibleDistances.includes(Number(distance))) {
      setDistance(String(compatibleDistances[0]));
    }
    setPage(1);
  };

  const submitAthleteSearch = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setAthleteSearch(athleteSearchInput.trim());
    setPage(1);
  };

  const clearAthleteSearch = () => {
    setAthleteSearchInput('');
    setAthleteSearch('');
    setPage(1);
  };

  const title = `${normalizedDistance}m ${strokeLabels[normalizedStroke] || normalizedStroke} ${genderLabels[gender] || gender}`;
  const subtitle = [
    ageGroup !== 'all' ? `Categoría ${ageGroup}` : 'Todas las categorías',
    courseType !== 'all' ? courseLabels[courseType] || courseType.toUpperCase() : 'Todas las piscinas',
    year !== 'all' ? year : 'Últimos 12 meses',
  ].join(' • ');

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <div>
        <h1 className="text-3xl font-bold text-ink tracking-tight">Rankings y estadísticas</h1>
        <p className="text-content-subtle mt-1">Elige si quieres ver rankings de nadadores o estadísticas de participación.</p>
      </div>

      <div className="grid min-w-0 gap-6 lg:grid-cols-[220px_minmax(0,1fr)]">
        <aside className="min-w-0 lg:sticky lg:top-24 lg:self-start">
          <nav className="flex gap-2 overflow-x-auto rounded-xl border border-brand-steel bg-brand-night p-2 shadow-sm lg:grid lg:grid-cols-1 lg:overflow-visible">
            <button
              type="button"
              onClick={() => setActiveView('swimmers')}
              className={`min-w-[180px] rounded-lg px-4 py-3 text-left text-sm font-semibold transition-colors lg:min-w-0 ${
                activeView === 'swimmers'
                  ? 'bg-brand-cyan text-brand-night ring-1 ring-brand-cyan/30'
                  : 'text-brand-muted hover:bg-brand-panel hover:text-brand-white'
              }`}
            >
              Rankings de nadadores
            </button>
            <button
              type="button"
              onClick={() => setActiveView('clubs')}
              className={`min-w-[180px] rounded-lg px-4 py-3 text-left text-sm font-semibold transition-colors lg:min-w-0 ${
                activeView === 'clubs'
                  ? 'bg-brand-cyan text-brand-night ring-1 ring-brand-cyan/30'
                  : 'text-brand-muted hover:bg-brand-panel hover:text-brand-white'
              }`}
            >
              Estadísticas de clubes
            </button>
            <button
              type="button"
              onClick={() => setActiveView('competitions')}
              className={`min-w-[180px] rounded-lg px-4 py-3 text-left text-sm font-semibold transition-colors lg:min-w-0 ${
                activeView === 'competitions'
                  ? 'bg-brand-cyan text-brand-night ring-1 ring-brand-cyan/30'
                  : 'text-brand-muted hover:bg-brand-panel hover:text-brand-white'
              }`}
            >
              Estadísticas por competencia
            </button>
          </nav>
        </aside>

        <div className="min-w-0 space-y-6">
          {activeView === 'swimmers' && (
            <>
              <section className="bg-surface rounded-xl shadow-sm border border-line p-4">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
                  <select value={normalizedStroke} onChange={(event) => updateStroke(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    {strokeOptions.map((value) => (
                      <option key={value} value={value}>{strokeLabels[value] || value}</option>
                    ))}
                  </select>
                  <select value={normalizedDistance} onChange={(event) => updateDistance(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    {distanceOptions.map((value) => (
                      <option key={value} value={value}>{value}m</option>
                    ))}
                  </select>
                  <select value={gender} onChange={(event) => resetPage(setGender)(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    <option value="women">Damas</option>
                    <option value="men">Varones</option>
                    <option value="mixed">Mixto</option>
                  </select>
                  <select value={ageGroup} onChange={(event) => resetPage(setAgeGroup)(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    <option value="all">Todas las categorías</option>
                    {filtersQuery.data?.age_groups.map((value) => (
                      <option key={value} value={value}>{value}</option>
                    ))}
                  </select>
                  <select value={courseType} onChange={(event) => resetPage(setCourseType)(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    <option value="all">Todas las piscinas</option>
                    <option value="scm">Piscina corta</option>
                    <option value="lcm">Piscina larga</option>
                    <option value="unknown">Sin dato</option>
                  </select>
                  <select value={year} onChange={(event) => resetPage(setYear)(event.target.value)} className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink">
                    <option value="all">Últimos 12 meses</option>
                    {filtersQuery.data?.years.map((value) => (
                      <option key={value} value={value}>{value}</option>
                    ))}
                  </select>
                </div>

                <form onSubmit={submitAthleteSearch} className="mt-4 flex flex-col gap-2 sm:flex-row">
                  <input
                    type="search"
                    value={athleteSearchInput}
                    onChange={(event) => setAthleteSearchInput(event.target.value)}
                    placeholder="Buscar atleta en este ranking"
                    className="min-w-0 flex-1 rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink"
                  />
                  <div className="flex gap-2">
                    <button type="submit" className="rounded-lg bg-action px-4 py-2 text-sm font-semibold text-brand-white hover:bg-brand-steel">
                      Buscar
                    </button>
                    {athleteSearch && (
                      <button type="button" onClick={clearAthleteSearch} className="rounded-lg border border-line px-4 py-2 text-sm font-semibold text-content-muted hover:bg-canvas">
                        Limpiar
                      </button>
                    )}
                  </div>
                </form>

                {athleteSearch && (
                  <p className="mt-3 text-sm text-content-subtle">
                    Mostrando coincidencias para <span className="font-semibold text-content-muted">{athleteSearch}</span> dentro del ranking filtrado.
                  </p>
                )}
              </section>

              <section className="space-y-4">
                <div className="bg-brand-night rounded-xl border border-brand-steel p-6 text-brand-white shadow-lg shadow-brand-night/20">
                  <h2 className="text-xl font-bold">Top marcas - {title}</h2>
                  <p className="text-brand-muted text-sm mt-1">{subtitle}</p>
                </div>

                {rankingsQuery.isLoading && <LoadingState />}
                {rankingsQuery.isError && <ErrorState onRetry={() => rankingsQuery.refetch()} />}

                {!rankingsQuery.isLoading && !rankingsQuery.isError && rankingsQuery.data && (
                  rankingsQuery.data.data.length === 0 ? (
                    <EmptyState title="No hay marcas para estos filtros" description="Prueba con otra categoría, prueba o piscina." />
                  ) : (
                    <div className="bg-surface rounded-xl shadow-sm border border-line overflow-hidden">
                      <div className="divide-y divide-line md:hidden">
                        {rankingsQuery.data.data.map((entry) => (
                          <article key={`mobile-${entry.rank}-${entry.athlete_id}-${entry.time_ms}`} className="p-4">
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0">
                                <p className="text-xs font-bold uppercase tracking-widest text-action">#{entry.rank}</p>
                                <Link to={`/athletes/${entry.athlete_id}`} className="mt-1 block truncate font-semibold text-action hover:underline">
                                  {entry.athlete_name}
                                </Link>
                                <p className="mt-1 truncate text-sm text-content-subtle">{entry.club_name || 'Sin club'}</p>
                              </div>
                              <span className="shrink-0 font-mono text-lg font-bold text-action">{entry.time_text}</span>
                            </div>
                            <div className="mt-3 grid grid-cols-2 gap-3 text-xs text-content-subtle">
                              <div>
                                <p className="font-semibold text-content-muted">{entry.age_group}</p>
                                {entry.current_age && <p>{entry.current_age} años</p>}
                              </div>
                              <div className="text-right">
                                <Link to={`/competitions/${entry.competition_id}`} className="block truncate text-action hover:underline">
                                  {entry.competition_name}
                                </Link>
                                <p className="text-content-subtle">
                                  {entry.date ? new Date(`${entry.date}T12:00:00`).getFullYear() : 's/f'}
                                  {' '}• categoría {entry.event_age_group}
                                </p>
                              </div>
                            </div>
                          </article>
                        ))}
                      </div>

                      <div className="hidden overflow-x-auto md:block">
                        <table className="w-full text-sm text-left">
                          <thead className="bg-canvas text-content-muted font-medium border-b border-line">
                            <tr>
                              <th className="px-6 py-4 w-16 text-center">Pos</th>
                              <th className="px-6 py-4">Atleta</th>
                              <th className="px-6 py-4">Club</th>
                              <th className="px-6 py-4">Tiempo</th>
                              <th className="px-6 py-4 text-center">Categoría actual</th>
                              <th className="px-6 py-4">Competencia</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-line">
                            {rankingsQuery.data.data.map((entry) => (
                              <tr key={`${entry.rank}-${entry.athlete_id}-${entry.time_ms}`} className="hover:bg-canvas transition-colors">
                                <td className="px-6 py-4 text-center">
                                  <span className={`inline-flex items-center justify-center w-8 h-8 rounded-full font-bold ${
                                    entry.rank === 1 ? 'bg-medal-gold/15 text-medal-gold ring-1 ring-medal-gold/40' :
                                    entry.rank === 2 ? 'bg-medal-silver/15 text-medal-silver ring-1 ring-medal-silver/40' :
                                    entry.rank === 3 ? 'bg-medal-bronze/15 text-medal-bronze ring-1 ring-medal-bronze/40' :
                                    'text-content-subtle'
                                  }`}>
                                    {entry.rank}
                                  </span>
                                </td>
                                <td className="px-6 py-4 font-semibold">
                                  <Link to={`/athletes/${entry.athlete_id}`} className="text-action hover:underline">
                                    {entry.athlete_name}
                                  </Link>
                                </td>
                                <td className="px-6 py-4 text-content-muted">{entry.club_name || 'Sin club'}</td>
                                <td className="px-6 py-4 ">
                                  <span className="font-mono font-bold text-action text-base">{entry.time_text}</span>
                                </td>
                                <td className="px-6 py-4 text-content-muted text-center">
                                  <span className="font-medium text-ink">{entry.age_group}</span>
                                  {entry.current_age && (
                                    <span className="block text-xs text-content-subtle">{entry.current_age} años</span>
                                  )}
                                </td>
                                <td className="px-6 py-4 text-content-subtle text-xs">
                                  <Link to={`/competitions/${entry.competition_id}`} className="text-action hover:underline">
                                    {entry.competition_name}
                                  </Link>
                                  <br />
                                  <span className="text-content-subtle">
                                    {entry.date ? new Date(`${entry.date}T12:00:00`).getFullYear() : 's/f'}
                                    {' '}• categoría {entry.event_age_group}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="flex flex-col gap-3 border-t border-line px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-6">
                        <p className="text-sm text-content-subtle">
                          Página {rankingsQuery.data.meta.page} de {rankingsQuery.data.meta.total_pages}
                        </p>
                        <div className="flex gap-2">
                          <button onClick={() => setPage((currentPage) => Math.max(1, currentPage - 1))} disabled={page === 1} className="px-4 py-2 border border-line rounded-lg text-sm font-medium text-content-muted hover:bg-canvas disabled:opacity-50">
                            Anterior
                          </button>
                          <button onClick={() => setPage((currentPage) => Math.min(rankingsQuery.data.meta.total_pages, currentPage + 1))} disabled={page >= rankingsQuery.data.meta.total_pages} className="px-4 py-2 border border-line rounded-lg text-sm font-medium text-content-muted hover:bg-canvas disabled:opacity-50">
                            Siguiente
                          </button>
                        </div>
                      </div>
                    </div>
                  )
                )}
              </section>
            </>
          )}

          {activeView === 'clubs' && (
            <section className="space-y-4">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <h2 className="text-2xl font-bold text-ink tracking-tight">Clubes con mayor participación</h2>
                  <p className="text-content-subtle text-sm">Ordenado por nadadores únicos que representaron a cada club.</p>
                </div>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <label className="text-sm font-medium text-content-muted">
                    Año
                    <select
                      value={clubStatsYear}
                      onChange={(event) => setClubStatsYear(event.target.value)}
                      className="mt-1 block w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink"
                    >
                      <option value={currentYear}>{currentYear}</option>
                      {clubStatsFilterQuery.data?.years.filter((value) => value !== currentYear).map((value) => (
                        <option key={value} value={value}>{value}</option>
                      ))}
                    </select>
                  </label>

                  <label className="text-sm font-medium text-content-muted">
                    Circuito
                    <select
                      value={clubStatsGoverningBody}
                      onChange={(event) => setClubStatsGoverningBody(event.target.value)}
                      className="mt-1 block w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink"
                    >
                      <option value="all">Todos los circuitos</option>
                      {clubStatsFilterQuery.data?.governing_bodies.map((option) => (
                        <option key={option.governing_body_code} value={option.governing_body_code}>
                          {option.governing_body_name || option.governing_body_code.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
              </div>

              {clubParticipationQuery.isLoading && <LoadingState />}
              {clubParticipationQuery.isError && <ErrorState onRetry={() => clubParticipationQuery.refetch()} />}
              {!clubParticipationQuery.isLoading && !clubParticipationQuery.isError && clubParticipationQuery.data && (
                <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
                  {clubParticipationQuery.data.data.slice(0, 6).map((club) => (
                    <Link key={club.club_id} to={`/clubs/${club.club_id}`} className="bg-surface rounded-xl border border-line p-5 text-action shadow-sm hover:border-action hover:shadow-md transition-all">
                      <div className="flex items-start justify-between gap-4">
                        <div>
                          <p className="text-xs font-bold uppercase tracking-widest text-action">#{club.rank}</p>
                          <h3 className="mt-1 text-lg font-bold text-action">{club.club_name}</h3>
                        </div>
                        <span className="rounded-full bg-brand-cyan px-3 py-1 text-sm font-bold text-brand-night">
                          {club.unique_athletes}
                        </span>
                      </div>
                      <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
                        <div>
                          <p className="text-content-subtle">Competencias</p>
                          <p className="font-bold text-ink">{club.competitions_count}</p>
                        </div>
                        <div>
                          <p className="text-content-subtle">Entradas</p>
                          <p className="font-bold text-ink">{club.entries_count}</p>
                        </div>
                      </div>
                    </Link>
                  ))}
                </div>
              )}

              <div className="bg-surface rounded-xl border border-line shadow-sm">
                <div className="border-b border-line px-4 py-4 sm:px-6">
                  <h3 className="font-bold text-ink">Participación por competencia</h3>
                  <p className="mt-1 text-sm text-content-subtle">
                    Atletas únicos que representaron al club en cada competencia.
                  </p>
                </div>

                {clubParticipationMatrixQuery.isLoading && <LoadingState />}
                {clubParticipationMatrixQuery.isError && <ErrorState onRetry={() => clubParticipationMatrixQuery.refetch()} />}
                {!clubParticipationMatrixQuery.isLoading && !clubParticipationMatrixQuery.isError && clubParticipationMatrixQuery.data && (
                  clubParticipationMatrixQuery.data.competitions.length === 0 ? (
                    <EmptyState title="No hay participación para estos filtros" description="Prueba con otro año o circuito." />
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="min-w-full text-left text-sm">
                        <thead className="bg-canvas text-content-muted">
                          <tr>
                            <th className="sticky left-0 z-20 min-w-[220px] border-r border-line bg-canvas px-4 py-3 font-semibold shadow-[4px_0_8px_-8px_rgba(15,23,42,0.45)]">Club</th>
                            <th className="min-w-[96px] px-4 py-3 text-center font-semibold">Total</th>
                            {clubParticipationMatrixQuery.data.competitions.map((competition) => (
                              <th key={competition.id} className="min-w-[140px] px-3 py-3 text-center align-bottom font-semibold">
                                <Link to={`/competitions/${competition.id}`} className="block max-h-14 overflow-hidden text-action hover:underline">
                                  {competition.name}
                                </Link>
                                <span className="mt-1 block text-xs font-normal text-content-subtle">
                                  {formatCompetitionDate(competition.date)}
                                </span>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-line">
                          <tr className="bg-brand-cyan/10 font-bold text-ink">
                            <td className="sticky left-0 z-20 border-r border-brand-cyan/30 bg-surface px-4 py-3 shadow-[4px_0_8px_-8px_rgba(15,23,42,0.45)]">Total competencia</td>
                            <td className="px-4 py-3 text-center">
                              {Object.values(clubParticipationMatrixQuery.data.totals).reduce((sum, value) => sum + value, 0)}
                            </td>
                            {clubParticipationMatrixQuery.data.competitions.map((competition) => (
                              <td key={competition.id} className="px-3 py-3 text-center">
                                {clubParticipationMatrixQuery.data.totals[String(competition.id)] || 0}
                              </td>
                            ))}
                          </tr>

                          {clubParticipationMatrixQuery.data.clubs.map((club) => (
                            <tr key={club.club_id} className="hover:bg-canvas">
                              <td className="sticky left-0 z-10 border-r border-line bg-surface px-4 py-3 shadow-[4px_0_8px_-8px_rgba(15,23,42,0.45)]">
                                <Link to={`/clubs/${club.club_id}`} className="font-semibold text-action hover:underline">
                                  {club.rank}. {club.club_name}
                                </Link>
                              </td>
                              <td className="px-4 py-3 text-center font-bold text-ink">{club.total_athletes}</td>
                              {clubParticipationMatrixQuery.data.competitions.map((competition) => {
                                const count = club.cells[String(competition.id)] || 0;
                                return (
                                  <td key={competition.id} className="px-3 py-3 text-center">
                                    <span className={count > 0 ? 'font-semibold text-ink' : 'text-content-subtle'}>
                                      {count > 0 ? count : '—'}
                                    </span>
                                  </td>
                                );
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )
                )}
              </div>
            </section>
          )}

          {activeView === 'competitions' && (
            <section className="space-y-4">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <h2 className="text-2xl font-bold text-ink tracking-tight">Estadísticas por competencia</h2>
                </div>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <label className="text-sm font-medium text-content-muted">
                    Año
                    <select
                      value={clubStatsYear}
                      onChange={(event) => setClubStatsYear(event.target.value)}
                      className="mt-1 block w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink"
                    >
                      <option value={currentYear}>{currentYear}</option>
                      {clubStatsFilterQuery.data?.years.filter((value) => value !== currentYear).map((value) => (
                        <option key={value} value={value}>{value}</option>
                      ))}
                    </select>
                  </label>

                  <label className="text-sm font-medium text-content-muted">
                    Circuito
                    <select
                      value={clubStatsGoverningBody}
                      onChange={(event) => setClubStatsGoverningBody(event.target.value)}
                      className="mt-1 block w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink"
                    >
                      <option value="all">Todos los circuitos</option>
                      {clubStatsFilterQuery.data?.governing_bodies.map((option) => (
                        <option key={option.governing_body_code} value={option.governing_body_code}>
                          {option.governing_body_name || option.governing_body_code.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
              </div>

              <div className="bg-surface rounded-xl border border-line shadow-sm">

                {competitionStatsQuery.isLoading && <LoadingState />}
                {competitionStatsQuery.isError && <ErrorState onRetry={() => competitionStatsQuery.refetch()} />}
                {!competitionStatsQuery.isLoading && !competitionStatsQuery.isError && competitionStatsQuery.data && (
                  competitionStatsQuery.data.data.length === 0 ? (
                    <EmptyState title="No hay competencias para estos filtros" description="Prueba con otro año o circuito." />
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="min-w-full text-left text-sm">
                        <thead className="bg-canvas text-content-muted">
                          <tr>
                            <th className="sticky left-0 z-20 min-w-[260px] border-r border-line bg-canvas px-4 py-3 font-semibold shadow-[4px_0_8px_-8px_rgba(15,23,42,0.45)]">Competencia</th>
                            <th className="min-w-[110px] px-4 py-3 text-center font-semibold">Fecha</th>
                            <th className="min-w-[130px] px-4 py-3 text-center font-semibold">Piscina</th>
                            <th className="px-4 py-3 text-center font-semibold">Participantes</th>
                            <th className="px-4 py-3 text-center font-semibold">Mujeres</th>
                            <th className="px-4 py-3 text-center font-semibold">Hombres</th>
                            <th className="px-4 py-3 text-center font-semibold">Clubes</th>
                            <th className="px-4 py-3 text-center font-semibold">Pruebas</th>
                            <th className="px-4 py-3 text-center font-semibold">Válidos</th>
                            <th className="px-4 py-3 text-center font-semibold">DQ</th>
                            <th className="px-4 py-3 text-center font-semibold">Entradas</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-line">
                          {competitionStatsQuery.data.data.map((competition) => (
                            <tr key={competition.id} className="hover:bg-canvas">
                              <td className="sticky left-0 z-10 border-r border-line bg-surface px-4 py-3 shadow-[4px_0_8px_-8px_rgba(15,23,42,0.45)]">
                                <Link to={`/competitions/${competition.id}`} className="font-semibold text-action hover:underline">
                                  {competition.name}
                                </Link>
                              </td>
                              <td className="px-4 py-3 text-center text-content-muted">
                                {competition.date ? new Date(`${competition.date}T12:00:00`).toLocaleDateString('es-CL') : 's/f'}
                              </td>
                              <td className="px-4 py-3 text-center text-content-muted">
                                {competition.course_type ? courseLabels[competition.course_type] || competition.course_type.toUpperCase() : 'Sin dato'}
                              </td>
                              <td className="px-4 py-3 text-center font-semibold text-ink">{competition.participants_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.women_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.men_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.clubs_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.events_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.valid_results_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.dsq_count}</td>
                              <td className="px-4 py-3 text-center text-content-muted">{competition.entries_count}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )
                )}
              </div>
            </section>
          )}
        </div>
      </div>
    </div>
  );
};
