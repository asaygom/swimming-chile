import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { athleteService } from '../api/athleteService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';
import { CourseBadge } from '../../../components/ui/CourseBadge';
import { getCourseMeta } from '../../../lib/courseMeta';
import type { CourseType } from '../../../lib/schemas/canon';
import type { AthleteResult } from '../../../lib/schemas/athlete';

const strokeTranslations: Record<string, string> = {
  freestyle: 'Libre',
  backstroke: 'Espalda',
  breaststroke: 'Pecho',
  butterfly: 'Mariposa',
  individual_medley: 'Combinado',
  medley_relay: 'Relevo Combinado',
  freestyle_relay: 'Relevo Libre',
};

const genderTranslations: Record<string, string> = {
  female: 'Dama',
  male: 'Varón',
};

type BestTimesCourseFilter = 'scm' | 'lcm' | 'all' ;

const courseFilterLabels: Record<BestTimesCourseFilter, string> = {
  scm: 'Piscina corta',
  lcm: 'Piscina larga',
  all: 'Ambas',
};

const formatMonthYear = (date?: string | null) => {
  if (!date) return null;

  const dateString = date.includes('T') ? date : `${date}T12:00:00`;
  const dateObj = new Date(dateString);
  if (Number.isNaN(dateObj.getTime())) return null;

  return dateObj.toLocaleDateString('es-CL', { month: 'short', year: 'numeric' });
};

const getCompetitionYear = (date?: string | null) => {
  const match = date?.match(/^(\d{4})/);
  return match?.[1] ?? null;
};

const ResultPositionBadge: React.FC<{ position?: number | null }> = ({ position }) => (
  <span
    className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-full text-sm font-bold ${
      position === 1 ? 'bg-warning/30 text-medal-gold' :
      position === 2 ? 'bg-canvas text-medal-silver' :
      position === 3 ? 'bg-warning/20 text-medal-bronze' :
      'border border-line bg-surface text-content-subtle'
    }`}
  >
    {position ? `${position}°` : '-'}
  </span>
);

const TimeComparison: React.FC<{ seedMs?: number | null; resultMs?: number | null }> = ({ seedMs, resultMs }) => {
  if (!seedMs || !resultMs) return null;

  const diffSeconds = (resultMs - seedMs) / 1000;
  if (diffSeconds === 0) {
    return <span className="text-xs font-semibold text-content-subtle">±0.00s</span>;
  }

  const improved = diffSeconds < 0;

  return (
    <span className={`text-xs font-bold ${improved ? 'text-success-strong' : 'text-danger-strong'}`}>
      {improved ? '' : '+'}{diffSeconds.toFixed(2)}s
    </span>
  );
};

type TrendPoint = {
  id: string | number;
  competition_name: string;
  competition_date?: string | null;
  course_type?: CourseType | null;
  result_time_text?: string | null;
  result_time_ms: number;
};

const PerformanceTrendChart: React.FC<{ points: TrendPoint[] }> = ({ points }) => {
  const containerRef = React.useRef<HTMLDivElement | null>(null);
  const [containerWidth, setContainerWidth] = React.useState(0);

  React.useEffect(() => {
    const node = containerRef.current;
    if (!node) return;

    const updateWidth = () => {
      setContainerWidth(Math.round(node.getBoundingClientRect().width));
    };

    updateWidth();

    const observer = new ResizeObserver(updateWidth);
    observer.observe(node);

    return () => observer.disconnect();
  }, []);

  if (points.length === 0) return null;

  const chartWidth = Math.max(containerWidth || 720, 280);
  const isCompact = chartWidth < 520;
  const chartHeight = isCompact ? 240 : 260;
  const padding = {
    top: 24,
    right: isCompact ? 12 : 24,
    bottom: isCompact ? 52 : 72,
    left: isCompact ? 52 : 64,
  };
  const showCompetitionNames = !isCompact;
  const plotWidth = chartWidth - padding.left - padding.right;
  const plotHeight = chartHeight - padding.top - padding.bottom;
  const times = points.map(point => point.result_time_ms);
  const minTime = Math.min(...times);
  const maxTime = Math.max(...times);
  const range = maxTime - minTime || 1000;
  const yMin = minTime - range * 0.08;
  const yMax = maxTime + range * 0.08;
  const xForIndex = (index: number) => padding.left + (points.length === 1 ? plotWidth / 2 : (index / (points.length - 1)) * plotWidth);
  const yForTime = (timeMs: number) => padding.top + ((yMax - timeMs) / (yMax - yMin)) * plotHeight;
  const yTicks = [yMin, (yMin + yMax) / 2, yMax];

  return (
    <div ref={containerRef} className="w-full">
      <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} width="100%" height={chartHeight}>
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={padding.top + plotHeight} stroke="var(--color-chart-axis)" />
        <line x1={padding.left} y1={padding.top + plotHeight} x2={padding.left + plotWidth} y2={padding.top + plotHeight} stroke="var(--color-chart-axis)" />
        {yTicks.map(tick => (
          <g key={tick}>
            <line x1={padding.left - 4} y1={yForTime(tick)} x2={padding.left + plotWidth} y2={yForTime(tick)} stroke="var(--color-chart-grid)" />
            <text x={padding.left - 10} y={yForTime(tick) + 4} textAnchor="end" className="fill-content-subtle text-[11px]">
              {(tick / 1000).toFixed(2)}s
            </text>
          </g>
        ))}
        {points.slice(1).map((point, index) => {
          const previous = points[index];
          const improved = point.result_time_ms < previous.result_time_ms;

          return (
            <line
              key={`${previous.id}-${point.id}`}
              x1={xForIndex(index)}
              y1={yForTime(previous.result_time_ms)}
              x2={xForIndex(index + 1)}
              y2={yForTime(point.result_time_ms)}
              stroke={improved ? 'var(--color-trend-improve)' : 'var(--color-trend-regress)'}
              strokeWidth="3"
              strokeDasharray="6 4"
              strokeLinecap="round"
            />
          );
        })}
        {points.map((point, index) => {
          const x = xForIndex(index);
          const y = yForTime(point.result_time_ms);
          const date = formatMonthYear(point.competition_date);
          const course = getCourseMeta(point.course_type);

          return (
            <g key={point.id}>
              <circle
                cx={x}
                cy={y}
                r="5"
                fill={point.course_type === 'lcm' ? 'var(--color-course-lcm)' : 'var(--color-course-scm)'}
                stroke="var(--color-surface)"
                strokeWidth="2"
              >
                <title>{`${point.competition_name}: ${point.result_time_text || `${(point.result_time_ms / 1000).toFixed(2)}s`} · ${course.description}`}</title>
              </circle>
              <text x={x} y={y - 10} textAnchor="middle" className="fill-content-muted text-[11px] font-semibold">
                {point.result_time_text}
              </text>
              <text x={x} y={padding.top + plotHeight + 20} textAnchor="middle" className="fill-content-subtle text-[10px]">
                {date || `Registro ${index + 1}`}
              </text>
              {showCompetitionNames && (
                <text x={x} y={padding.top + plotHeight + 36} textAnchor="middle" className="fill-content-subtle text-[10px]">
                  {point.competition_name.length > 16 ? `${point.competition_name.slice(0, 16)}…` : point.competition_name}
                </text>
              )}
            </g>
          );
        })}
        <g transform={`translate(${padding.left}, ${chartHeight - 10})`}>
          <circle cx="0" cy="0" r="4" fill="var(--color-course-scm)" />
          <text x="10" y="4" className="fill-content-subtle text-[11px]">Piscina corta</text>
          <circle cx="110" cy="0" r="4" fill="var(--color-course-lcm)" />
          <text x="120" y="4" className="fill-content-subtle text-[11px]">Piscina larga</text>
        </g>
      </svg>
    </div>
  );
};

const AthleteResultHistory: React.FC<{ results: AthleteResult[] }> = ({ results }) => {
  const [historyYearSelection, setHistoryYearSelection] = React.useState<string | null>(null);
  const [isExpanded, setIsExpanded] = React.useState(false);
  const availableYears = React.useMemo(() => {
    const years = results
      .map(result => getCompetitionYear(result.competition_date))
      .filter((year): year is string => Boolean(year));

    return Array.from(new Set(years)).sort((left, right) => Number(right) - Number(left));
  }, [results]);
  const currentYear = new Date().getFullYear().toString();
  const defaultYear = availableYears.includes(currentYear)
    ? currentYear
    : availableYears[0] ?? 'all';
  const selectedYear = historyYearSelection === 'all' || availableYears.includes(historyYearSelection ?? '')
    ? historyYearSelection!
    : defaultYear;
  const competitionHistory = React.useMemo(() => {
    const grouped = new Map<string, { key: string; name: string; results: AthleteResult[] }>();

    results
      .filter(result => (
        selectedYear === 'all'
        || getCompetitionYear(result.competition_date) === selectedYear
      ))
      .forEach(result => {
        const key = `${result.competition_name}::${result.competition_date ?? ''}`;
        const group = grouped.get(key) ?? { key, name: result.competition_name, results: [] };
        group.results.push(result);
        grouped.set(key, group);
      });

    return Array.from(grouped.values());
  }, [results, selectedYear]);
  const visibleCompetitionHistory = isExpanded
    ? competitionHistory
    : competitionHistory.slice(0, 10);

  return (
    <div>
      <div className="mb-4 mt-8 flex flex-col gap-3 px-1 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-xl font-bold text-ink">Historial de Resultados</h2>
        {results.length > 0 && (
          <label className="flex items-center gap-2 text-sm font-medium text-content-muted">
            <span>Año</span>
            <select
              value={selectedYear}
              onChange={event => {
                setHistoryYearSelection(event.target.value);
                setIsExpanded(false);
              }}
              className="rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink shadow-sm focus:border-action focus:outline-none focus:ring-2 focus:ring-action/20"
            >
              <option value="all">Todos</option>
              {availableYears.map(year => (
                <option key={year} value={year}>{year}</option>
              ))}
            </select>
          </label>
        )}
      </div>

      {results.length === 0 ? (
        <EmptyState title="Sin resultados" description="Este atleta no tiene tiempos registrados aún." />
      ) : (
        <div id="athlete-result-history" className="space-y-6">
          {visibleCompetitionHistory.map(({ key, name: competitionName, results: competitionResults }) => {
            const competitionMonthYear = formatMonthYear(competitionResults[0]?.competition_date);
            const representedClubs = Array.from(new Set(
              competitionResults
                .map(result => result.club_name?.trim())
                .filter((clubName): clubName is string => Boolean(clubName)),
            ));

            return (
              <div key={key} className="overflow-hidden rounded-xl border border-line bg-surface shadow-sm">
                <div className="border-b border-line bg-canvas px-4 py-3">
                  <h3 className="flex flex-wrap items-baseline gap-x-2 gap-y-1 font-bold text-ink">
                    {competitionName}
                    {competitionMonthYear && <span className="text-sm font-medium text-content-subtle">({competitionMonthYear})</span>}
                    {representedClubs.length > 0 && (
                      <span className="text-sm font-medium text-content-muted">
                        · {representedClubs.join(' / ')}
                      </span>
                    )}
                  </h3>
                </div>
                <div className="divide-y divide-line">
                  {competitionResults.map(result => (
                    <div key={result.id} className="flex flex-col justify-between gap-2 px-4 py-3 transition-colors hover:bg-canvas/50 sm:flex-row sm:items-center">
                      <div className="flex items-center gap-3">
                        <ResultPositionBadge position={result.rank_position} />
                        <div>
                          <div className="font-semibold text-ink">
                            {result.distance_m}m {result.stroke ? strokeTranslations[result.stroke] : 'Estilo no informado'}
                          </div>
                          <div className="flex items-center gap-2 text-xs uppercase text-content-subtle">
                            <CourseBadge courseType={result.course_type} variant="compact" />
                            {result.age_group && (
                              <>
                                <span className="h-1 w-1 rounded-full bg-chart-axis"></span>
                                <span className="tracking-wide">Cat: {result.age_group}</span>
                              </>
                            )}
                          </div>
                        </div>
                      </div>

                      <div className="mt-2 flex w-full items-center justify-between border-t border-line pt-2 sm:mt-0 sm:w-auto sm:flex-col sm:items-end sm:justify-center sm:border-t-0 sm:pt-0">
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-semibold text-ink">{result.result_time_text}</span>
                          <TimeComparison seedMs={result.seed_time_ms} resultMs={result.result_time_ms} />
                          {result.status !== 'valid' && (
                            <span className="rounded bg-danger/15 px-1.5 py-0.5 text-xs font-bold text-danger-strong">{result.status}</span>
                          )}
                        </div>
                        {result.seed_time_text && (
                          <div className="mt-1 text-xs text-content-subtle">
                            Seed {result.seed_time_text}
                          </div>
                        )}
                        {result.points && (
                          <div className="mt-1 text-xs text-content-subtle">
                            <span className="font-semibold text-success-strong">{result.points}</span> pts
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}

          {competitionHistory.length > 10 && (
            <div className="text-center">
              <button
                type="button"
                onClick={() => setIsExpanded(current => !current)}
                aria-expanded={isExpanded}
                aria-controls="athlete-result-history"
                className="rounded-lg border border-line bg-surface px-4 py-2 text-sm font-semibold text-action transition-colors hover:bg-canvas hover:text-brand-steel"
              >
                {isExpanded ? 'Ver menos' : 'Ver más'}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export const AthleteProfilePage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [courseFilter, setCourseFilter] = React.useState<BestTimesCourseFilter>('all');
  const [trendSelection, setTrendSelection] = React.useState('');

  const { data: athlete, isLoading, isError, refetch } = useQuery({
    queryKey: ['athlete', id],
    queryFn: () => athleteService.getAthleteProfile(id!),
    enabled: !!id,
  });

  const pbs = React.useMemo(() => {
    if (!athlete?.recent_results) return [];
    
    // PBs
    const bests = new Map<string, typeof athlete.recent_results[0]>();
    athlete.recent_results.forEach(res => {
      if (res.status !== 'valid' || !res.result_time_ms) return;
      const key = `${res.distance_m}-${res.stroke}-${res.course_type}`;
      if (!bests.has(key) || res.result_time_ms < bests.get(key)!.result_time_ms!) {
        bests.set(key, res);
      }
    });
    
    const pbArray = Array.from(bests.values()).sort((a, b) => {
      if (a.stroke !== b.stroke) return (a.stroke || '').localeCompare(b.stroke || '');
      if (a.distance_m !== b.distance_m) return (a.distance_m || 0) - (b.distance_m || 0);
      return (a.course_type || '').localeCompare(b.course_type || '');
    });

    return pbArray;
  }, [athlete]);

  const availablePoolFilters = React.useMemo(
    () => new Set(pbs.map(res => res.course_type).filter((course): course is 'scm' | 'lcm' => course === 'scm' || course === 'lcm')),
    [pbs],
  );

  const filteredPbs = React.useMemo(
    () => courseFilter === 'all' ? pbs : pbs.filter(res => res.course_type === courseFilter),
    [pbs, courseFilter],
  );

  const trendOptions = React.useMemo(() => {
    if (!athlete?.recent_results) return [];

    const options = new Map<string, { key: string; label: string }>();
    athlete.recent_results.forEach(result => {
      if (result.status !== 'valid' || !result.result_time_ms || !result.distance_m || !result.stroke) return;
      const key = `${result.distance_m}-${result.stroke}`;
      options.set(key, {
        key,
        label: `${result.distance_m}m ${strokeTranslations[result.stroke]}`,
      });
    });

    return Array.from(options.values()).sort((a, b) => a.label.localeCompare(b.label));
  }, [athlete]);

  const selectedTrendKey = trendOptions.some(option => option.key === trendSelection)
    ? trendSelection
    : trendOptions[0]?.key || '';

  const trendPoints = React.useMemo(() => {
    if (!athlete?.recent_results || !selectedTrendKey) return [];

    const [distance, stroke] = selectedTrendKey.split('-');
    return athlete.recent_results
      .map((result, index) => ({ result, index }))
      .filter(({ result }) => (
        result.status === 'valid' &&
        result.result_time_ms &&
        result.distance_m?.toString() === distance &&
        result.stroke === stroke
      ))
      .sort((a, b) => {
        const leftDate = a.result.competition_date ? new Date(`${a.result.competition_date}T12:00:00`).getTime() : 0;
        const rightDate = b.result.competition_date ? new Date(`${b.result.competition_date}T12:00:00`).getTime() : 0;
        return rightDate - leftDate || a.index - b.index;
      })
      .slice(0, 5)
      .reverse()
      .map(({ result }) => ({
        id: result.id,
        competition_name: result.competition_name,
        competition_date: result.competition_date,
        course_type: result.course_type,
        result_time_text: result.result_time_text,
        result_time_ms: result.result_time_ms!,
      }));
  }, [athlete, selectedTrendKey]);

  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState onRetry={() => refetch()} />;
  if (!athlete) return <EmptyState title="Atleta no encontrado" />;



  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      {/* Breadcrumb / Back button */}
      <div className="mb-6">
        <button 
          onClick={() => navigate(-1)}
          className="text-sm font-medium text-action hover:text-brand-steel flex items-center gap-1 cursor-pointer bg-transparent border-none p-0"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Volver atrás
        </button>
      </div>

      {/* Header Profile */}
      <div className="bg-surface rounded-xl shadow-sm border border-line p-6 md:p-8">
        <div className="flex flex-col md:flex-row gap-6 items-start md:items-center">
          <div className="w-20 h-20 bg-brand-navy rounded-full flex items-center justify-center text-brand-muted shadow-inner flex-shrink-0">
            <svg className="w-10 h-10" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
            </svg>
          </div>
          <div>
            <h1 className="text-3xl font-bold text-ink tracking-tight">{athlete.full_name}</h1>
            <div className="mt-2 flex flex-wrap gap-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-brand-cyan text-brand-night border border-brand-cyan capitalize">
                {athlete.gender ? genderTranslations[athlete.gender] : 'Sin género'}
              </span>
              {athlete.birth_year && (
                <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-canvas text-content-muted border border-line">
                  Año de nacimiento: {athlete.birth_year}
                </span>
              )}
              {(athlete.current_club_name || athlete.club_name) && (
                <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-success/20 text-success-strong border border-success">
                  Club vigente: {athlete.current_club_name || athlete.club_name}
                </span>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Mejores Tiempos */}
      {pbs.length > 0 && (
        <div>
          <div className="mb-4 flex flex-col gap-3 px-1 sm:flex-row sm:items-center sm:justify-between">
            <h2 className="text-xl font-bold text-ink">Mejores Tiempos</h2>
            <div className="flex flex-wrap gap-2">
              {(['scm', 'lcm', 'all'] as const).map(course => {
                const isActive = courseFilter === course;
                const isDisabled = course !== 'all' && !availablePoolFilters.has(course);
                const activeClass = course === 'scm'
                  ? 'border-course-scm bg-course-scm text-brand-night'
                  : course === 'lcm'
                    ? 'border-course-lcm bg-course-lcm text-brand-white'
                    : 'border-brand-night bg-brand-night text-brand-white';

                return (
                  <button
                    key={course}
                    type="button"
                    disabled={isDisabled}
                    onClick={() => setCourseFilter(course)}
                    className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-bold uppercase tracking-wider transition-colors ${
                      isActive ? activeClass : 'border-line bg-surface text-content-subtle hover:bg-canvas'
                    } ${isDisabled ? 'cursor-not-allowed opacity-40' : ''}`}
                  >
                    {courseFilterLabels[course]}
                  </button>
                );
              })}
            </div>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {filteredPbs.map(res => {
              const achievedAt = formatMonthYear(res.competition_date);

              return (
              <div key={res.id} className="bg-surface rounded-xl shadow-sm border border-line p-4 flex items-center justify-between">
                <div>
                  <div className="font-bold text-ink">{res.distance_m}m {res.stroke ? strokeTranslations[res.stroke] : 'Estilo no informado'}</div>
                  <div className="text-xs text-content-subtle uppercase flex items-center gap-2 mt-0.5 tracking-wider">
                    <CourseBadge courseType={res.course_type} variant="compact" />
                    {res.age_group && (
                      <>
                        <span className="w-1 h-1 rounded-full bg-chart-axis"></span>
                        <span className="tracking-wide">Cat: {res.age_group}</span>
                      </>
                    )}
                  </div>
                </div>
                <div className="text-right">
                  <div className="font-mono text-action font-bold text-lg">{res.result_time_text}</div>
                  <div className="text-xs text-content-subtle truncate max-w-[150px]" title={res.competition_name}>
                    {achievedAt && <span className="ml-1 text-content-subtle">({achievedAt}) </span>}
                    {res.competition_name}
                  </div>
                </div>
              </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Evolución de Tiempos */}
      {trendOptions.length > 0 && (
        <div>
          <div className="mb-4 flex flex-col gap-3 px-1 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-xl font-bold text-ink">Evolución de Tiempos</h2>
              <p className="text-sm text-content-subtle">
                Últimos 5 registros ordenados de más antiguo a más reciente.
              </p>
            </div>
            <select
              value={selectedTrendKey}
              onChange={(event) => setTrendSelection(event.target.value)}
              className="w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-content-muted shadow-sm focus:ring-2 focus:ring-action sm:w-72"
            >
              {trendOptions.map(option => (
                <option key={option.key} value={option.key}>{option.label}</option>
              ))}
            </select>
          </div>
          <div className="rounded-xl border border-line bg-surface p-4 shadow-sm">
            {trendPoints.length > 0 ? (
              <PerformanceTrendChart points={trendPoints} />
            ) : (
              <EmptyState title="Sin registros suficientes" description="No hay tiempos válidos para esta selección." />
            )}
          </div>
        </div>
      )}

      <AthleteResultHistory key={id} results={athlete.recent_results ?? []} />
    </div>
  );
};
