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

  const { pbs, groupedRecent } = React.useMemo(() => {
    if (!athlete || !athlete.recent_results) return { pbs: [], groupedRecent: {} };
    
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

    // Grouping by Competition
    const grouped = athlete.recent_results.reduce((acc, res) => {
      if (!acc[res.competition_name]) acc[res.competition_name] = [];
      acc[res.competition_name].push(res);
      return acc;
    }, {} as Record<string, typeof athlete.recent_results>);
    
    return { pbs: pbArray, groupedRecent: grouped };
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

      {/* Recent Results */}
      <div>
        <h2 className="text-xl font-bold text-ink mb-4 px-1 mt-8">Historial de Resultados</h2>
        
        {!athlete.recent_results || athlete.recent_results.length === 0 ? (
          <EmptyState title="Sin resultados" description="Este atleta no tiene tiempos registrados aún." />
        ) : (
          <div className="space-y-6">
            {Object.entries(groupedRecent).map(([compName, results]) => {
              const competitionMonthYear = formatMonthYear(results[0]?.competition_date);
              const representedClubs = Array.from(new Set(
                results
                  .map(result => result.club_name?.trim())
                  .filter((clubName): clubName is string => Boolean(clubName)),
              ));

              return (
              <div key={compName} className="bg-surface rounded-xl shadow-sm border border-line overflow-hidden">
                <div className="bg-canvas border-b border-line px-4 py-3">
                  <h3 className="flex flex-wrap items-baseline gap-x-2 gap-y-1 font-bold text-ink">
                    {compName}
                    {competitionMonthYear && <span className="text-sm font-medium text-content-subtle">({competitionMonthYear})</span>}
                    {representedClubs.length > 0 && (
                      <span className="text-sm font-medium text-content-muted">
                        · {representedClubs.join(' / ')}
                      </span>
                    )}
                  </h3>
                </div>
                <div className="divide-y divide-line">
                  {results.map(res => (
                    <div key={res.id} className="px-4 py-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2 hover:bg-canvas/50 transition-colors">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-lg bg-brand-cyan flex items-center justify-center text-brand-night font-bold text-sm shrink-0">
                          {res.rank_position ? `${res.rank_position}°` : '-'}
                        </div>
                        <div>
                          <div className="font-semibold text-ink">{res.distance_m}m {res.stroke ? strokeTranslations[res.stroke] : 'Estilo no informado'}</div>
                          <div className="text-xs text-content-subtle uppercase flex items-center gap-2">
                            <CourseBadge courseType={res.course_type} variant="compact" />
                            {res.age_group && (
                              <>
                                <span className="w-1 h-1 rounded-full bg-chart-axis"></span>
                                <span className="tracking-wide">Cat: {res.age_group}</span>
                              </>
                            )}
                          </div>
                        </div>
                      </div>
                      
                      <div className="flex sm:flex-col items-center sm:items-end justify-between sm:justify-center w-full sm:w-auto mt-2 sm:mt-0 pt-2 sm:pt-0 border-t sm:border-t-0 border-line">
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-ink font-semibold">{res.result_time_text}</span>
                          <TimeComparison seedMs={res.seed_time_ms} resultMs={res.result_time_ms} />
                          {res.status !== 'valid' && (
                            <span className="text-xs font-bold text-danger-strong bg-danger/15 px-1.5 py-0.5 rounded">{res.status}</span>
                          )}
                        </div>
                        {res.seed_time_text && (
                          <div className="text-xs text-content-subtle mt-1">
                            Seed {res.seed_time_text}
                          </div>
                        )}
                        {res.points && (
                          <div className="text-xs text-content-subtle mt-1">
                            <span className="font-semibold text-success-strong">{res.points}</span> pts
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};
