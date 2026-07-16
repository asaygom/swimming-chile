import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { competitionService } from '../api/competitionService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';
import { CourseBadge } from '../../../components/ui/CourseBadge';
import { getCourseMeta } from '../../../lib/courseMeta';
import type { Competition } from '../../../lib/schemas/competition';

const GoverningBodyBadge: React.FC<{ competition: Competition }> = ({ competition }) => {
  const governingBodyCode = competition.governing_body_code?.trim().toLowerCase();
  const label = competition.governing_body_code?.trim().toUpperCase()
    || competition.governing_body_name?.trim()
    || competition.organizer?.trim();

  if (!label) return null;

  const colorClass = governingBodyCode === 'fchmn'
    ? 'border-action/30 bg-action/10 text-action'
    : governingBodyCode === 'fechida'
      ? 'border-danger/30 bg-danger/10 text-danger-strong'
      : 'border-line bg-canvas text-content-muted';

  return (
    <span className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold ${colorClass}`}>
      {label}
    </span>
  );
};

const CompetitionCard: React.FC<{ comp: Competition; isUpcoming?: boolean }> = ({ comp, isUpcoming = false }) => {
  // Ajuste para evitar bugs de zona horaria: agregar T00:00:00 si viene solo YYYY-MM-DD
  const dateString = comp.date_start.includes('T') ? comp.date_start : `${comp.date_start}T12:00:00`;
  const dateObj = new Date(dateString);
  const month = dateObj.toLocaleDateString('es-CL', { month: 'short' }).toUpperCase();
  const day = dateObj.getDate();
  const yearStr = dateObj.getFullYear();
  const course = getCourseMeta(comp.course_type);
  
  const innerContent = (
    <>
      <div className="flex bg-canvas border-b border-line">
        <div className="w-20 bg-brand-cyan text-ink flex flex-col items-center justify-center p-3 text-center">
          <span className="text-xs font-bold tracking-widest">{month}</span>
          <span className="text-2xl font-black leading-none my-1">{day}</span>
          <span className="text-xs opacity-80">{yearStr}</span>
        </div>
        <div className="p-4 flex-1 flex flex-col justify-center">
          <h3 className={`text-lg font-bold text-ink leading-tight transition-colors ${!isUpcoming ? 'group-hover:text-action' : ''}`}>
            {comp.name}
          </h3>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <CourseBadge courseType={comp.course_type} />
            <GoverningBodyBadge competition={comp} />
          </div>
        </div>
      </div>

      <div className="p-5 flex-1 flex flex-col justify-between">
        <div className="space-y-3 mb-6">
          <div className="flex items-start gap-2 text-content-muted text-sm">
            <svg className="w-4 h-4 mt-0.5 text-content-subtle shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            <span>{comp.location || 'Sede por confirmar'}</span>
          </div>
          <div className="flex items-center gap-2 text-content-muted text-sm">
            <svg className="w-4 h-4 text-content-subtle shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            <span>{course.description}</span>
          </div>
        </div>

        <div className="flex justify-end mt-auto pt-4 border-t border-line">
          {isUpcoming ? (
            <span className="text-content-subtle text-sm font-medium flex items-center gap-1">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
              Calendario
            </span>
          ) : (
            <span className="text-action text-sm font-semibold flex items-center gap-1 group-hover:translate-x-1 transition-transform">
              Ver Resultados
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </span>
          )}
        </div>
      </div>
    </>
  );

  if (isUpcoming) {
    return (
      <div className="group bg-surface rounded-2xl shadow-sm border border-line overflow-hidden flex flex-col h-full opacity-90 transition-all">
        {innerContent}
      </div>
    );
  }

  return (
    <Link 
      to={`/competitions/${comp.id}`}
      className="group bg-surface rounded-2xl shadow-sm border border-line overflow-hidden flex flex-col h-full transition-all hover:shadow-md hover:border-brand-pool cursor-pointer"
    >
      {innerContent}
    </Link>
  );
};

// CompetitionGroup ha sido eliminado en favor de una grilla mensual y listado directo

export const CompetitionsPage: React.FC<{ mode: 'upcoming' | 'past' }> = ({ mode }) => {
  const [searchTerm, setSearchTerm] = React.useState('');
  const [debouncedQuery, setDebouncedQuery] = React.useState('');
  const [year, setYear] = React.useState('all');
  const [governingBody, setGoverningBody] = React.useState('all');
  const [page, setPage] = React.useState(1);
  const hasActiveFilters = searchTerm.trim() !== '' || year !== 'all' || governingBody !== 'all';

  const clearFilters = () => {
    setSearchTerm('');
    setDebouncedQuery('');
    setYear('all');
    setGoverningBody('all');
    setPage(1);
  };

  React.useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedQuery(searchTerm);
      setPage(1);
    }, 400);
    return () => clearTimeout(handler);
  }, [searchTerm]);

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['competitions', debouncedQuery, year, governingBody, page, mode],
    queryFn: () => competitionService.getCompetitions(debouncedQuery, year, page, mode, 'all', governingBody),
  });

  const { data: filterOptions } = useQuery({
    queryKey: ['competition-filter-options', mode],
    queryFn: () => competitionService.getCompetitionFilterOptions(mode),
  });

  const availableYears = filterOptions?.years || [];
  const governingBodyOptions = filterOptions?.governing_bodies || [];

  // Filtrado local removido, el backend ahora filtra por date_start
  const competitionsList = React.useMemo(() => {
    if (!data) return [];
    return data.data;
  }, [data]);

  const groupedByMonth = React.useMemo(() => {
    if (mode !== 'upcoming' || !competitionsList) return {};
    return competitionsList.reduce((acc, comp) => {
      const d = new Date(comp.date_start.includes('T') ? comp.date_start : `${comp.date_start}T12:00:00`);
      const monthYear = d.toLocaleDateString('es-CL', { month: 'long', year: 'numeric' });
      const key = monthYear.charAt(0).toUpperCase() + monthYear.slice(1);
      if (!acc[key]) acc[key] = [];
      acc[key].push(comp);
      return acc;
    }, {} as Record<string, typeof competitionsList>);
  }, [competitionsList, mode]);

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {/* Header & Search */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-ink tracking-tight">
            {mode === 'upcoming' ? 'Calendario' : 'Resultados de Competencias'}
          </h1>
          <p className="text-content-subtle mt-1">
            {mode === 'upcoming' 
              ? 'Revisa las próximas competencias programadas.' 
              : 'Explora las competencias y sus resultados detallados.'}
          </p>
        </div>

        <div className="flex flex-col sm:flex-row gap-3 w-full lg:w-auto">
          <div className="relative flex-1 lg:w-64">
            <input
              type="text"
              className="w-full pl-10 pr-4 py-2 border border-line rounded-lg shadow-sm focus:ring-2 focus:ring-brand-pool focus:border-brand-pool transition-shadow outline-none bg-surface text-sm"
              placeholder="Buscar torneo o sede..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
            <svg className="w-5 h-5 text-content-subtle absolute left-3 top-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          
          <div className="flex gap-3">
            {mode === 'past' && (
              <select 
                value={year} 
                onChange={(e) => {
                  setYear(e.target.value);
                  setPage(1);
                }}
                className="flex-1 sm:w-28 py-2 pl-3 pr-8 border border-line bg-surface rounded-lg shadow-sm focus:ring-2 focus:ring-brand-pool focus:border-brand-pool text-sm"
              >
                <option value="all">Año</option>
                {availableYears.map(y => (
                  <option key={y} value={y.toString()}>{y}</option>
                ))}
              </select>
            )}

            <select 
              value={governingBody} 
              onChange={(e) => {
                setGoverningBody(e.target.value);
                setPage(1);
              }}
              className="flex-1 sm:w-32 py-2 pl-3 pr-8 border border-line bg-surface rounded-lg shadow-sm focus:ring-2 focus:ring-brand-pool focus:border-brand-pool text-sm font-medium"
            >
              <option value="all">Todos</option>
              {governingBodyOptions.map((option) => (
                <option key={option.governing_body_code} value={option.governing_body_code}>
                  {option.governing_body_name || option.governing_body_code.toUpperCase()}
                </option>
              ))}
            </select>
            {hasActiveFilters && (
              <button
                type="button"
                onClick={clearFilters}
                className="whitespace-nowrap rounded-lg border border-line bg-surface px-3 py-2 text-sm font-medium text-content-muted shadow-sm transition-colors hover:bg-canvas hover:text-ink"
              >
                Limpiar
              </button>
            )}
          </div>
        </div>
      </div>

      {isLoading && <LoadingState />}
      {isError && <ErrorState onRetry={() => refetch()} />}
      
      {!isLoading && !isError && data && (
        <>
          {data.data.length === 0 ? (
            <EmptyState title="No se encontraron competencias" description="Modifica los filtros para ver más resultados." />
          ) : (
            <div className="space-y-10">
              {mode === 'upcoming' ? (
                <div className="space-y-12">
                  {Object.entries(groupedByMonth).map(([month, comps]) => (
                    <div key={month} className="space-y-6">
                      <h2 className="text-2xl font-bold text-ink border-b border-line pb-2">{month}</h2>
                      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                        {(comps as Competition[]).map((comp) => (
                          <CompetitionCard key={comp.id} comp={comp} isUpcoming={true} />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
                  {competitionsList.map((comp) => (
                    <CompetitionCard key={comp.id} comp={comp} isUpcoming={false} />
                  ))}
                </div>
              )}

              {/* Paginación */}
              <div className="flex items-center justify-between border-t border-line pt-4">
                <p className="text-sm text-content-subtle">
                  Mostrando página {data.meta.page} de {data.meta.total_pages} ({data.meta.total_results} resultados)
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    disabled={data.meta.page === 1}
                    className="px-4 py-2 border border-line rounded-lg text-sm font-medium text-content-muted bg-surface hover:bg-canvas disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    Anterior
                  </button>
                  <button
                    onClick={() => setPage((p) => Math.min(data.meta.total_pages, p + 1))}
                    disabled={data.meta.page >= data.meta.total_pages}
                    className="px-4 py-2 border border-line rounded-lg text-sm font-medium text-content-muted bg-surface hover:bg-canvas disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    Siguiente
                  </button>
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};
