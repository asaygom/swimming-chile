import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { athleteService } from '../api/athleteService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';

export const AthletesPage: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [debouncedQuery, setDebouncedQuery] = useState('');
  const [genderFilter, setGenderFilter] = useState('all');
  const [page, setPage] = useState(1);
  const hasActiveFilters = searchTerm.trim() !== '' || genderFilter !== 'all';

  const clearFilters = () => {
    setSearchTerm('');
    setDebouncedQuery('');
    setGenderFilter('all');
    setPage(1);
  };

  // Sincronización simple de debouncing para no saturar llamadas
  React.useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedQuery(searchTerm);
      setPage(1); // Resetear página a 1 cada vez que se busca algo nuevo
    }, 400);
    return () => clearTimeout(handler);
  }, [searchTerm]);

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['athletes', debouncedQuery, genderFilter, page],
    queryFn: () => athleteService.searchAthletes({ query: debouncedQuery, gender: genderFilter, page }),
  });

  return (
    <div className="space-y-6">
      {/* Header & Search */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-ink tracking-tight">Atletas</h1>
          <p className="text-content-subtle mt-1">Busca nadadores y revisa su historial competitivo.</p>
        </div>
        <div className="flex flex-col sm:flex-row w-full md:w-auto gap-3">
          <div className="relative w-full sm:w-80">
            <input
              type="text"
              placeholder="Buscar por nombre (ej. Perez, Juan)..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-line rounded-lg shadow-sm focus:ring-2 focus:ring-brand-pool focus:border-brand-pool transition-shadow outline-none bg-surface"
            />
            <svg className="w-5 h-5 text-content-subtle absolute left-3 top-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          <select 
            value={genderFilter}
            onChange={(e) => { setGenderFilter(e.target.value); setPage(1); }}
            className="w-full sm:w-40 px-4 py-2 border border-line rounded-lg shadow-sm focus:ring-2 focus:ring-brand-pool focus:border-brand-pool outline-none bg-surface text-content-muted"
          >
            <option value="all">Ambos géneros</option>
            <option value="female">Femenino</option>
            <option value="male">Masculino</option>
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

      {/* States */}
      {isLoading && <LoadingState />}
      {isError && <ErrorState onRetry={() => refetch()} />}
      
      {/* List */}
      {!isLoading && !isError && data && (
        <>
          {data.data.length === 0 ? (
            <EmptyState 
              title="No se encontraron atletas" 
              description={`No hay coincidencias para "${debouncedQuery}". Intenta con otro nombre.`} 
            />
          ) : (
            <div className="space-y-4">
              <div className="bg-surface rounded-xl shadow-sm border border-line overflow-hidden">
                <ul className="divide-y divide-line">
                  {data.data.map((athlete) => {
                    const clubName = athlete.current_club_name || athlete.club_name;

                    return (
                      <li key={athlete.id} className="hover:bg-canvas transition-colors">
                        <Link to={`/athletes/${athlete.id}`} className="block p-4 sm:px-6">
                          <div className="flex items-center justify-between gap-3">
                            <div className="min-w-0 flex flex-col">
                              <p className="truncate text-sm font-semibold text-action">{athlete.full_name}</p>
                              <p className="mt-1 flex items-center gap-2 text-sm text-content-subtle">
                                <span className="capitalize">{athlete.gender}</span>
                                {athlete.birth_year && (
                                  <>
                                    <span>&bull;</span>
                                    <span>Nacido en {athlete.birth_year}</span>
                                  </>
                                )}
                              </p>
                              {clubName && (
                                <p className="mt-1 truncate text-xs font-medium text-content-subtle md:hidden">
                                  {clubName}
                                </p>
                              )}
                            </div>
                            <div className="flex shrink-0 items-center gap-4">
                              {clubName && (
                                <span className="hidden items-center rounded-full bg-canvas px-2.5 py-0.5 text-xs font-medium text-ink md:inline-flex">
                                  {clubName}
                                </span>
                              )}
                              <svg className="w-5 h-5 text-content-subtle" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                              </svg>
                            </div>
                          </div>
                        </Link>
                      </li>
                    );
                  })}
                </ul>
              </div>

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
