import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { competitionService } from '../api/competitionService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';

export const CompetitionsPage: React.FC = () => {
  const [query, setQuery] = React.useState('');
  const [year, setYear] = React.useState('all');
  const [circuit, setCircuit] = React.useState('FCHMN'); // Default FCHMN

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['competitions'],
    queryFn: () => competitionService.getCompetitions(),
  });

  const filteredComps = data?.data.filter(comp => {
    const matchQuery = comp.name.toLowerCase().includes(query.toLowerCase()) || 
                      (comp.location && comp.location.toLowerCase().includes(query.toLowerCase()));
    
    const compYear = new Date(comp.date_start).getFullYear().toString();
    const matchYear = year === 'all' || compYear === year;

    // TODO: En el futuro el esquema traerá 'circuit'. Por ahora asumimos todo FCHMN en la UI local.
    const matchCircuit = circuit === 'all' || circuit === 'FCHMN';

    return matchQuery && matchYear && matchCircuit;
  });

  // Extraer años únicos para el selector
  const availableYears = React.useMemo(() => {
    if (!data) return [];
    const years = data.data.map(c => new Date(c.date_start).getFullYear());
    return Array.from(new Set(years)).sort((a, b) => b - a);
  }, [data]);

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {/* Header & Search */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Competencias</h1>
          <p className="text-slate-500 mt-1">Explora las competencias y sus resultados detallados.</p>
        </div>

        <div className="flex flex-col sm:flex-row gap-3 w-full lg:w-auto">
          <div className="relative flex-1 lg:w-64">
            <input
              type="text"
              className="w-full pl-10 pr-4 py-2 border border-slate-300 rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-shadow outline-none bg-white text-sm"
              placeholder="Buscar torneo o sede..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <svg className="w-5 h-5 text-slate-400 absolute left-3 top-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          
          <div className="flex gap-3">
            <select 
              value={year} 
              onChange={(e) => setYear(e.target.value)}
              className="flex-1 sm:w-28 py-2 pl-3 pr-8 border border-slate-300 bg-white rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm"
            >
              <option value="all">Año</option>
              {availableYears.map(y => (
                <option key={y} value={y.toString()}>{y}</option>
              ))}
            </select>

            <select 
              value={circuit} 
              onChange={(e) => setCircuit(e.target.value)}
              className="flex-1 sm:w-32 py-2 pl-3 pr-8 border border-slate-300 bg-white rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm font-medium"
            >
              <option value="FCHMN">FCHMN</option>
              <option value="FECHIDA">FECHIDA</option>
              <option value="all">Todos</option>
            </select>
          </div>
        </div>
      </div>

      {isLoading && <LoadingState />}
      {isError && <ErrorState onRetry={() => refetch()} />}
      
      {!isLoading && !isError && filteredComps && (
        <>
          {filteredComps.length === 0 ? (
            <EmptyState title="No se encontraron competencias" description="Modifica los filtros para ver más resultados." />
          ) : (
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {filteredComps.map((comp) => {
                const dateObj = new Date(comp.date_start);
                const month = dateObj.toLocaleDateString('es-CL', { month: 'short' }).toUpperCase();
                const day = dateObj.getDate();
                const yearStr = dateObj.getFullYear();
                
                return (
                  <Link 
                    key={comp.id} 
                    to={`/competitions/${comp.id}`}
                    className="group bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden hover:shadow-md hover:border-blue-300 transition-all flex flex-col h-full"
                  >
                    {/* Tarjeta Tipo Calendario */}
                    <div className="flex bg-slate-50 border-b border-slate-100">
                      {/* Fecha "Tear-off Calendar" */}
                      <div className="w-20 bg-blue-600 text-white flex flex-col items-center justify-center p-3 text-center">
                        <span className="text-xs font-bold tracking-widest">{month}</span>
                        <span className="text-2xl font-black leading-none my-1">{day}</span>
                        <span className="text-xs opacity-80">{yearStr}</span>
                      </div>
                      
                      <div className="p-4 flex-1 flex flex-col justify-center">
                        <h3 className="text-lg font-bold text-slate-900 leading-tight group-hover:text-blue-700 transition-colors">
                          {comp.name}
                        </h3>
                      </div>
                    </div>

                    <div className="p-5 flex-1 flex flex-col justify-between">
                      <div className="space-y-3 mb-6">
                        <div className="flex items-start gap-2 text-slate-600 text-sm">
                          <svg className="w-4 h-4 mt-0.5 text-slate-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                          </svg>
                          <span>{comp.location || 'Sede por confirmar'}</span>
                        </div>
                        <div className="flex items-center gap-2 text-slate-600 text-sm">
                          <svg className="w-4 h-4 text-slate-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                          </svg>
                          <span>
                            Piscina {comp.course_type === 'scm' ? 'Corta (25m)' : 'Larga (50m)'}
                          </span>
                        </div>
                      </div>

                      <div className="flex justify-end mt-auto pt-4 border-t border-slate-100">
                        <span className="text-blue-600 text-sm font-semibold flex items-center gap-1 group-hover:translate-x-1 transition-transform">
                          Ver Resultados
                          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                          </svg>
                        </span>
                      </div>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}
        </>
      )}
    </div>
  );
};
