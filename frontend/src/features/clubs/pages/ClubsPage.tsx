import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { clubService } from '../api/clubService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';
import { Link } from 'react-router-dom';

export const ClubsPage: React.FC = () => {
  const [query, setQuery] = React.useState('');
  
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['clubs'],
    queryFn: () => clubService.getClubs(),
  });

  const filteredClubs = data?.data.filter(club => 
    club.name.toLowerCase().includes(query.toLowerCase()) ||
    (club.city && club.city.toLowerCase().includes(query.toLowerCase()))
  );

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {/* Header & Search */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Clubes</h1>
          <p className="text-slate-500 mt-1">Explora los clubes de natación registrados.</p>
        </div>
        <div className="relative w-full md:w-96">
          <input
            type="text"
            className="w-full pl-10 pr-4 py-2 border border-slate-300 rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-shadow outline-none bg-white text-sm"
            placeholder="Buscar por nombre o ciudad..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <svg className="w-5 h-5 text-slate-400 absolute left-3 top-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
      </div>

      {isLoading && <LoadingState />}
      {isError && <ErrorState onRetry={() => refetch()} />}
      
      {!isLoading && !isError && filteredClubs && (
        <>
          {filteredClubs.length === 0 ? (
            <EmptyState title="No se encontraron clubes" description="Intenta con otro término de búsqueda." />
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
              {filteredClubs.map((club) => (
                <Link 
                  key={club.id} 
                  to={`/clubs/${club.id}`}
                  className="group bg-white rounded-xl shadow-sm border border-slate-200 p-6 hover:shadow-md hover:border-blue-300 transition-all flex flex-col h-full"
                >
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 rounded-full bg-blue-50 text-blue-600 flex items-center justify-center font-bold text-xl group-hover:bg-blue-600 group-hover:text-white transition-colors">
                      {club.name.charAt(0)}
                    </div>
                    <div>
                      <h2 className="text-lg font-bold text-slate-900 leading-tight group-hover:text-blue-700 transition-colors">{club.name}</h2>
                      <p className="text-sm text-slate-500 mt-0.5 flex items-center gap-1">
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                        </svg>
                        {club.city || 'Desconocido'}, {club.country || 'Chile'}
                      </p>
                    </div>
                  </div>
                  <div className="mt-6 pt-4 border-t border-slate-100 flex justify-between items-center">
                    <span className="text-sm font-medium text-slate-600">Nadadores registrados:</span>
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">
                      {club.total_athletes || 0}
                    </span>
                  </div>
                </Link>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
};
