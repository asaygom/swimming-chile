import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { rankingService } from '../api/rankingService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';
import { Link } from 'react-router-dom';

export const RankingsPage: React.FC = () => {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['rankings'],
    queryFn: () => rankingService.getRankings(),
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Rankings Nacionales</h1>
        <p className="text-slate-500 mt-1">Mejores marcas históricas por prueba y categoría.</p>
      </div>

      {isLoading && <LoadingState />}
      {isError && <ErrorState onRetry={() => refetch()} />}
      
      {!isLoading && !isError && data && (
        <>
          {/* Header decorativo de prueba actual */}
          <div className="bg-slate-900 rounded-xl p-6 text-white shadow-lg shadow-slate-900/20">
            <h2 className="text-xl font-bold">Top 10 - 50m Libre (Hombres)</h2>
            <p className="text-slate-300 text-sm mt-1">Categoría 40-44 años • Piscina Corta (SCM)</p>
          </div>

          {data.data.length === 0 ? (
            <EmptyState title="No hay marcas para esta prueba" />
          ) : (
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-slate-50 text-slate-600 font-medium border-b border-slate-200">
                    <tr>
                      <th className="px-6 py-4 w-16 text-center">Pos</th>
                      <th className="px-6 py-4">Atleta</th>
                      <th className="px-6 py-4">Club</th>
                      <th className="px-6 py-4">Competencia</th>
                      <th className="px-6 py-4 text-right">Tiempo</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {data.data.map((entry) => (
                      <tr key={entry.athlete_id} className="hover:bg-slate-50 transition-colors group">
                        <td className="px-6 py-4 text-center">
                          <span className={`inline-flex items-center justify-center w-8 h-8 rounded-full font-bold ${
                            entry.rank === 1 ? 'bg-amber-100 text-amber-700 ring-1 ring-amber-300' :
                            entry.rank === 2 ? 'bg-slate-100 text-slate-600 ring-1 ring-slate-300' :
                            entry.rank === 3 ? 'bg-orange-50 text-orange-700 ring-1 ring-orange-200' :
                            'text-slate-500'
                          }`}>
                            {entry.rank}
                          </span>
                        </td>
                        <td className="px-6 py-4 font-semibold text-slate-900">
                          <Link to={`/athletes/${entry.athlete_id}`} className="hover:text-blue-600 hover:underline">
                            {entry.athlete_name}
                          </Link>
                        </td>
                        <td className="px-6 py-4 text-slate-600">{entry.club_name}</td>
                        <td className="px-6 py-4 text-slate-500 text-xs">
                          {entry.competition_name}<br/>
                          <span className="text-slate-400">{new Date(entry.date).getFullYear()}</span>
                        </td>
                        <td className="px-6 py-4 text-right">
                          <span className="font-mono font-bold text-blue-700 text-base">{entry.time_text}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};
