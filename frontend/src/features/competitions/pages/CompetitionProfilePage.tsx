import React, { useState, useMemo } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { competitionService } from '../api/competitionService';
import { LoadingState } from '../../../components/ui/LoadingState';
import { ErrorState } from '../../../components/ui/ErrorState';
import { EmptyState } from '../../../components/ui/EmptyState';
import { CourseBadge } from '../../../components/ui/CourseBadge';
import { getCourseMeta } from '../../../lib/courseMeta';
import type { CompetitionEvent } from '../../../lib/schemas/competition';

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
  women: 'Damas',
  men: 'Varones',
  mixed: 'Mixto',
};

// Componente local para cada Prueba (Colapsable)
const getMinAge = (ageGroup: string) => {
  const match = ageGroup.match(/\d+/);
  return match ? parseInt(match[0], 10) : 0;
};

type AgeGroupCategory = CompetitionEvent & { categoryTitle: string };

type PruebaGroup = {
  pruebaKey: string;
  pruebaTitle: string;
  distance_m: number;
  stroke: string;
  gender: string;
  ageGroups: AgeGroupCategory[];
};

const normalizeSearchText = (value: string) =>
  value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();

const searchTokens = (value: string) => {
  const tokens = normalizeSearchText(value).match(/[a-z0-9]+/g) ?? [];
  return Array.from(new Set(tokens));
};

const matchesSearchTokens = (value: string, tokens: string[]) => {
  const normalizedValue = normalizeSearchText(value);
  return tokens.every(token => normalizedValue.includes(token));
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

const CategoryResults: React.FC<{ cat: AgeGroupCategory; isRelay: boolean; isSearching: boolean }> = ({ cat, isRelay, isSearching }) => {
  const [expanded, setExpanded] = useState(false);
  const showContent = isSearching || expanded;

  return (
    <div className="border-b border-line last:border-0">
      <button
        type="button"
        className="flex w-full items-center justify-between gap-3 bg-canvas/50 px-6 py-2 text-left border-b border-line hover:bg-canvas transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-2">
          <svg className={`w-4 h-4 text-content-subtle transition-transform ${showContent ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
          <h4 className="font-semibold text-content-muted text-sm">{cat.categoryTitle}</h4>
        </div>
        <span className="text-xs font-medium text-content-subtle">{cat.results.length} {isRelay ? 'equipos' : 'nadadores'}</span>
      </button>

      {showContent && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="bg-surface text-content-subtle font-medium border-b border-line">
              <tr>
                <th className="px-6 py-2 w-16 text-center">Pos</th>
                <th className="px-6 py-2">{isRelay ? 'Equipo' : 'Nadador'}</th>
                <th className="px-6 py-2 hidden sm:table-cell">Club</th>
                <th className="px-6 py-2 text-right hidden md:table-cell">Seed</th>
                <th className="px-6 py-2 text-right">Tiempo</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-line">
              {cat.results.map((result, idx) => (
                <tr key={`${result.athlete_id || 'relay'}-${idx}`} className="hover:bg-canvas transition-colors">
                  <td className="px-6 py-2 text-center">
                    {result.rank ? (
                      <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full font-bold text-xs ${
                        result.rank === 1 ? 'bg-warning/30 text-medal-gold' :
                        result.rank === 2 ? 'bg-canvas text-medal-silver' :
                        result.rank === 3 ? 'bg-warning/20 text-medal-bronze' :
                        'text-content-subtle'
                      }`}>
                        {result.rank}
                      </span>
                    ) : (
                      <span className="text-content-subtle font-bold">-</span>
                    )}
                  </td>
                  <td className="px-6 py-2">
                    {result.athlete_id ? (
                      <Link to={`/athletes/${result.athlete_id}`} className="font-semibold text-action hover:text-brand-steel hover:underline">
                        {result.athlete_name}
                      </Link>
                    ) : (
                      <span className="font-semibold text-ink">
                        {result.athlete_name}
                      </span>
                    )}
                    <div className="text-xs text-content-subtle sm:hidden mt-0.5">{result.club_name}</div>
                  </td>
                  <td className="px-6 py-2 text-content-muted hidden sm:table-cell">{result.club_name}</td>
                  <td className="px-6 py-2 text-right hidden md:table-cell">
                    <span className="font-mono text-content-subtle">{result.seed_time_text || '-'}</span>
                  </td>
                  <td className="px-6 py-2 text-right">
                    {result.status === 'valid' ? (
                      <div className="flex flex-col items-end">
                        <span className="font-mono font-bold text-ink">{result.time_text}</span>
                        <TimeComparison seedMs={result.seed_time_ms} resultMs={result.result_time_ms} />
                      </div>
                    ) : (
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-danger/15 text-danger-strong uppercase">
                        {result.status}
                      </span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

const PruebaCard: React.FC<{ group: PruebaGroup; isSearching: boolean }> = ({ group, isSearching }) => {
  const [expanded, setExpanded] = useState(false);
  const showContent = isSearching || expanded;
  const totalParticipants = group.ageGroups.reduce((acc, cat) => acc + cat.results.length, 0);
  const isRelay = group.stroke.includes('relay');

  return (
    <div className="bg-surface rounded-xl shadow-sm border border-line overflow-hidden">
      <div 
        className="bg-canvas border-b border-line px-6 py-4 flex flex-col sm:flex-row sm:items-center justify-between gap-3 cursor-pointer hover:bg-canvas transition-colors select-none"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-3">
          <svg className={`w-5 h-5 text-content-subtle transition-transform ${showContent ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
          <h3 className="text-lg font-bold text-ink">{group.pruebaTitle}</h3>
        </div>
        <div className="flex items-center gap-3">
           <span className="text-sm text-content-subtle font-medium">{totalParticipants} {isRelay ? 'equipos' : 'nadadores'}</span>
           <span className="text-xs font-mono font-medium text-content-subtle bg-line px-2 py-1 rounded hidden sm:inline-block">{group.ageGroups.length} categorías</span>
        </div>
      </div>
      
      {showContent && (
        <div className="flex flex-col">
          {group.ageGroups.map(cat => (
            <CategoryResults key={cat.id} cat={cat} isRelay={isRelay} isSearching={isSearching} />
          ))}
        </div>
      )}
    </div>
  );
};

export const CompetitionProfilePage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');
  const [genderFilter, setGenderFilter] = useState('all');
  const [expandedMedalTableId, setExpandedMedalTableId] = useState<string | null>(null);

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['competition-detail', id],
    queryFn: () => competitionService.getCompetitionDetail(id!),
    enabled: !!id,
    retry: false,
  });

  const statsQuery = useQuery({
    queryKey: ['competition-stats', id],
    queryFn: () => competitionService.getCompetitionStats(id!),
    enabled: !!id,
    retry: false,
  });

  const groupedEvents = useMemo(() => {
    if (!data) return [];
    
    const groups = new Map<string, PruebaGroup>();

    data.events.forEach(event => {
      if (genderFilter !== 'all' && event.gender !== genderFilter) return;

      const pruebaKey = `${event.distance_m}-${event.stroke}-${event.gender}`;
      const pruebaTitle = `${event.distance_m}m ${strokeTranslations[event.stroke]} ${genderTranslations[event.gender]}`;
      const categoryTitle = `Categoría: ${event.age_group} años`;

      const queryTokens = searchTokens(searchQuery);
      
      let matchingResults = event.results;
      let matchesSearch = false;

      if (queryTokens.length > 0) {
        const matchesPruebaTitle = matchesSearchTokens(pruebaTitle, queryTokens);
        const matchesCategory = matchesSearchTokens(categoryTitle, queryTokens);
        
        matchingResults = event.results.filter(r => 
          matchesSearchTokens(r.athlete_name, queryTokens) ||
          matchesSearchTokens(r.club_name || '', queryTokens)
        );

        if (matchesPruebaTitle || matchesCategory) {
          matchingResults = event.results;
          matchesSearch = true;
        } else if (matchingResults.length > 0) {
          matchesSearch = true;
        }
      } else {
        matchesSearch = true;
      }

      if (matchesSearch) {
        if (!groups.has(pruebaKey)) {
          groups.set(pruebaKey, {
            pruebaKey,
            pruebaTitle,
            distance_m: event.distance_m,
            stroke: event.stroke,
            gender: event.gender,
            ageGroups: []
          });
        }
        groups.get(pruebaKey)!.ageGroups.push({
          ...event,
          categoryTitle,
          results: matchingResults
        });
      }
    });

    return Array.from(groups.values()).map(group => {
      group.ageGroups.sort((a, b) => getMinAge(a.age_group) - getMinAge(b.age_group));
      return group;
    });
  }, [data, searchQuery, genderFilter]);

  const totalUniquePruebas = useMemo(() => {
    if (!data) return 0;
    const unique = new Set(data.events.map(e => `${e.distance_m}-${e.stroke}-${e.gender}`));
    return unique.size;
  }, [data]);

  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState onRetry={() => refetch()} />;
  if (!data) return <EmptyState title="Competencia no encontrada" description="La competencia que buscas no existe o fue removida." />;

  const { competition } = data;
  // Avoid timezone shifts when the API returns a date-only value (YYYY-MM-DD).
  const dateString = competition.date_start.includes('T') ? competition.date_start : `${competition.date_start}T12:00:00`;
  const dateObj = new Date(dateString);
  const formattedDate = dateObj.toLocaleDateString('es-CL', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
  const isSearching = searchQuery.trim().length > 0;
  const course = getCourseMeta(competition.course_type);
  const hasActiveFilters = searchQuery.trim() !== '' || genderFilter !== 'all';
  const clubMedalTable = statsQuery.data?.club_medal_table ?? [];
  const isMedalTableExpanded = expandedMedalTableId === id;
  const visibleClubMedals = isMedalTableExpanded ? clubMedalTable : clubMedalTable.slice(0, 10);

  const clearFilters = () => {
    setSearchQuery('');
    setGenderFilter('all');
  };

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
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

      {/* Header Competencia */}
      <div className="bg-brand-night rounded-2xl shadow-xl p-6 md:p-8 text-brand-white relative overflow-hidden">
        <div className="absolute top-0 right-0 -mt-8 -mr-8 w-48 h-48 bg-brand-pool rounded-full opacity-20 blur-3xl pointer-events-none"></div>
        
        <div className="relative z-10 flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <CourseBadge courseType={competition.course_type} variant="dark" />
              <span className="text-xs font-medium text-brand-muted">{course.description}</span>
            </div>
            <h1 className="text-3xl md:text-4xl font-black tracking-tight mb-2">{competition.name}</h1>
            <div className="flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-6 text-brand-muted text-sm">
              <span className="flex items-center gap-1.5">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                </svg>
                <span className="capitalize">{formattedDate}</span>
              </span>
              <span className="flex items-center gap-1.5">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                </svg>
                {competition.location || 'Sede por confirmar'}
              </span>
              {competition.source_url && (
                <a
                  href={competition.source_url}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1.5 text-brand-cyan hover:text-brand-white hover:underline"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.5 6H18m0 0v4.5M18 6l-7.5 7.5" />
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 6h4.5M6 6v12h12v-4.5" />
                  </svg>
                  Fuente oficial
                </a>
              )}
            </div>
          </div>
          
          <div className="bg-surface/10 backdrop-blur-sm rounded-xl p-4 text-center min-w-32 border border-brand-white/10 shadow-inner">
            <span className="block text-3xl font-black text-brand-white leading-none">{totalUniquePruebas}</span>
            <span className="text-xs font-medium text-brand-muted uppercase tracking-widest mt-1 block">Pruebas Totales</span>
          </div>
        </div>
      </div>

      {statsQuery.data && (
        <div className="grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-8">
          {[
            ['Participantes', statsQuery.data.participants_count],
            ['Mujeres', statsQuery.data.women_count],
            ['Hombres', statsQuery.data.men_count],
            ['Clubes', statsQuery.data.clubs_count],
            ['Pruebas', statsQuery.data.events_count],
            ['Válidos', statsQuery.data.valid_results_count],
            ['DQ', statsQuery.data.dsq_count],
            ['Entradas', statsQuery.data.entries_count],
          ].map(([label, value]) => (
            <div key={label} className="rounded-xl border border-line bg-surface p-4 shadow-sm">
              <p className="text-xs font-bold uppercase tracking-widest text-content-subtle">{label}</p>
              <p className="mt-1 text-2xl font-black text-ink">{value}</p>
            </div>
          ))}
        </div>
      )}

      {clubMedalTable.length > 0 && (
        <section className="space-y-4" aria-labelledby="club-medal-table-heading">
          <div>
            <h2 id="club-medal-table-heading" className="text-2xl font-bold tracking-tight text-ink">
              Medallero de Clubes
            </h2>
            <p id="club-medal-table-description" className="mt-1 text-sm text-content-subtle">
              Las categorías Pre-Master están excluidas de este medallero.
            </p>
          </div>

          <div className="overflow-hidden rounded-xl border border-line bg-surface shadow-sm">
            <div className="overflow-x-auto">
              <table id="club-medal-table" className="w-full min-w-[40rem] text-left text-sm" aria-describedby="club-medal-table-description">
                <thead className="border-b border-line bg-canvas text-xs font-bold uppercase tracking-widest text-content-subtle">
                  <tr>
                    <th scope="col" className="w-20 px-4 py-3 text-center">Pos.</th>
                    <th scope="col" className="px-4 py-3">Club</th>
                    <th scope="col" className="px-4 py-3 text-right text-medal-gold">Oro</th>
                    <th scope="col" className="px-4 py-3 text-right text-medal-silver">Plata</th>
                    <th scope="col" className="px-4 py-3 text-right text-medal-bronze">Bronce</th>
                    <th scope="col" className="px-4 py-3 text-right">Total</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {visibleClubMedals.map((club, index) => (
                    <tr key={club.club_id} className="transition-colors hover:bg-canvas">
                      <td className="px-4 py-3 text-center font-bold text-content-subtle">{index + 1}</td>
                      <th scope="row" className="px-4 py-3">
                        <Link to={`/clubs/${club.club_id}`} className="font-semibold text-action hover:text-brand-steel hover:underline">
                          {club.club_name}
                        </Link>
                      </th>
                      <td className="px-4 py-3 text-right font-bold text-medal-gold">{club.gold_medals}</td>
                      <td className="px-4 py-3 text-right font-bold text-medal-silver">{club.silver_medals}</td>
                      <td className="px-4 py-3 text-right font-bold text-medal-bronze">{club.bronze_medals}</td>
                      <td className="px-4 py-3 text-right font-black text-ink">{club.total_medals}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {clubMedalTable.length > 10 && (
              <div className="border-t border-line px-4 py-3 text-center">
                <button
                  type="button"
                  onClick={() => setExpandedMedalTableId(isMedalTableExpanded ? null : id ?? null)}
                  aria-expanded={isMedalTableExpanded}
                  aria-controls="club-medal-table"
                  className="rounded-lg border border-line bg-surface px-4 py-2 text-sm font-semibold text-action transition-colors hover:bg-canvas hover:text-brand-steel"
                >
                  {isMedalTableExpanded ? 'Ver menos' : 'Ver más'}
                </button>
              </div>
            )}
          </div>
        </section>
      )}

      {/* Resultados por Evento */}
      <div className="space-y-4">
        <div className="mb-6 flex flex-col justify-between gap-4 lg:flex-row lg:items-center">
          <h2 className="text-2xl font-bold text-ink tracking-tight">Resultados</h2>

          <div className="flex w-full flex-col gap-3 sm:flex-row lg:w-auto">
            <div className="relative flex-1 lg:w-96">
              <input
                type="text"
                className="w-full pl-10 pr-4 py-2 border border-line rounded-lg shadow-sm focus:ring-2 focus:ring-action focus:border-action transition-shadow outline-none bg-surface text-sm"
                placeholder="Buscar por prueba, atleta o club..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <svg className="w-5 h-5 text-content-subtle absolute left-3 top-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>

            <div className="flex gap-3">
              <select
                value={genderFilter}
                onChange={(e) => setGenderFilter(e.target.value)}
                className="min-w-0 flex-1 py-2 pl-3 pr-8 border border-line bg-surface rounded-lg shadow-sm focus:ring-2 focus:ring-action focus:border-action text-sm sm:w-48"
              >
                <option value="all">Ambos Géneros</option>
                <option value="women">Damas</option>
                <option value="men">Varones</option>
                <option value="mixed">Mixtos</option>
              </select>
              {hasActiveFilters && (
                <button
                  type="button"
                  onClick={clearFilters}
                  className="whitespace-nowrap rounded-lg border border-line bg-surface px-4 py-2 text-sm font-medium text-content-muted shadow-sm transition-colors hover:bg-canvas hover:text-ink"
                >
                  Limpiar
                </button>
              )}
            </div>
          </div>
        </div>
        
        {groupedEvents.length === 0 ? (
          <EmptyState 
            title="No se encontraron resultados" 
            description={isSearching ? "Intenta con otros términos de búsqueda." : "No hay resultados cargados para esta competencia."} 
          />
        ) : (
          <div className="flex flex-col gap-4">
            {groupedEvents.map((group) => (
              <PruebaCard 
                key={group.pruebaKey} 
                group={group} 
                isSearching={isSearching} 
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
