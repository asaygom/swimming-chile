import { CompetitionDetailResponseSchema, CompetitionFilterOptionsSchema, CompetitionStatsSchema, CompetitionsResponseSchema, LiveHeatResponseSchema, LiveHeatUpdateResponseSchema, MeetProgramResponseSchema, OperatorSessionResponseSchema } from '../../../lib/schemas/competition';
import type { CompetitionDetailResponse, CompetitionFilterOptions, CompetitionStats, CompetitionsResponse, LiveHeatResponse, LiveHeatUpdate, MeetProgramResponse } from '../../../lib/schemas/competition';
import { ApiError } from '../../../lib/api/fetcher';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000';

export const competitionService = {
  async getCompetitions(
    query: string = '',
    year: string = 'all',
    page: number = 1,
    timeframe: string = 'all',
    competitionScope: string = 'all',
    governingBody: string = 'all'
  ): Promise<CompetitionsResponse> {
    const url = new URL(`${API_BASE_URL}/api/competitions`);
    if (query) url.searchParams.append('search', query);
    if (year !== 'all') url.searchParams.append('year', year);
    if (timeframe !== 'all') url.searchParams.append('timeframe', timeframe);
    if (competitionScope !== 'all') url.searchParams.append('competition_scope', competitionScope);
    if (governingBody !== 'all') url.searchParams.append('governing_body', governingBody);
    url.searchParams.append('page', page.toString());
    
    const response = await fetch(url);
    if (!response.ok) throw new Error('Failed to fetch competitions');
    
    const data = await response.json();
    return CompetitionsResponseSchema.parse(data);
  },

  async getCompetitionDetail(id: string): Promise<CompetitionDetailResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}`);
    if (!response.ok) throw new Error('Failed to fetch competition details');
    
    const data = await response.json();
    return CompetitionDetailResponseSchema.parse(data);
  },

  async getCompetitionStats(id: string): Promise<CompetitionStats> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/stats`);
    if (!response.ok) throw new Error('Failed to fetch competition stats');

    const data = await response.json();
    return CompetitionStatsSchema.parse(data);
  },

  async getMeetProgram(id: string): Promise<MeetProgramResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/meet-program`);
    if (!response.ok) throw new Error('Failed to fetch meet program');

    const data = await response.json();
    return MeetProgramResponseSchema.parse(data);
  },

  async getLiveHeat(id: string): Promise<LiveHeatResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-heat`);
    if (!response.ok) throw new Error('Failed to fetch live heat');

    const data = await response.json();
    return LiveHeatResponseSchema.parse(data);
  },

  async createLiveHeatSession(id: string, code: string): Promise<void> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-heat/session`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo iniciar la sesión');
    OperatorSessionResponseSchema.parse(await response.json());
  },

  async updateLiveHeat(id: string, update: LiveHeatUpdate) {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-heat`, {
      method: 'PUT',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(update),
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo actualizar el llamador');
    return LiveHeatUpdateResponseSchema.parse(await response.json());
  },

  async getCompetitionYears(): Promise<number[]> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/years`);
    if (!response.ok) throw new Error('Failed to fetch competition years');
    const data = await response.json();
    return data.years;
  },

  async getCompetitionFilterOptions(timeframe: string = 'all'): Promise<CompetitionFilterOptions> {
    const url = new URL(`${API_BASE_URL}/api/competitions/filter-options`);
    if (timeframe !== 'all') url.searchParams.append('timeframe', timeframe);

    const response = await fetch(url);
    if (!response.ok) throw new Error('Failed to fetch competition filter options');
    const data = await response.json();
    return CompetitionFilterOptionsSchema.parse(data);
  }
};
