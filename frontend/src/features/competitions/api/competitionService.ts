import { AdminSessionResponseSchema, SupabasePasswordResponseSchema } from '../../../lib/schemas/auth';
import { CompetitionDetailResponseSchema, CompetitionFilterOptionsSchema, CompetitionStatsSchema, CompetitionsResponseSchema, LiveAnnouncementHistoryResponseSchema, LiveAnnouncementResponseSchema, LiveAnnouncementsResponseSchema, LiveBrandingResponseSchema, LiveHeatHistoryResponseSchema, LiveHeatResponseSchema, LiveHeatUpdateResponseSchema, MeetProgramPreviewSchema, MeetProgramResponseSchema, OperatorSessionResponseSchema } from '../../../lib/schemas/competition';
import type { CompetitionDetailResponse, CompetitionFilterOptions, CompetitionStats, CompetitionsResponse, LiveAnnouncementActivation, LiveAnnouncementCreate, LiveAnnouncementHistoryResponse, LiveAnnouncementResponse, LiveAnnouncementsResponse, LiveAnnouncementUpdate, LiveBrandingResponse, LiveHeatHistoryResponse, LiveHeatResponse, LiveHeatUpdate, MeetProgramPreview, MeetProgramResponse } from '../../../lib/schemas/competition';
import { ApiError } from '../../../lib/api/fetcher';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000';
const SUPABASE_URL = (import.meta.env.VITE_SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_ANON_KEY = import.meta.env.VITE_SUPABASE_ANON_KEY || '';

export const competitionService = {
  isLiveAnnouncementAdminAuthConfigured(): boolean {
    return Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  },

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

  async getLiveHeatHistory(id: string, limit: number = 8): Promise<LiveHeatHistoryResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-heat/history?limit=${limit}`, {
      credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'Live heat history request failed');
    return LiveHeatHistoryResponseSchema.parse(await response.json());
  },

  async getActiveLiveAnnouncement(id: string): Promise<LiveAnnouncementResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-announcements/active`);
    if (!response.ok) throw new Error('Failed to fetch live announcement');

    return LiveAnnouncementResponseSchema.parse(await response.json());
  },

  async uploadMeetProgram(
    id: string,
    file: File,
    sourceFormat: 'pdf' | 'csv',
    action: 'preview' | 'publish',
    scheduledDate?: string,
  ): Promise<MeetProgramPreview> {
    const params = new URLSearchParams({ source_format: sourceFormat, source_name: file.name });
    if (scheduledDate) params.set('scheduled_date', scheduledDate);
    const response = await fetch(
      `${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/meet-program/${action}?${params}`,
      { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/octet-stream' }, body: file },
    );
    const data = await response.json().catch(() => null);
    if (!response.ok) {
      // 422 al publicar devuelve el resumen completo como detail: se conserva
      // para poder mostrar los issues en vez de un mensaje generico.
      const detail = data?.detail;
      if (response.status === 422 && detail && typeof detail === 'object') {
        return MeetProgramPreviewSchema.parse(detail);
      }
      throw new ApiError(response.status, typeof detail === 'string' ? detail : 'No se pudo procesar el sembrado');
    }
    return MeetProgramPreviewSchema.parse(data);
  },

  async getLiveBranding(id: string): Promise<LiveBrandingResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-branding`);
    if (!response.ok) throw new ApiError(response.status, 'No se pudo cargar el logo de la competencia');
    return LiveBrandingResponseSchema.parse(await response.json());
  },

  getLiveBrandingLogoUrl(id: string, revision: number): string {
    return `${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-branding/logo?revision=${revision}`;
  },

  async uploadLiveBranding(id: string, file: File, expectedRevision: number): Promise<LiveBrandingResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-branding?expected_revision=${expectedRevision}`, {
      method: 'PUT', credentials: 'include', headers: { 'Content-Type': file.type }, body: file,
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo guardar el logo');
    return LiveBrandingResponseSchema.parse(await response.json());
  },

  async deleteLiveBranding(id: string, expectedRevision: number): Promise<LiveBrandingResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-branding?expected_revision=${expectedRevision}`, {
      method: 'DELETE', credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo eliminar el logo');
    return LiveBrandingResponseSchema.parse(await response.json());
  },

  async createLiveAnnouncementAdminSession(email: string, password: string): Promise<void> {
    if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
      throw new Error('Autenticación administrativa no configurada');
    }
    const providerResponse = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
      method: 'POST',
      headers: { apikey: SUPABASE_ANON_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!providerResponse.ok) throw new ApiError(providerResponse.status, 'Credenciales administrativas inválidas');
    const ephemeral = {
      accessToken: SupabasePasswordResponseSchema.parse(await providerResponse.json()).access_token,
    };
    try {
      const response = await fetch(`${API_BASE_URL}/api/auth/admin-session`, {
        method: 'POST', credentials: 'include',
        headers: { Authorization: `Bearer ${ephemeral.accessToken}` },
      });
      if (!response.ok) throw new ApiError(response.status, 'Cuenta administrativa no habilitada');
      AdminSessionResponseSchema.parse(await response.json());
    } finally {
      ephemeral.accessToken = '';
    }
  },

  async deleteLiveAnnouncementAdminSession(): Promise<void> {
    const response = await fetch(`${API_BASE_URL}/api/auth/admin-session/logout`, {
      method: 'POST', credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo cerrar la sesión');
  },

  async getLiveAnnouncements(id: string): Promise<LiveAnnouncementsResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements`, {
      credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo validar el acceso administrativo');
    return LiveAnnouncementsResponseSchema.parse(await response.json());
  },

  async getLiveAnnouncementHistory(id: string, limit: number = 20): Promise<LiveAnnouncementHistoryResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements/history?limit=${limit}`, {
      credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'Announcement history request failed');
    return LiveAnnouncementHistoryResponseSchema.parse(await response.json());
  },

  async createLiveAnnouncement(id: string, body: LiveAnnouncementCreate): Promise<LiveAnnouncementResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements`, {
      method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo crear el comunicado');
    return LiveAnnouncementResponseSchema.parse(await response.json());
  },

  async updateLiveAnnouncement(id: string, announcementId: number, body: LiveAnnouncementUpdate): Promise<LiveAnnouncementResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements/${announcementId}`, {
      method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo actualizar el comunicado');
    return LiveAnnouncementResponseSchema.parse(await response.json());
  },

  async setLiveAnnouncementActivation(id: string, announcementId: number, body: LiveAnnouncementActivation): Promise<LiveAnnouncementResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements/${announcementId}/activation`, {
      method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo cambiar la publicación');
    return LiveAnnouncementResponseSchema.parse(await response.json());
  },

  async deleteLiveAnnouncement(id: string, announcementId: number, expected_revision: number): Promise<LiveAnnouncementResponse> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${encodeURIComponent(id)}/live-announcements/${announcementId}?expected_revision=${expected_revision}`, {
      method: 'DELETE', credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo eliminar el comunicado');
    return LiveAnnouncementResponseSchema.parse(await response.json());
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

  async deleteLiveHeatSession(id: string): Promise<void> {
    const response = await fetch(`${API_BASE_URL}/api/competitions/${id}/live-heat/session/logout`, {
      method: 'POST', credentials: 'include',
    });
    if (!response.ok) throw new ApiError(response.status, 'No se pudo cerrar la sesi\u00f3n');
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
