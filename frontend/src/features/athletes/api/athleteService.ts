import { AthletesResponseSchema, AthleteProfileSchema } from '../../../lib/schemas/athlete';
import type { AthletesResponse, AthleteProfile } from '../../../lib/schemas/athlete';
import fixtureData from '../../../test/fixtures/athletes.json';

// Simulador de latencia de red
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const athleteService = {
  /**
   * Obtiene la lista de atletas (Mock).
   * Valida la respuesta del fixture contra el esquema Zod para probar el contrato.
   */
  async searchAthletes(query: string = ''): Promise<AthletesResponse> {
    await delay(600); // Fake network delay

    // Filtro básico en memoria para simular el endpoint real
    const allData = fixtureData.search_results as AthletesResponse;
    
    // Validación estricta del fixture para asegurar que nuestro mock cumple el contrato
    const parsed = AthletesResponseSchema.safeParse(allData);
    if (!parsed.success) {
      throw new Error("Fixture data invalid: " + parsed.error.message);
    }

    if (!query) return parsed.data;

    const filteredData = parsed.data.data.filter(a => 
      a.full_name.toLowerCase().includes(query.toLowerCase())
    );

    return {
      data: filteredData,
      meta: {
        ...parsed.data.meta,
        total_results: filteredData.length,
      }
    };
  },

  /**
   * Obtiene el perfil de un atleta por ID (Mock).
   */
  async getAthleteProfile(id: string): Promise<AthleteProfile> {
    await delay(800);

    const profile = fixtureData.athlete_profile as AthleteProfile;
    
    const parsed = AthleteProfileSchema.safeParse(profile);
    if (!parsed.success) {
      throw new Error("Fixture data invalid: " + parsed.error.message);
    }

    // Para el mock, siempre devolvemos el mismo perfil a menos que el ID sea específico para forzar un error
    if (id === 'error') throw new Error("Error forzado de prueba");

    return parsed.data;
  }
};
