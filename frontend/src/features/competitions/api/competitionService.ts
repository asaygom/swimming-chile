import { CompetitionsResponseSchema } from '../../../lib/schemas/competition';
import type { CompetitionsResponse } from '../../../lib/schemas/competition';
import fixtureData from '../../../test/fixtures/competitions.json';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const competitionService = {
  async getCompetitions(): Promise<CompetitionsResponse> {
    await delay(500);

    const allData = fixtureData.search_results as CompetitionsResponse;
    
    const parsed = CompetitionsResponseSchema.safeParse(allData);
    if (!parsed.success) {
      throw new Error("Fixture data invalid: " + parsed.error.message);
    }

    return parsed.data;
  }
};
