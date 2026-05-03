import { RankingsResponseSchema } from '../../../lib/schemas/ranking';
import type { RankingsResponse } from '../../../lib/schemas/ranking';
import fixtureData from '../../../test/fixtures/rankings.json';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const rankingService = {
  async getRankings(): Promise<RankingsResponse> {
    await delay(700);

    const allData = fixtureData.search_results as RankingsResponse;
    
    const parsed = RankingsResponseSchema.safeParse(allData);
    if (!parsed.success) {
      throw new Error("Fixture data invalid: " + parsed.error.message);
    }

    return parsed.data;
  }
};
