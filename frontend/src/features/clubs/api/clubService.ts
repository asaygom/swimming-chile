import { ClubsResponseSchema } from '../../../lib/schemas/club';
import type { ClubsResponse } from '../../../lib/schemas/club';
import fixtureData from '../../../test/fixtures/clubs.json';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export const clubService = {
  async getClubs(): Promise<ClubsResponse> {
    await delay(500);

    const allData = fixtureData.search_results as ClubsResponse;
    
    const parsed = ClubsResponseSchema.safeParse(allData);
    if (!parsed.success) {
      throw new Error("Fixture data invalid: " + parsed.error.message);
    }

    return parsed.data;
  }
};
