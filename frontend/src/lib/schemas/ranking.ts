import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';
import { AthleteGenderSchema, StrokeSchema, CourseTypeSchema } from './canon';

export const RankingEntrySchema = z.object({
  rank: z.number().int(),
  athlete_name: z.string(),
  athlete_id: z.string().uuid(),
  club_name: z.string(),
  time_text: z.string(),
  time_ms: z.number().int(),
  competition_name: z.string(),
  date: z.string().datetime(),
  distance_m: z.number().int(),
  stroke: StrokeSchema,
  course_type: CourseTypeSchema,
  gender: AthleteGenderSchema,
  age_group: z.string(),
});

export type RankingEntry = z.infer<typeof RankingEntrySchema>;

export const RankingsResponseSchema = z.object({
  data: z.array(RankingEntrySchema),
  meta: PaginationMetaSchema,
});

export type RankingsResponse = z.infer<typeof RankingsResponseSchema>;
