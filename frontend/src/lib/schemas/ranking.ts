import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';
import { EventGenderSchema, StrokeSchema, CourseTypeSchema } from './canon';

export const RankingEntrySchema = z.object({
  rank: z.number().int(),
  athlete_name: z.string(),
  athlete_id: z.union([z.string(), z.number()]),
  club_name: z.string().nullable().optional(),
  time_text: z.string(),
  time_ms: z.number().int(),
  competition_id: z.union([z.string(), z.number()]),
  competition_name: z.string(),
  date: z.string().nullable().optional(),
  distance_m: z.number().int(),
  stroke: StrokeSchema,
  course_type: CourseTypeSchema.nullable().optional(),
  gender: EventGenderSchema,
  age_group: z.string(),
  event_age_group: z.string(),
  birth_year: z.number().int().nullable().optional(),
  current_age: z.number().int().nullable().optional(),
});

export type RankingEntry = z.infer<typeof RankingEntrySchema>;

export const RankingsResponseSchema = z.object({
  data: z.array(RankingEntrySchema),
  meta: PaginationMetaSchema,
});

export type RankingsResponse = z.infer<typeof RankingsResponseSchema>;

export const RankingFilterOptionsSchema = z.object({
  distances: z.array(z.number().int()),
  strokes: z.array(StrokeSchema),
  event_options: z.array(z.object({
    distance_m: z.number().int(),
    stroke: StrokeSchema,
  })),
  age_groups: z.array(z.string()),
  years: z.array(z.number().int()),
  scopes: z.array(z.string()),
});

export type RankingFilterOptions = z.infer<typeof RankingFilterOptionsSchema>;

export const ClubParticipationEntrySchema = z.object({
  rank: z.number().int(),
  club_id: z.union([z.string(), z.number()]),
  club_name: z.string(),
  unique_athletes: z.number().int(),
  competitions_count: z.number().int(),
  entries_count: z.number().int(),
});

export type ClubParticipationEntry = z.infer<typeof ClubParticipationEntrySchema>;

export const ClubParticipationResponseSchema = z.object({
  data: z.array(ClubParticipationEntrySchema),
  meta: PaginationMetaSchema,
});

export type ClubParticipationResponse = z.infer<typeof ClubParticipationResponseSchema>;

export const ClubStatsFilterOptionsSchema = z.object({
  years: z.array(z.number().int()),
  governing_bodies: z.array(z.object({
    governing_body_code: z.string(),
    governing_body_name: z.string().nullable().optional(),
  })),
});

export type ClubStatsFilterOptions = z.infer<typeof ClubStatsFilterOptionsSchema>;

export const ClubParticipationMatrixSchema = z.object({
  year: z.number().int(),
  governing_body: z.string(),
  competitions: z.array(z.object({
    id: z.union([z.string(), z.number()]),
    name: z.string(),
    date: z.string().nullable().optional(),
  })),
  totals: z.record(z.string(), z.number().int()),
  clubs: z.array(z.object({
    rank: z.number().int(),
    club_id: z.union([z.string(), z.number()]),
    club_name: z.string(),
    total_athletes: z.number().int(),
    competitions_count: z.number().int(),
    cells: z.record(z.string(), z.number().int()),
  })),
});

export type ClubParticipationMatrix = z.infer<typeof ClubParticipationMatrixSchema>;

export const CompetitionStatsRowSchema = z.object({
  id: z.union([z.string(), z.number()]),
  name: z.string(),
  date: z.string().nullable().optional(),
  course_type: CourseTypeSchema.nullable().optional(),
  governing_body_code: z.string().nullable().optional(),
  governing_body_name: z.string().nullable().optional(),
  participants_count: z.number().int(),
  women_count: z.number().int(),
  men_count: z.number().int(),
  clubs_count: z.number().int(),
  events_count: z.number().int(),
  valid_results_count: z.number().int(),
  dsq_count: z.number().int(),
  entries_count: z.number().int(),
});

export const CompetitionStatsTableSchema = z.object({
  year: z.number().int(),
  governing_body: z.string(),
  data: z.array(CompetitionStatsRowSchema),
});

export type CompetitionStatsTable = z.infer<typeof CompetitionStatsTableSchema>;
