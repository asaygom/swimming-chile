import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';
import { CourseTypeSchema } from './canon';

export const CompetitionSchema = z.object({
  id: z.union([z.string(), z.number()]),
  name: z.string(),
  date_start: z.string(),
  date_end: z.string().nullable().optional(),
  location: z.string().nullable().optional(),
  course_type: CourseTypeSchema.nullable().optional(),
  competition_scope: z.string().nullable().optional(),
  governing_body_code: z.string().nullable().optional(),
  governing_body_name: z.string().nullable().optional(),
  organizer: z.string().nullable().optional(),
  source_url: z.string().nullable().optional(),
});

export type Competition = z.infer<typeof CompetitionSchema>;

export const CompetitionsResponseSchema = z.object({
  data: z.array(CompetitionSchema),
  meta: PaginationMetaSchema,
});

export type CompetitionsResponse = z.infer<typeof CompetitionsResponseSchema>;

export const CompetitionFilterOptionsSchema = z.object({
  years: z.array(z.number().int()),
  scopes: z.array(z.string()),
  governing_bodies: z.array(z.object({
    governing_body_code: z.string(),
    governing_body_name: z.string().nullable().optional(),
  })),
});

export type CompetitionFilterOptions = z.infer<typeof CompetitionFilterOptionsSchema>;

import { EventGenderSchema, StrokeSchema, ResultStatusSchema } from './canon';

export const CompetitionResultSchema = z.object({
  rank: z.number().int().nullable().optional(),
  athlete_name: z.string(),
  athlete_id: z.union([z.string(), z.number()]).nullable().optional(),
  club_name: z.string().nullable().optional(),
  time_text: z.string().nullable(),
  seed_time_text: z.string().nullable().optional(),
  seed_time_ms: z.number().int().nullable().optional(),
  result_time_ms: z.number().int().nullable().optional(),
  status: ResultStatusSchema,
});

export type CompetitionResult = z.infer<typeof CompetitionResultSchema>;

export const CompetitionEventSchema = z.object({
  id: z.union([z.string(), z.number()]),
  distance_m: z.number().int(),
  stroke: StrokeSchema,
  gender: EventGenderSchema,
  age_group: z.string(),
  results: z.array(CompetitionResultSchema),
});

export type CompetitionEvent = z.infer<typeof CompetitionEventSchema>;

export const CompetitionDetailResponseSchema = z.object({
  competition: CompetitionSchema,
  events: z.array(CompetitionEventSchema),
});

export type CompetitionDetailResponse = z.infer<typeof CompetitionDetailResponseSchema>;

export const MeetProgramEntrySchema = z.object({
  lane: z.number().int().nonnegative(),
  entry_type: z.enum(['individual', 'relay']),
  display_name: z.string(),
  club_name: z.string().nullable(),
  seed_time_text: z.string().nullable(),
  seed_time_ms: z.number().int().nullable(),
  relay_members: z.array(z.string()),
});

export const MeetProgramHeatSchema = z.object({
  heat_number: z.number().int().positive(),
  heat_total: z.number().int().positive().nullable(),
  estimated_start_time: z.string().regex(/^(?:[01]\d|2[0-3]):[0-5]\d$/).nullable(),
  entries: z.array(MeetProgramEntrySchema),
});

export const MeetProgramEventSchema = z.object({
  event_number: z.number().int().positive(),
  event_name: z.string(),
  distance_m: z.number().int().positive().nullable(),
  stroke: z.string().nullable(),
  heats: z.array(MeetProgramHeatSchema),
});

export const MeetProgramSessionSchema = z.object({
  publication_id: z.number().int().positive(),
  session_number: z.number().int().positive(),
  session_name: z.string(),
  stage_number: z.number().int().positive().default(1),
  pool_role: z.enum(['main', 'competition', 'training']).default('main'),
  scheduled_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).nullable().default(null),
  events: z.array(MeetProgramEventSchema),
});

export const MeetProgramResponseSchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  events_count: z.number().int().nonnegative().nullable(),
  publication: z.object({
    published_at: z.string(),
    source_url: z.string().nullable(),
    entry_count: z.number().int().nonnegative(),
  }).nullable(),
  sessions: z.array(MeetProgramSessionSchema),
});

export type MeetProgramResponse = z.infer<typeof MeetProgramResponseSchema>;
export type MeetProgramSession = z.infer<typeof MeetProgramSessionSchema>;

export const LiveHeatStateSchema = z.object({
  publication_id: z.number().int().positive(),
  stage_number: z.number().int().positive(),
  pool_role: z.enum(['main', 'competition', 'training']),
  session_number: z.number().int().positive(),
  event_number: z.number().int().positive(),
  event_name: z.string(),
  heat_number: z.number().int().positive(),
  heat_total: z.number().int().positive().nullable(),
  status: z.enum(['not_started', 'active', 'paused', 'finished']),
  revision: z.number().int().positive(),
  updated_at: z.string(),
});

export const LiveHeatEntrySchema = z.object({
  lane: z.number().int().nonnegative(),
  entry_type: z.enum(['individual', 'relay']),
  display_name: z.string(),
  club_name: z.string().nullable(),
  seed_time_text: z.string().nullable(),
  relay_members: z.array(z.string()),
});

export const LiveHeatResponseSchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  state: LiveHeatStateSchema.nullable(),
  entries: z.array(LiveHeatEntrySchema),
});

export type LiveHeatResponse = z.infer<typeof LiveHeatResponseSchema>;

export const LiveAnnouncementSchema = z.object({
  id: z.number().int().positive(),
  message: z.string().min(1).max(1000),
  display_mode: z.enum(['fullscreen', 'ticker']),
  is_active: z.boolean(),
  revision: z.number().int().positive(),
  created_at: z.string(),
  updated_at: z.string(),
  activated_at: z.string().nullable(),
});

export const LiveAnnouncementResponseSchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  announcement: LiveAnnouncementSchema.nullable(),
});

export type LiveAnnouncementResponse = z.infer<typeof LiveAnnouncementResponseSchema>;

export const LiveAnnouncementsResponseSchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  announcements: z.array(LiveAnnouncementSchema),
});

export type LiveAnnouncementsResponse = z.infer<typeof LiveAnnouncementsResponseSchema>;

export type LiveAnnouncementCreate = {
  message: string;
  display_mode: 'fullscreen' | 'ticker';
  expected_revision: 0;
};

export type LiveAnnouncementUpdate = Omit<LiveAnnouncementCreate, 'expected_revision'> & {
  expected_revision: number;
};

export type LiveAnnouncementActivation = {
  is_active: boolean;
  expected_revision: number;
};

export const OperatorSessionResponseSchema = z.object({
  authenticated: z.literal(true),
  expires_in_seconds: z.number().int().positive(),
});

export const LiveHeatUpdateStateSchema = z.object({
  publication_id: z.number().int().positive(),
  stage_number: z.number().int().positive(),
  pool_role: z.enum(['main', 'competition', 'training']),
  session_number: z.number().int().positive(),
  event_number: z.number().int().positive(),
  heat_number: z.number().int().positive(),
  status: z.enum(['not_started', 'active', 'paused', 'finished']),
  revision: z.number().int().positive(),
  updated_at: z.string(),
});

export const LiveHeatUpdateResponseSchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  state: LiveHeatUpdateStateSchema,
});

export type LiveHeatUpdate = {
  publication_id: number;
  stage_number: number;
  pool_role: 'main' | 'competition' | 'training';
  session_number: number;
  event_number: number;
  heat_number: number;
  status: 'not_started' | 'active' | 'paused' | 'finished';
  expected_revision: number;
};

export const CompetitionClubMedalEntrySchema = z.object({
  club_id: z.union([z.string(), z.number()]),
  club_name: z.string(),
  gold_medals: z.number().int(),
  silver_medals: z.number().int(),
  bronze_medals: z.number().int(),
  total_medals: z.number().int(),
});

export type CompetitionClubMedalEntry = z.infer<typeof CompetitionClubMedalEntrySchema>;

export const CompetitionClubPointsEntrySchema = z.object({
  club_id: z.union([z.string(), z.number()]),
  club_name: z.string(),
  individual_points: z.number().int(),
  relay_points: z.number().int(),
  total_points: z.number().int(),
});

export type CompetitionClubPointsEntry = z.infer<typeof CompetitionClubPointsEntrySchema>;

export const CompetitionStatsSchema = z.object({
  participants_count: z.number().int(),
  women_count: z.number().int(),
  men_count: z.number().int(),
  clubs_count: z.number().int(),
  dsq_count: z.number().int(),
  valid_results_count: z.number().int(),
  entries_count: z.number().int(),
  events_count: z.number().int(),
  club_medal_table: z.array(CompetitionClubMedalEntrySchema).default([]),
  club_points_table: z.array(CompetitionClubPointsEntrySchema).default([]),
  premaster_club_medal_table: z.array(CompetitionClubMedalEntrySchema).default([]),
  premaster_club_points_table: z.array(CompetitionClubPointsEntrySchema).default([]),
});

export type CompetitionStats = z.infer<typeof CompetitionStatsSchema>;
