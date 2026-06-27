import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';

export const ClubAttendanceCompetitionSchema = z.object({
  id: z.union([z.string(), z.number()]),
  name: z.string(),
  date: z.string().nullable().optional(),
});

export const ClubAttendanceEntrySchema = z.object({
  competition_id: z.union([z.string(), z.number()]),
  entries: z.number().int(),
  status: z.enum(['attended', 'no_show']),
});

export const ClubAttendanceAthleteSchema = z.object({
  athlete_id: z.union([z.string(), z.number()]),
  athlete_name: z.string(),
  competitions: z.array(ClubAttendanceEntrySchema),
});

export const ClubAttendanceMatrixSchema = z.object({
  competitions: z.array(ClubAttendanceCompetitionSchema),
  athletes: z.array(ClubAttendanceAthleteSchema),
});

export const ClubSchema = z.object({
  id: z.union([z.string(), z.number()]),
  name: z.string(),
  city: z.string().nullable().optional(),
  country: z.string().nullable().optional(),
  total_athletes: z.number().int().nullable().optional(),
  attendance_matrix: ClubAttendanceMatrixSchema.optional(),
});

export type Club = z.infer<typeof ClubSchema>;

export const ClubsResponseSchema = z.object({
  data: z.array(ClubSchema),
  meta: PaginationMetaSchema,
});

export type ClubsResponse = z.infer<typeof ClubsResponseSchema>;

import { AthleteSchema } from './athlete';

export const ClubProfileSchema = z.object({
  club: ClubSchema,
  athletes: z.array(AthleteSchema),
});

export type ClubProfile = z.infer<typeof ClubProfileSchema>;
