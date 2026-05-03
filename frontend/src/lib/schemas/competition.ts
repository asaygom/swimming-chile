import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';
import { CourseTypeSchema } from './canon';

export const CompetitionSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  date_start: z.string().datetime(),
  date_end: z.string().datetime().optional(),
  location: z.string().optional(),
  course_type: CourseTypeSchema,
});

export type Competition = z.infer<typeof CompetitionSchema>;

export const CompetitionsResponseSchema = z.object({
  data: z.array(CompetitionSchema),
  meta: PaginationMetaSchema,
});

export type CompetitionsResponse = z.infer<typeof CompetitionsResponseSchema>;
