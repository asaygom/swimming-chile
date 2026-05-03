import { z } from 'zod';
import { PaginationMetaSchema } from './athlete';

export const ClubSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  city: z.string().optional(),
  country: z.string().optional(),
  total_athletes: z.number().int().optional(),
});

export type Club = z.infer<typeof ClubSchema>;

export const ClubsResponseSchema = z.object({
  data: z.array(ClubSchema),
  meta: PaginationMetaSchema,
});

export type ClubsResponse = z.infer<typeof ClubsResponseSchema>;
