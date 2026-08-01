import { z } from 'zod';

export const SupabasePasswordResponseSchema = z.object({
  access_token: z.string().min(1),
});

export const AdminSessionResponseSchema = z.object({
  authenticated: z.literal(true),
  expires_in_seconds: z.number().int().positive(),
});
