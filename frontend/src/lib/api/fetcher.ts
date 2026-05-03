import { z } from 'zod';

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = 'ApiError';
  }
}

interface FetchOptions<T> extends RequestInit {
  schema?: z.ZodType<T>;
}

// URL base configurable
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

/**
 * Cliente HTTP genérico que obliga a validar las respuestas contra un esquema Zod (Contract-First).
 */
export async function apiFetch<T>(endpoint: string, options: FetchOptions<T>): Promise<T> {
  const { schema, ...init } = options;
  const url = `${API_BASE_URL}${endpoint}`;

  const response = await fetch(url, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...init.headers,
    },
  });

  if (!response.ok) {
    throw new ApiError(response.status, `Error fetching ${url}: ${response.statusText}`);
  }

  // Si no pasamos esquema (ej. llamadas que no nos importan validar o devuelven 204), retornamos null
  if (!schema) {
    return null as unknown as T;
  }

  const data = await response.json();
  
  // Parseo estricto: lanzará error si el backend rompe el contrato (schema)
  const parsedResult = schema.safeParse(data);
  if (!parsedResult.success) {
    console.error('Contract Violation:', parsedResult.error);
    throw new Error('La respuesta de la API no cumple con el contrato esperado.');
  }

  return parsedResult.data;
}
