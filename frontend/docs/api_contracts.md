# Contratos de API (Frontend -> Backend)

Este documento centraliza los esquemas de comunicación que el frontend espera recibir del backend mediante **FastAPI**.

## 1. Atletas (Athletes)

### `GET /api/athletes`
Buscador general de atletas.

**Query Params:**
- `query` (string, opcional): Búsqueda difusa por nombre o alias.
- `gender` (enum: 'male', 'female', opcional).

**Response (200 OK):**
```json
{
  "data": [
    {
      "id": "uuid",
      "canonical_name": "Apellido, Nombre",
      "gender": "male",
      "birth_year": 1990
    }
  ],
  "meta": { "total_results": 1 }
}
```
*(Más endpoints por definir en la Fase FE3)*
