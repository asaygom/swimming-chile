# Natación Chile - Frontend

Este es el frontend oficial del ecosistema de datos de competencias máster de Natación Chile.

Está construido como una **Aplicación de Página Única (SPA)** utilizando **React**, **TypeScript** y **Vite**, y estilizado con **Tailwind CSS v4**.

## Arquitectura y Metodología

El desarrollo del frontend sigue las mismas directivas estrictas del backend:
- **Contract-First:** La interfaz no se comunica con la base de datos de forma directa. Todo componente consume datos a través de contratos API estrictos expuestos por FastAPI.
- **Mocks & Fixtures:** Las interfaces se construyen y prueban primero contra respuestas predefinidas (mocks) en `src/test/fixtures/` antes de consumir endpoints reales.
- **Sin Lógica de Negocio:** El frontend actúa puramente como una capa de presentación (Data Product). La curaduría de entidades (ej. aliases de atletas) ocurre exclusivamente en el backend.

## Stack Tecnológico

- **Framework:** React + TypeScript + Vite
- **Estilos:** Tailwind CSS v4
- **Enrutamiento:** React Router v7+
- **Manejo de Estado/Datos:** TanStack Query
- **Validación de Esquemas:** Zod
- **Testing:** Vitest + Testing Library

## Desarrollo Local

Para levantar el entorno de desarrollo:

```bash
npm install
npm run dev
```

## Documentación Interna

Para entender los procesos operativos de desarrollo, por favor consulta:
- `docs/ui_workflow.md` (Metodología y reglas de trabajo UI)
- `docs/api_contracts.md` (Contratos esperados de la API)
