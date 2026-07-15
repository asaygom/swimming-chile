import React from 'react';
import { Link } from 'react-router-dom';

const primaryActions = [
  {
    title: 'Resultados por atleta',
    description: 'Encuentra nadadores y revisa su historial competitivo.',
    to: '/athletes',
  },
  {
    title: 'Ver clubes',
    description: 'Explora clubes y asistencia a competencias.',
    to: '/clubs',
  },
  {
    title: 'Resultados por competencia',
    description: 'Consulta competencias y resultados cargados.',
    to: '/competitions',
  },
  {
    title: 'Calendario',
    description: 'Revisa próximas competencias disponibles.',
    to: '/calendar',
  },
];

export const HomePage: React.FC = () => (
  <div className="space-y-10 bg-brand-night px-4 py-8 sm:px-6 lg:px-8">
    <section className="mx-auto max-w-7xl overflow-hidden rounded-3xl border border-brand-steel bg-brand-navy shadow-sm">
      <div className="grid gap-8 p-6 md:grid-cols-[1.2fr_0.8fr] md:p-10">
        <div className="flex flex-col justify-center">
          <span className="mb-4 inline-flex w-fit rounded-full bg-brand-cyan px-3 py-1 text-sm font-semibold text-brand-night ring-1 ring-brand-cyan/20">
            Plataforma de datos de natación master
          </span>
          <h1 className="text-4xl font-bold tracking-tight text-brand-white md:text-5xl">
            SwimStats Chile
          </h1>
          <p className="mt-4 max-w-2xl text-lg leading-8 text-brand-muted">
            Explora atletas, clubes, competencias y resultados históricos de natación en Chile desde una interfaz simple y orientada a datos.
          </p>
          <p className="mt-3 max-w-2xl text-sm text-brand-subtle">
            Proyecto independiente y no oficial. Los datos se construyen desde resultados públicos y procesos de validación propios.
          </p>
          <div className="mt-8 flex flex-col gap-3 sm:flex-row">
            <Link
              to="/athletes"
              className="inline-flex items-center justify-center rounded-xl bg-brand-cyan px-5 py-3 text-sm font-semibold text-brand-night shadow-sm transition-colors hover:bg-brand-turquoise"
            >
              Ver resultados por atleta
            </Link>
            <Link
              to="/competitions"
              className="inline-flex items-center justify-center rounded-xl border border-brand-steel bg-transparent px-5 py-3 text-sm font-semibold text-brand-white shadow-sm transition-colors hover:border-brand-cyan hover:text-brand-cyan"
            >
              Ver resultados por competencia
            </Link>
          </div>
        </div>

        <div className="hidden rounded-2xl bg-gradient-to-br from-brand-panel via-brand-navy to-brand-steel p-6 ring-1 ring-brand-steel md:block">
          <div className="grid h-full gap-4">
            <div className="rounded-2xl bg-brand-panel/80 p-4 shadow-sm">
              <p className="text-sm font-semibold text-brand-white">Datos centralizados</p>
              <p className="mt-1 text-sm text-brand-muted">Resultados, atletas y clubes en un solo lugar.</p>
            </div>
            <div className="rounded-2xl bg-brand-panel/80 p-4 shadow-sm">
              <p className="text-sm font-semibold text-brand-white">Historial competitivo</p>
              <p className="mt-1 text-sm text-brand-muted">Consulta marcas, competencias y evolución.</p>
            </div>
            <div className="rounded-2xl bg-brand-panel/80 p-4 shadow-sm">
              <p className="text-sm font-semibold text-brand-white">Exploración por club</p>
              <p className="mt-1 text-sm text-brand-muted">Visualiza planteles, asistencia y participación.</p>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section className="mx-auto hidden max-w-7xl gap-4 md:grid md:grid-cols-4">
      {primaryActions.map(action => (
        <Link
          key={action.to}
          to={action.to}
          className="group rounded-2xl border border-brand-steel bg-brand-panel p-5 shadow-sm transition-all hover:-translate-y-0.5 hover:border-brand-cyan hover:shadow-md"
        >
          <h2 className="font-bold text-brand-white group-hover:text-brand-cyan">{action.title}</h2>
          <p className="mt-2 text-sm leading-6 text-brand-muted">{action.description}</p>
        </Link>
      ))}
    </section>
  </div>
);
