import { createBrowserRouter } from 'react-router-dom';
import { MainLayout } from '../components/layout/MainLayout';

// Páginas
import { AthletesPage } from '../features/athletes/pages/AthletesPage';
import { AthleteProfilePage } from '../features/athletes/pages/AthleteProfilePage';
import { ClubsPage } from '../features/clubs/pages/ClubsPage';
import { ClubProfilePage } from '../features/clubs/pages/ClubProfilePage';
import { CompetitionsPage } from '../features/competitions/pages/CompetitionsPage';

const PlaceholderPage = ({ title }: { title: string }) => (
  <div className="py-10 px-4 max-w-7xl mx-auto">
    <h1 className="text-3xl font-bold text-slate-900 mb-6">{title}</h1>
    <div className="bg-slate-50 border border-slate-200 rounded-xl p-8 text-center text-slate-500">
      Módulo en construcción: Esta sección se implementará más adelante.
    </div>
  </div>
);

export const router = createBrowserRouter([
  {
    path: '/',
    element: <MainLayout />,
    children: [
      {
        index: true,
        element: <AthletesPage />,
      },
      {
        path: 'athletes/:id',
        element: <AthleteProfilePage />,
      },
      {
        path: 'clubs',
        element: <ClubsPage />,
      },
      {
        path: 'clubs/:id',
        element: <ClubProfilePage />,
      },
      {
        path: 'competitions',
        element: <CompetitionsPage />,
      },
      {
        path: 'competitions/:id',
        element: <PlaceholderPage title="Resultados de la Competencia" />,
      },
    ],
  },
]);
