import { createBrowserRouter } from 'react-router-dom';
import { MainLayout } from '../components/layout/MainLayout';

// Páginas
import { AthletesPage } from '../features/athletes/pages/AthletesPage';
import { AthleteProfilePage } from '../features/athletes/pages/AthleteProfilePage';
import { ClubsPage } from '../features/clubs/pages/ClubsPage';
import { ClubProfilePage } from '../features/clubs/pages/ClubProfilePage';
import { CompetitionsPage } from '../features/competitions/pages/CompetitionsPage';
import { CompetitionProfilePage } from '../features/competitions/pages/CompetitionProfilePage';
import { CompetitionLiveHeatPage } from '../features/competitions/pages/CompetitionLiveHeatPage';
import { CompetitionLiveHeatControlPage } from '../features/competitions/pages/CompetitionLiveHeatControlPage';
import { CompetitionLiveAnnouncementAdminPage } from '../features/competitions/pages/CompetitionLiveAnnouncementAdminPage';
import { CompetitionMeetProgramAdminPage } from '../features/competitions/pages/CompetitionMeetProgramAdminPage';
import { RelaysPage } from '../features/relays/pages/RelaysPage';
import { HomePage } from '../features/home/pages/HomePage';
import { RankingsPage } from '../features/rankings/pages/RankingsPage';

export const router = createBrowserRouter([
  {
    path: '/competitions/:id/live',
    element: <CompetitionLiveHeatPage />,
  },
  {
    path: '/competitions/:id/live/control',
    element: <CompetitionLiveHeatControlPage />,
  },
  {
    path: '/competitions/:id/live/admin',
    element: <CompetitionLiveAnnouncementAdminPage />,
  },
  {
    path: '/competitions/:id/live/program',
    element: <CompetitionMeetProgramAdminPage />,
  },
  {
    path: '/',
    element: <MainLayout />,
    children: [
      {
        index: true,
        element: <HomePage />,
      },
      {
        path: 'athletes',
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
        path: 'calendar',
        element: <CompetitionsPage mode="upcoming" />,
      },
      {
        path: 'competitions',
        element: <CompetitionsPage mode="past" />,
      },
      {
        path: 'competitions/:id',
        element: <CompetitionProfilePage />,
      },
      {
        path: 'relays',
        element: <RelaysPage />,
      },
      {
        path: 'rankings',
        element: <RankingsPage />,
      },
    ],
  },
]);
