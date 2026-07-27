import { useState } from 'react';
import { Link } from 'react-router-dom';
import type {
  CompetitionClubMedalEntry,
  CompetitionClubPointsEntry,
} from '../../../lib/schemas/competition';

type ClassificationTab = 'medals' | 'points' | 'premaster-medals' | 'premaster-points';

type CompetitionClubClassificationProps = {
  clubMedals: CompetitionClubMedalEntry[];
  clubPoints: CompetitionClubPointsEntry[];
  premasterClubMedals: CompetitionClubMedalEntry[];
  premasterClubPoints: CompetitionClubPointsEntry[];
};

type MedalTableProps = {
  clubs: CompetitionClubMedalEntry[];
  tableId: string;
  descriptionId: string;
};

type PointsTableProps = {
  clubs: CompetitionClubPointsEntry[];
  tableId: string;
  descriptionId: string;
};

const MedalTable = ({ clubs, tableId, descriptionId }: MedalTableProps) => (
  <div className="overflow-x-auto border-t border-line">
    <table id={tableId} className="w-full min-w-[40rem] text-left text-sm" aria-describedby={descriptionId}>
      <thead className="border-b border-line bg-canvas text-xs font-bold uppercase tracking-widest text-content-subtle">
        <tr>
          <th scope="col" className="w-20 px-4 py-3 text-center">Pos.</th>
          <th scope="col" className="px-4 py-3">Club</th>
          <th scope="col" className="px-4 py-3 text-right text-medal-gold">Oro</th>
          <th scope="col" className="px-4 py-3 text-right text-medal-silver">Plata</th>
          <th scope="col" className="px-4 py-3 text-right text-medal-bronze">Bronce</th>
          <th scope="col" className="px-4 py-3 text-right">Total</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-line">
        {clubs.map((club, index) => (
          <tr key={club.club_id} className="transition-colors hover:bg-canvas">
            <td className="px-4 py-3 text-center font-bold text-content-subtle">{index + 1}</td>
            <th scope="row" className="px-4 py-3">
              <Link to={`/clubs/${club.club_id}`} className="font-semibold text-action hover:text-brand-steel hover:underline">
                {club.club_name}
              </Link>
            </th>
            <td className="px-4 py-3 text-right font-bold text-medal-gold">{club.gold_medals}</td>
            <td className="px-4 py-3 text-right font-bold text-medal-silver">{club.silver_medals}</td>
            <td className="px-4 py-3 text-right font-bold text-medal-bronze">{club.bronze_medals}</td>
            <td className="px-4 py-3 text-right font-black text-ink">{club.total_medals}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

const PointsTable = ({ clubs, tableId, descriptionId }: PointsTableProps) => (
  <div className="overflow-x-auto border-t border-line">
    <table id={tableId} className="w-full min-w-[40rem] text-left text-sm" aria-describedby={descriptionId}>
      <thead className="border-b border-line bg-canvas text-xs font-bold uppercase tracking-widest text-content-subtle">
        <tr>
          <th scope="col" className="w-20 px-4 py-3 text-center">Pos.</th>
          <th scope="col" className="px-4 py-3">Club</th>
          <th scope="col" className="px-4 py-3 text-right">Individuales</th>
          <th scope="col" className="px-4 py-3 text-right">Relevos</th>
          <th scope="col" className="px-4 py-3 text-right">Total</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-line">
        {clubs.map((club, index) => (
          <tr key={club.club_id} className="transition-colors hover:bg-canvas">
            <td className="px-4 py-3 text-center font-bold text-content-subtle">{index + 1}</td>
            <th scope="row" className="px-4 py-3">
              <Link to={`/clubs/${club.club_id}`} className="font-semibold text-action hover:text-brand-steel hover:underline">
                {club.club_name}
              </Link>
            </th>
            <td className="px-4 py-3 text-right font-semibold text-ink">{club.individual_points}</td>
            <td className="px-4 py-3 text-right font-semibold text-ink">{club.relay_points}</td>
            <td className="px-4 py-3 text-right font-black text-ink">{club.total_points}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

export const CompetitionClubClassification = ({
  clubMedals,
  clubPoints,
  premasterClubMedals,
  premasterClubPoints,
}: CompetitionClubClassificationProps) => {
  const [activeTab, setActiveTab] = useState<ClassificationTab>('medals');
  const [expandedTabs, setExpandedTabs] = useState<Partial<Record<ClassificationTab, boolean>>>({});

  const tabs = [
    { id: 'medals' as const, label: 'Medallero', data: clubMedals, kind: 'medals' as const, premaster: false },
    { id: 'points' as const, label: 'Puntuación', data: clubPoints, kind: 'points' as const, premaster: false },
    { id: 'premaster-medals' as const, label: 'Medallero Pre-Máster', data: premasterClubMedals, kind: 'medals' as const, premaster: true },
    { id: 'premaster-points' as const, label: 'Puntuación Pre-Máster', data: premasterClubPoints, kind: 'points' as const, premaster: true },
  ];
  const active = tabs.find(tab => tab.id === activeTab) ?? tabs[0];
  const isExpanded = Boolean(expandedTabs[active.id]);
  const visibleClubs = isExpanded ? active.data : active.data.slice(0, 10);
  const tableId = `club-${active.id}-table`;
  const descriptionId = `club-${active.id}-description`;

  return (
    <section className="space-y-4" aria-labelledby="club-classification-heading">
      <h2 id="club-classification-heading" className="text-2xl font-bold tracking-tight text-ink">
        Clasificación de Clubes
      </h2>

      <div className="overflow-hidden rounded-xl border border-line bg-surface shadow-sm">
        <div className="overflow-x-auto border-b border-line px-4 pt-3">
          <div role="tablist" aria-label="Clasificación de clubes" className="flex min-w-max gap-1">
            {tabs.map(tab => (
              <button
                key={tab.id}
                type="button"
                id={`club-${tab.id}-tab`}
                role="tab"
                aria-selected={activeTab === tab.id}
                aria-controls={`club-${tab.id}-panel`}
                onClick={() => setActiveTab(tab.id)}
                className={`whitespace-nowrap border-b-2 px-4 py-2 text-sm font-semibold transition-colors ${
                  activeTab === tab.id
                    ? 'border-action text-action'
                    : 'border-transparent text-content-subtle hover:text-ink'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        <div
          id={`club-${active.id}-panel`}
          role="tabpanel"
          aria-labelledby={`club-${active.id}-tab`}
        >
          <p id={descriptionId} className="px-4 py-3 text-sm text-content-subtle">
            {active.premaster
              ? 'Clasificación calculada exclusivamente con categorías Pre-Máster.'
              : 'Clasificación Máster; las categorías Pre-Máster se calculan por separado.'}
            {active.kind === 'points' && (
              <> Los puntos se recalculan desde las posiciones, independientemente de los puntos de la fuente.</>
            )}
          </p>

          {active.data.length === 0 ? (
            <p className="border-t border-line px-4 py-4 text-sm text-content-subtle">
              No hay {active.kind === 'medals' ? 'medallas' : 'puntos'} de clubes para esta clasificación.
            </p>
          ) : (
            <>
              {active.kind === 'medals' ? (
                <MedalTable
                  clubs={visibleClubs as CompetitionClubMedalEntry[]}
                  tableId={tableId}
                  descriptionId={descriptionId}
                />
              ) : (
                <PointsTable
                  clubs={visibleClubs as CompetitionClubPointsEntry[]}
                  tableId={tableId}
                  descriptionId={descriptionId}
                />
              )}
              {active.data.length > 10 && (
                <div className="border-t border-line px-4 py-3 text-center">
                  <button
                    type="button"
                    onClick={() => setExpandedTabs(current => ({
                      ...current,
                      [active.id]: !isExpanded,
                    }))}
                    aria-expanded={isExpanded}
                    aria-controls={tableId}
                    className="rounded-lg border border-line bg-surface px-4 py-2 text-sm font-semibold text-action transition-colors hover:bg-canvas hover:text-brand-steel"
                  >
                    {isExpanded ? 'Ver menos' : 'Ver más'}
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
};
