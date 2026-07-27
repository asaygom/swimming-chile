import type { Competition } from '../../lib/schemas/competition';

type GoverningBodyBadgeProps = {
  competition: Competition;
  variant?: 'light' | 'dark';
};

export const GoverningBodyBadge = ({
  competition,
  variant = 'light',
}: GoverningBodyBadgeProps) => {
  const governingBodyCode = competition.governing_body_code?.trim().toLowerCase();
  const label = competition.governing_body_code?.trim().toUpperCase()
    || competition.governing_body_name?.trim()
    || competition.organizer?.trim();

  if (!label) return null;

  const colorClass = variant === 'dark'
    ? governingBodyCode === 'fchmn'
      ? 'border-brand-cyan/40 bg-brand-cyan/15 text-brand-cyan'
      : governingBodyCode === 'fechida'
        ? 'border-danger/40 bg-danger/15 text-danger'
        : 'border-brand-steel bg-brand-panel text-brand-muted'
    : governingBodyCode === 'fchmn'
      ? 'border-action/30 bg-action/10 text-action'
      : governingBodyCode === 'fechida'
        ? 'border-danger/30 bg-danger/10 text-danger-strong'
        : 'border-line bg-canvas text-content-muted';

  return (
    <span className={`inline-flex w-fit items-center rounded-full border px-2.5 py-1 text-xs font-semibold ${colorClass}`}>
      {label}
    </span>
  );
};
