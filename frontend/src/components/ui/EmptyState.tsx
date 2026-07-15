import React from 'react';

interface EmptyStateProps {
  title?: string;
  description?: string;
}

export const EmptyState: React.FC<EmptyStateProps> = ({ 
  title = "No hay resultados", 
  description = "No pudimos encontrar información con los filtros actuales." 
}) => {
  return (
    <div className="flex flex-col items-center justify-center p-12 text-content-muted text-center bg-surface rounded-lg border border-line shadow-sm">
      <svg className="w-16 h-16 mb-4 text-chart-axis" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <h3 className="text-lg font-semibold text-ink">{title}</h3>
      <p className="mt-1 text-sm text-content-muted max-w-sm">{description}</p>
    </div>
  );
};
