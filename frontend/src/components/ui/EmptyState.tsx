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
    <div className="flex flex-col items-center justify-center p-12 text-slate-500 text-center bg-white rounded-lg border border-slate-100 shadow-sm">
      <svg className="w-16 h-16 mb-4 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <h3 className="text-lg font-semibold text-slate-800">{title}</h3>
      <p className="mt-1 text-sm text-slate-500 max-w-sm">{description}</p>
    </div>
  );
};
