import React from 'react';

interface ErrorStateProps {
  message?: string;
  onRetry?: () => void;
}

export const ErrorState: React.FC<ErrorStateProps> = ({ 
  message = "Ocurrió un error inesperado al procesar la solicitud.",
  onRetry 
}) => {
  return (
    <div className="flex flex-col items-center justify-center p-12 bg-red-50 rounded-lg border border-red-100">
      <svg className="w-12 h-12 mb-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
      </svg>
      <h3 className="text-lg font-semibold text-red-800 mb-2">Error de comunicación</h3>
      <p className="text-sm text-red-600 text-center max-w-md">{message}</p>
      {onRetry && (
        <button 
          onClick={onRetry}
          className="mt-6 px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-md shadow-sm hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
        >
          Intentar de nuevo
        </button>
      )}
    </div>
  );
};
