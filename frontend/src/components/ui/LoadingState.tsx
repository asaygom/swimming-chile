import React from 'react';

export const LoadingState: React.FC = () => {
  return (
    <div className="flex flex-col items-center justify-center p-12 text-slate-500">
      <div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mb-4"></div>
      <p className="text-sm font-medium">Cargando información...</p>
    </div>
  );
};
