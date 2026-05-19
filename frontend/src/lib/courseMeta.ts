import type { CourseType } from './schemas/canon';

const courseMeta: Record<CourseType, { label: string; description: string; light: string; dark: string }> = {
  scm: {
    label: 'SCM',
    description: 'Piscina corta (25m)',
    light: 'bg-blue-50 text-blue-700 border-blue-200',
    dark: 'bg-blue-500/20 text-blue-200 border-blue-400/40',
  },
  lcm: {
    label: 'LCM',
    description: 'Piscina larga (50m)',
    light: 'bg-violet-50 text-violet-700 border-violet-200',
    dark: 'bg-violet-500/20 text-violet-200 border-violet-400/40',
  },
  owy: {
    label: 'OWY',
    description: 'Aguas abiertas',
    light: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    dark: 'bg-emerald-500/20 text-emerald-200 border-emerald-400/40',
  },
  unknown: {
    label: 'N/D',
    description: 'Piscina desconocida',
    light: 'bg-slate-50 text-slate-600 border-slate-200',
    dark: 'bg-slate-500/20 text-slate-200 border-slate-400/40',
  },
};

export const getCourseMeta = (courseType?: CourseType | null) => courseMeta[courseType || 'unknown'];
