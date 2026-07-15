import type { CourseType } from './schemas/canon';

const courseMeta: Record<CourseType, { label: string; description: string; light: string; dark: string }> = {
  scm: {
    label: 'SCM',
    description: 'Piscina corta (25m)',
    light: 'bg-course-scm/10 text-course-scm border-course-scm/30',
    dark: 'bg-course-scm/20 text-brand-white border-course-scm/40',
  },
  lcm: {
    label: 'LCM',
    description: 'Piscina larga (50m)',
    light: 'bg-course-lcm/10 text-course-lcm border-course-lcm/30',
    dark: 'bg-course-lcm/20 text-brand-white border-course-lcm/40',
  },
  owy: {
    label: 'OWY',
    description: 'Aguas abiertas',
    light: 'bg-course-open/10 text-course-open border-course-open/30',
    dark: 'bg-course-open/20 text-brand-white border-course-open/40',
  },
  unknown: {
    label: 'N/D',
    description: 'Piscina desconocida',
    light: 'bg-canvas text-content-muted border-line',
    dark: 'bg-brand-steel/20 text-brand-muted border-brand-subtle/40',
  },
};

export const getCourseMeta = (courseType?: CourseType | null) => courseMeta[courseType || 'unknown'];
