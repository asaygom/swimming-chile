import type { CourseType } from '../../lib/schemas/canon';
import { getCourseMeta } from '../../lib/courseMeta';

type CourseBadgeVariant = 'light' | 'dark' | 'compact';

type CourseBadgeProps = {
  courseType?: CourseType | null;
  variant?: CourseBadgeVariant;
};

export const CourseBadge = ({ courseType, variant = 'light' }: CourseBadgeProps) => {
  const course = getCourseMeta(courseType);
  const palette = variant === 'dark' ? course.dark : course.light;
  const padding = variant === 'compact' ? 'px-2 py-0.5' : 'px-2.5 py-0.5';

  return (
    <span
      className={`inline-flex w-fit items-center rounded-full border text-xs font-bold uppercase tracking-wider ${padding} ${palette}`}
      title={course.description}
    >
      {course.label}
    </span>
  );
};
