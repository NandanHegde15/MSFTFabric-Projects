import type { ReactNode } from 'react';

export function PageHeader({
  title,
  lead,
  actions,
}: {
  title: string;
  lead?: string;
  actions?: ReactNode;
}) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
          {title}
        </h1>
        {lead && (
          <p className="mt-1.5 max-w-2xl text-sm leading-relaxed text-slate-600">
            {lead}
          </p>
        )}
      </div>
      {actions && <div className="flex flex-wrap gap-2">{actions}</div>}
    </div>
  );
}

export function Bar({
  value,
  label,
  tone = 'accent',
}: {
  /** 0-100 */
  value: number;
  label?: string;
  tone?: 'accent' | 'slate';
}) {
  const pct = Math.max(0, Math.min(100, value));
  return (
    <div>
      {label && (
        <div className="mb-1 flex items-baseline justify-between text-xs text-slate-500">
          <span>{label}</span>
          <span className="tabular-nums font-medium text-slate-700">{pct}%</span>
        </div>
      )}
      <div
        className="h-2 w-full overflow-hidden rounded-full bg-slate-200"
        role="progressbar"
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label={label}
      >
        <div
          className={
            tone === 'accent' ? 'accent-bg h-full rounded-full' : 'h-full rounded-full bg-slate-500'
          }
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

export function Stat({
  label,
  value,
  hint,
}: {
  label: string;
  value: string | number;
  hint?: string;
}) {
  return (
    <div className="card p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
        {label}
      </p>
      <p className="mt-1.5 text-2xl font-semibold tabular-nums text-slate-900">
        {value}
      </p>
      {hint && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
    </div>
  );
}

export function Chip({
  children,
  tone = 'slate',
}: {
  children: ReactNode;
  tone?: 'slate' | 'accent' | 'green' | 'red' | 'amber';
}) {
  const tones: Record<string, string> = {
    slate: 'bg-slate-100 text-slate-600 border-slate-200',
    accent: 'accent-soft-bg accent-text accent-border',
    green: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    red: 'bg-rose-50 text-rose-700 border-rose-200',
    amber: 'bg-amber-50 text-amber-800 border-amber-200',
  };
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium ${tones[tone]}`}
    >
      {children}
    </span>
  );
}

export function EmptyState({
  title,
  body,
  action,
}: {
  title: string;
  body: string;
  action?: ReactNode;
}) {
  return (
    <div className="card flex flex-col items-center gap-3 px-6 py-14 text-center">
      <p className="text-sm font-medium text-slate-800">{title}</p>
      <p className="max-w-md text-sm text-slate-500">{body}</p>
      {action}
    </div>
  );
}
