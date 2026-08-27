import { useEffect } from 'react';
import { NavLink, Outlet } from 'react-router-dom';

import { EXAM_IDS, type ExamId } from '@/data/exams';
import { dueCount, streak, useExam, useProgress } from '@/lib/progress';

const NAV = [
  { to: '/', label: 'Dashboard', end: true },
  { to: '/explore', label: 'Ecosystem' },
  { to: '/flashcards', label: 'Flashcards' },
  { to: '/quiz', label: 'Quiz' },
  { to: '/spot', label: 'Spot the error' },
  { to: '/castle', label: 'Castle journey' },
  { to: '/capacity', label: 'Capacity lab' },
  { to: '/leaderboard', label: 'Leaderboard' },
  { to: '/progress', label: 'Progress' },
];

export function Layout() {
  const [exam, setExamId] = useExam();
  const progress = useProgress();
  const days = streak(progress.days);
  const due = dueCount(progress, exam);

  useEffect(() => {
    document.documentElement.dataset.exam = exam;
    document.title = `FabricQuiz — ${exam} practice`;
  }, [exam]);

  return (
    <div className="min-h-screen flex flex-col">
      <header className="sticky top-0 z-20 border-b border-slate-200 bg-white/85 backdrop-blur">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-x-6 gap-y-3 px-4 py-3 sm:px-6">
          <NavLink to="/" className="flex items-center gap-2.5">
            <span
              className="accent-bg flex h-8 w-8 items-center justify-center rounded-lg text-sm font-bold text-white"
              aria-hidden
            >
              F
            </span>
            <span className="text-[15px] font-semibold tracking-tight text-slate-900">
              FabricQuiz
            </span>
          </NavLink>

          <ExamSwitch exam={exam} onChange={setExamId} />

          <div className="ml-auto flex items-center gap-2 text-xs text-slate-500">
            <NavLink
              to="/leaderboard"
              className="flex items-center gap-1.5 rounded-full border border-slate-200 px-2.5 py-1 font-medium text-slate-600 transition-colors hover:bg-slate-50"
            >
              {progress.learner ? (
                <>
                  <span aria-hidden>{progress.learner.emblem}</span>
                  <span className="max-w-[9rem] truncate">
                    {progress.learner.handle}
                  </span>
                </>
              ) : (
                <span>Create a player</span>
              )}
            </NavLink>
            {due > 0 && (
              <span className="accent-soft-bg accent-text accent-border rounded-full border px-2.5 py-1 font-medium">
                {due} card{due === 1 ? '' : 's'} due
              </span>
            )}
            <span className="rounded-full border border-slate-200 px-2.5 py-1 font-medium text-slate-600">
              {days === 0 ? 'No streak yet' : `${days}-day streak`}
            </span>
          </div>
        </div>

        <nav className="mx-auto max-w-6xl px-4 sm:px-6">
          <ul className="-mb-px flex gap-1 overflow-x-auto">
            {NAV.map((item) => (
              <li key={item.to}>
                <NavLink
                  to={item.to}
                  end={item.end}
                  className={({ isActive }) =>
                    [
                      'inline-block whitespace-nowrap border-b-2 px-3 py-2.5 text-sm transition-colors',
                      isActive
                        ? 'accent-text border-current font-medium'
                        : 'border-transparent text-slate-500 hover:text-slate-800',
                    ].join(' ')
                  }
                >
                  {item.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>
      </header>

      <main className="mx-auto w-full max-w-6xl flex-1 px-4 py-8 sm:px-6">
        <Outlet />
      </main>

      <footer className="border-t border-slate-200 bg-white">
        <div className="mx-auto max-w-6xl px-4 py-6 text-xs leading-relaxed text-slate-500 sm:px-6">
          <p>
            An unofficial, community study aid for the Microsoft Fabric DP-600
            and DP-700 certifications. Not affiliated with or endorsed by
            Microsoft. Exam objectives and weightings change — always check the
            official skills outline before you book.
          </p>
          <p className="mt-2">
            Your progress is stored in this browser only. Nothing is uploaded
            unless you choose to publish a score to the community board.
          </p>
        </div>
      </footer>
    </div>
  );
}

function ExamSwitch({
  exam,
  onChange,
}: {
  exam: ExamId;
  onChange: (e: ExamId) => void;
}) {
  return (
    <div
      role="radiogroup"
      aria-label="Certification"
      className="flex rounded-xl border border-slate-200 bg-slate-100 p-0.5"
    >
      {EXAM_IDS.map((id) => {
        const active = id === exam;
        return (
          <button
            key={id}
            role="radio"
            aria-checked={active}
            onClick={() => onChange(id)}
            className={[
              'rounded-[10px] px-3 py-1.5 text-sm font-medium transition-colors',
              active
                ? 'bg-white text-slate-900 shadow-sm'
                : 'text-slate-500 hover:text-slate-700',
            ].join(' ')}
          >
            {id}
          </button>
        );
      })}
    </div>
  );
}
