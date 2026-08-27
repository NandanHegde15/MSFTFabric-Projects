import { Link } from 'react-router-dom';

import { Bar, Chip, PageHeader, Stat } from '@/components/ui';
import { examById } from '@/data/exams';
import { FLASHCARDS } from '@/data/flashcards';
import { QUESTIONS } from '@/data/questions';
import { SCENARIOS } from '@/data/spotTheError';
import {
  domainStats,
  dueCount,
  overallMastery,
  streak,
  useExam,
  useProgress,
} from '@/lib/progress';

const TOOLS = [
  {
    to: '/explore',
    name: 'Ecosystem model',
    body: 'The whole Fabric platform in 3D. Take a workload apart to see its items, and those items to see their capabilities.',
  },
  {
    to: '/flashcards',
    name: 'Flashcards',
    body: 'Spaced repetition over the terms, numbers and gotchas that the exam actually leans on.',
  },
  {
    to: '/quiz',
    name: 'Quiz',
    body: 'Scenario questions drawn from the exam objective areas, with an explanation on every answer.',
  },
  {
    to: '/spot',
    name: 'Spot the error',
    body: 'A broken config, query or briefing note. Find the one line that is wrong.',
  },
  {
    to: '/castle',
    name: 'Castle journey',
    body: 'Six stages, three hearts, one dragon. Answer your way through the keep and fell it at the end.',
  },
  {
    to: '/capacity',
    name: 'Capacity lab',
    body: 'Work the CU maths: SKU sizing, smoothing, throttling thresholds and cost per run.',
  },
  {
    to: '/leaderboard',
    name: 'Leaderboard',
    body: 'Create a player, publish your runs and see how you rank against everyone else revising.',
  },
];

export function DashboardPage() {
  const [exam] = useExam();
  const progress = useProgress();
  const exams = examById(exam);
  const stats = domainStats(progress, exam);
  const mastery = overallMastery(stats);
  const due = dueCount(progress, exam);

  const cards = FLASHCARDS.filter((c) => c.exam === exam).length;
  const questions = QUESTIONS.filter((q) => q.exam === exam).length;
  const scenarios = SCENARIOS.filter((s) => s.exam === exam).length;

  const answered = stats.reduce((t, s) => t + s.questionsSeen + s.scenariosSeen, 0);
  const weakest = [...stats].sort((a, b) => a.mastery - b.mastery)[0];

  return (
    <>
      <PageHeader
        title={`${exam} — ${exams.certification}`}
        lead={exams.blurb}
        actions={
          <a
            className="btn btn-ghost"
            href={exams.skillsOutlineUrl}
            target="_blank"
            rel="noreferrer noopener"
          >
            Official skills outline
          </a>
        }
      />

      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Stat
          label="Overall readiness"
          value={`${mastery}%`}
          hint="Weighted by exam domain"
        />
        <Stat
          label="Items answered"
          value={answered}
          hint={`of ${questions + scenarios} in the bank`}
        />
        <Stat
          label="Cards due"
          value={due}
          hint={`${cards} cards for this exam`}
        />
        <Stat
          label="Study streak"
          value={streak(progress.days)}
          hint="consecutive days"
        />
      </section>

      <section className="mt-8">
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
          Practise
        </h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {TOOLS.map((t) => (
            <Link
              key={t.to}
              to={t.to}
              className="card group flex flex-col p-5 transition-shadow hover:shadow-md"
            >
              <span className="accent-text text-sm font-semibold">{t.name}</span>
              <span className="mt-2 text-sm leading-relaxed text-slate-600">
                {t.body}
              </span>
              <span className="accent-text mt-4 text-xs font-medium opacity-0 transition-opacity group-hover:opacity-100">
                Start →
              </span>
            </Link>
          ))}
        </div>
      </section>

      <section className="mt-8">
        <div className="mb-3 flex items-end justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-500">
            Exam domains
          </h2>
          <Link to="/progress" className="accent-text text-xs font-medium">
            Full progress →
          </Link>
        </div>

        <div className="grid gap-4 lg:grid-cols-3">
          {stats.map((s) => (
            <article key={s.domainId} className="card p-5">
              <div className="flex items-start justify-between gap-3">
                <h3 className="text-sm font-semibold leading-snug text-slate-900">
                  {s.title}
                </h3>
                <Chip>
                  {s.weight[0]}–{s.weight[1]}%
                </Chip>
              </div>
              <div className="mt-4">
                <Bar value={s.mastery} label="Readiness" />
              </div>
              <dl className="mt-4 grid grid-cols-3 gap-2 text-center">
                <div>
                  <dt className="text-[11px] text-slate-500">Cards</dt>
                  <dd className="text-sm font-medium tabular-nums text-slate-800">
                    {s.cardsMastered}/{s.cardsTotal}
                  </dd>
                </div>
                <div>
                  <dt className="text-[11px] text-slate-500">Questions</dt>
                  <dd className="text-sm font-medium tabular-nums text-slate-800">
                    {s.questionsSeen}/{s.questionsTotal}
                  </dd>
                </div>
                <div>
                  <dt className="text-[11px] text-slate-500">Accuracy</dt>
                  <dd className="text-sm font-medium tabular-nums text-slate-800">
                    {s.accuracy === null ? '—' : `${Math.round(s.accuracy * 100)}%`}
                  </dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </section>

      {weakest && answered > 0 && weakest.mastery < 70 && (
        <section className="accent-soft-bg accent-border mt-8 rounded-2xl border p-5">
          <p className="text-sm text-slate-800">
            <span className="font-semibold">Weakest domain right now:</span>{' '}
            {weakest.title} at {weakest.mastery}%.
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            <Link
              className="btn btn-primary"
              to={`/quiz?domain=${weakest.domainId}`}
            >
              Drill this domain
            </Link>
            <Link
              className="btn btn-ghost"
              to={`/flashcards?domain=${weakest.domainId}`}
            >
              Review its cards
            </Link>
          </div>
        </section>
      )}
    </>
  );
}
