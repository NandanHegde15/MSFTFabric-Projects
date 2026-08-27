import { useState } from 'react';
import { Link } from 'react-router-dom';

import { Bar, Chip, PageHeader, Stat } from '@/components/ui';
import { domainsFor, examById } from '@/data/exams';
import {
  domainStats,
  exportProgress,
  importProgress,
  overallMastery,
  resetProgress,
  streak,
  updateSettings,
  useExam,
  useProgress,
} from '@/lib/progress';
import { speechSupported, toggleSpeak } from '@/lib/speech';

export function ProgressPage() {
  const [exam] = useExam();
  const progress = useProgress();
  const stats = domainStats(progress, exam);
  const mastery = overallMastery(stats);
  const domains = domainsFor(exam);

  const runs = [...progress.runs].filter((r) => r.exam === exam).reverse();
  const [confirmReset, setConfirmReset] = useState(false);
  const { settings } = progress;

  const download = () => {
    const blob = new Blob([exportProgress()], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'fabricquiz-progress.json';
    a.click();
    URL.revokeObjectURL(url);
  };

  const upload = (file: File) => {
    void file.text().then((text) => {
      if (!importProgress(text)) {
        window.alert('That file could not be read as FabricQuiz progress.');
      }
    });
  };

  return (
    <>
      <PageHeader
        title="Progress"
        lead={`Readiness across the ${exam} domains, weighted the way the exam weights them. Everything here lives in this browser.`}
      />

      <section className="grid gap-4 sm:grid-cols-3">
        <Stat label="Overall readiness" value={`${mastery}%`} />
        <Stat
          label="Study streak"
          value={`${streak(progress.days)} day${streak(progress.days) === 1 ? '' : 's'}`}
        />
        <Stat label="Sessions completed" value={runs.length} />
      </section>

      <section className="mt-8 space-y-4">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-500">
          By domain
        </h2>
        {stats.map((s) => {
          const domain = domains.find((d) => d.id === s.domainId);
          return (
            <article key={s.domainId} className="card p-5">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <h3 className="text-sm font-semibold text-slate-900">
                    {s.title}
                  </h3>
                  <p className="mt-1 text-xs text-slate-500">
                    {s.weight[0]}–{s.weight[1]}% of {exam}
                  </p>
                </div>
                <div className="flex gap-2">
                  <Link
                    className="btn btn-ghost"
                    to={`/flashcards?domain=${s.domainId}`}
                  >
                    Cards
                  </Link>
                  <Link
                    className="btn btn-ghost"
                    to={`/quiz?domain=${s.domainId}`}
                  >
                    Quiz
                  </Link>
                </div>
              </div>

              <div className="mt-4 grid gap-4 sm:grid-cols-2">
                <Bar value={s.mastery} label="Readiness" />
                <Bar
                  value={
                    s.cardsTotal ? Math.round((s.cardsMastered / s.cardsTotal) * 100) : 0
                  }
                  label={`Cards known (${s.cardsMastered}/${s.cardsTotal})`}
                  tone="slate"
                />
              </div>

              <dl className="mt-4 flex flex-wrap gap-x-8 gap-y-2 text-sm">
                <div className="flex gap-2">
                  <dt className="text-slate-500">Questions seen</dt>
                  <dd className="font-medium tabular-nums text-slate-800">
                    {s.questionsSeen}/{s.questionsTotal}
                  </dd>
                </div>
                <div className="flex gap-2">
                  <dt className="text-slate-500">Scenarios seen</dt>
                  <dd className="font-medium tabular-nums text-slate-800">
                    {s.scenariosSeen}/{s.scenariosTotal}
                  </dd>
                </div>
                <div className="flex gap-2">
                  <dt className="text-slate-500">Accuracy</dt>
                  <dd className="font-medium tabular-nums text-slate-800">
                    {s.accuracy === null
                      ? 'not attempted'
                      : `${Math.round(s.accuracy * 100)}%`}
                  </dd>
                </div>
              </dl>

              {domain && (
                <details className="mt-4">
                  <summary className="cursor-pointer text-xs font-medium text-slate-500 hover:text-slate-700">
                    Objectives in this domain
                  </summary>
                  <div className="mt-3 space-y-3">
                    {domain.objectives.map((o) => (
                      <div key={o.id}>
                        <p className="text-sm font-medium text-slate-800">
                          {o.title}
                        </p>
                        <ul className="mt-1 list-inside list-disc space-y-0.5 text-xs leading-relaxed text-slate-500">
                          {o.points.map((p) => (
                            <li key={p}>{p}</li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </article>
          );
        })}
      </section>

      <section className="mt-8 grid gap-4 lg:grid-cols-2">
        <div className="card p-5">
          <h2 className="text-sm font-semibold text-slate-900">Your sessions</h2>
          {runs.length === 0 ? (
            <p className="mt-3 text-sm text-slate-500">
              No completed sessions yet for {exam}.
            </p>
          ) : (
            <ul className="mt-3 divide-y divide-slate-100 text-sm">
              {runs.slice(0, 12).map((r, i) => (
                <li key={i} className="flex items-center gap-3 py-2">
                  <Chip
                    tone={
                      r.mode === 'castle'
                        ? r.outcome === 'victory'
                          ? 'green'
                          : 'red'
                        : r.mode === 'quiz'
                          ? 'accent'
                          : 'slate'
                    }
                  >
                    {r.mode === 'castle'
                      ? r.outcome === 'victory'
                        ? '🐉 Slain'
                        : 'Castle'
                      : r.mode === 'quiz'
                        ? 'Quiz'
                        : 'Spot'}
                  </Chip>
                  <span className="tabular-nums text-slate-800">
                    {r.correct}/{r.total}
                  </span>
                  {r.points != null && (
                    <span className="text-xs tabular-nums text-slate-500">
                      {r.points} pts
                    </span>
                  )}
                  <span className="ml-auto text-xs text-slate-400">
                    {new Date(r.at).toLocaleString()}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="card p-5">
          <h2 className="text-sm font-semibold text-slate-900">Read aloud</h2>
          {speechSupported() ? (
            <>
              <p className="mt-1 text-xs leading-relaxed text-slate-500">
                Explanations and flashcards can be spoken using your browser's
                built-in voice. Nothing is sent anywhere — synthesis happens on
                your device.
              </p>

              <label className="mt-4 flex cursor-pointer items-center gap-2.5 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={settings.autoSpeak}
                  onChange={(e) => updateSettings({ autoSpeak: e.target.checked })}
                />
                Start reading as soon as an answer is revealed
              </label>

              <label className="mt-4 block text-sm text-slate-700">
                Speed:{' '}
                <span className="font-medium tabular-nums">
                  {settings.speechRate.toFixed(2)}x
                </span>
                <input
                  type="range"
                  min={0.5}
                  max={2}
                  step={0.05}
                  value={settings.speechRate}
                  onChange={(e) =>
                    updateSettings({ speechRate: Number(e.target.value) })
                  }
                  className="mt-2 w-full"
                />
              </label>

              <button
                className="btn btn-ghost mt-3"
                onClick={() =>
                  toggleSpeak(
                    'settings-preview',
                    'Direct Lake loads Delta Parquet columns from OneLake straight into the VertiPaq engine on demand, with no refresh and no second copy of the data.',
                    settings.speechRate
                  )
                }
              >
                Test the voice
              </button>
            </>
          ) : (
            <p className="mt-3 text-sm leading-relaxed text-slate-500">
              This browser has no speech synthesis, so the Listen buttons are
              hidden. Chrome, Edge and Safari all support it.
            </p>
          )}
        </div>
      </section>

      <section className="card mt-8 p-5">
        <h2 className="text-sm font-semibold text-slate-900">Your data</h2>
        <p className="mt-1.5 max-w-2xl text-sm leading-relaxed text-slate-600">
          Progress is kept in this browser under a single localStorage key.
          Clearing site data wipes it, so export before you switch machines.
        </p>
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <button className="btn btn-ghost" onClick={download}>
            Export progress
          </button>
          <label className="btn btn-ghost">
            Import progress
            <input
              type="file"
              accept="application/json"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) upload(file);
                e.target.value = '';
              }}
            />
          </label>
          {confirmReset ? (
            <>
              <button
                className="btn btn-ghost text-rose-600"
                onClick={() => {
                  resetProgress();
                  setConfirmReset(false);
                }}
              >
                Yes, erase everything
              </button>
              <button
                className="btn btn-ghost"
                onClick={() => setConfirmReset(false)}
              >
                Cancel
              </button>
            </>
          ) : (
            <button
              className="btn btn-ghost"
              onClick={() => setConfirmReset(true)}
            >
              Reset progress
            </button>
          )}
        </div>
      </section>

      <p className="mt-6 text-xs text-slate-500">
        Domain weightings shown here follow the{' '}
        <a
          className="accent-text underline"
          href={examById(exam).skillsOutlineUrl}
          target="_blank"
          rel="noreferrer noopener"
        >
          official {exam} skills outline
        </a>
        . Microsoft updates these periodically — check the current version before
        you book.
      </p>
    </>
  );
}
