import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { PublishScore } from '@/components/PublishScore';
import { SpeakButton } from '@/components/SpeakButton';
import { Bar, Chip, EmptyState, PageHeader } from '@/components/ui';
import { domainById, domainsFor } from '@/data/exams';
import { SCENARIOS } from '@/data/spotTheError';
import { recordRun, recordSpot, useExam } from '@/lib/progress';
import { shuffle } from '@/lib/shuffle';

export function SpotErrorPage() {
  const [exam] = useExam();
  const [params, setParams] = useSearchParams();
  const domainFilter = params.get('domain') ?? '';
  const domains = domainsFor(exam);

  const [index, setIndex] = useState(0);
  const [picked, setPicked] = useState<number | null>(null);
  const [score, setScore] = useState({ correct: 0, total: 0 });
  const [finished, setFinished] = useState(false);

  const deck = useMemo(
    () =>
      shuffle(
        SCENARIOS.filter(
          (s) => s.exam === exam && (!domainFilter || s.domainId === domainFilter)
        )
      ),
    [exam, domainFilter]
  );

  useEffect(() => {
    setIndex(0);
    setPicked(null);
    setScore({ correct: 0, total: 0 });
    setFinished(false);
  }, [exam, domainFilter]);

  const scenario = deck[index];

  const choose = (line: number) => {
    if (picked !== null || !scenario) return;
    setPicked(line);
    const correct = line === scenario.faultyLine;
    setScore((s) => ({
      correct: s.correct + (correct ? 1 : 0),
      total: s.total + 1,
    }));
    recordSpot({
      id: scenario.id,
      exam: scenario.exam,
      domainId: scenario.domainId,
      correct,
      at: Date.now(),
    });
  };

  const next = () => {
    if (index + 1 >= deck.length) {
      recordRun({
        at: Date.now(),
        exam,
        mode: 'spot-the-error',
        domainId: domainFilter || 'mixed',
        correct: score.correct,
        total: score.total,
      });
      setFinished(true);
      return;
    }
    setIndex((i) => i + 1);
    setPicked(null);
  };

  const restart = () => {
    setIndex(0);
    setPicked(null);
    setScore({ correct: 0, total: 0 });
    setFinished(false);
  };

  const setDomain = (value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set('domain', value);
    else next.delete('domain');
    setParams(next, { replace: true });
  };

  const header = (
    <>
      <PageHeader
        title="Spot the error"
        lead="Each of these is one line away from being right. Pick the line that breaks it — the explanation tells you why the tempting lines are actually fine."
      />
      <div className="card mb-6 flex flex-wrap items-center gap-3 p-4">
        <label className="text-sm text-slate-600">
          Domain
          <select
            value={domainFilter}
            onChange={(e) => setDomain(e.target.value)}
            className="ml-2 rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-sm text-slate-800"
          >
            <option value="">All domains</option>
            {domains.map((d) => (
              <option key={d.id} value={d.id}>
                {d.short}
              </option>
            ))}
          </select>
        </label>
        <span className="ml-auto text-xs text-slate-500">
          {score.correct}/{score.total} correct this session
        </span>
      </div>
    </>
  );

  if (deck.length === 0) {
    return (
      <>
        {header}
        <EmptyState
          title="No scenarios for this filter"
          body="Pick another domain, or switch the exam in the header."
        />
      </>
    );
  }

  if (finished) {
    const pct = score.total ? Math.round((score.correct / score.total) * 100) : 0;
    return (
      <>
        {header}
        <div className="card p-6">
          <Bar value={pct} label="Session score" />
          <p className="mt-4 text-sm text-slate-600">
            {score.correct} of {score.total} scenarios spotted.
          </p>
          <button className="btn btn-primary mt-4" onClick={restart}>
            Go again
          </button>
        </div>
        <div className="mt-6">
          <PublishScore
            exam={exam}
            mode="spot-the-error"
            domainId={domainFilter || 'mixed'}
            correct={score.correct}
            total={score.total}
          />
        </div>
      </>
    );
  }

  if (!scenario) return <>{header}</>;

  const revealed = picked !== null;
  const gotIt = picked === scenario.faultyLine;

  return (
    <>
      {header}

      <div className="mb-4">
        <Bar
          value={Math.round((index / deck.length) * 100)}
          label={`Scenario ${index + 1} of ${deck.length}`}
        />
      </div>

      <article className="card overflow-hidden">
        <header className="border-b border-slate-200 p-5">
          <div className="mb-2 flex flex-wrap gap-2">
            <Chip tone="accent">{domainById(scenario.domainId)?.short}</Chip>
            <Chip>{scenario.language}</Chip>
          </div>
          <h2 className="text-base font-semibold text-slate-900">
            {scenario.title}
          </h2>
          <p className="mt-1.5 text-sm leading-relaxed text-slate-600">
            {scenario.brief}
          </p>
          <p className="mt-3 text-xs font-medium text-slate-500">
            {scenario.kind === 'claims'
              ? 'Click the statement that is wrong.'
              : 'Click the line that is wrong.'}
          </p>
        </header>

        <ol className="divide-y divide-slate-100 bg-slate-50/60">
          {scenario.lines.map((line, i) => {
            const isFault = i === scenario.faultyLine;
            const isPicked = i === picked;

            let tone = 'hover:bg-white';
            if (revealed && isFault) tone = 'bg-emerald-50';
            else if (revealed && isPicked) tone = 'bg-rose-50';
            else if (revealed) tone = '';

            return (
              <li key={i}>
                <button
                  type="button"
                  onClick={() => choose(i)}
                  disabled={revealed}
                  className={`flex w-full items-start gap-3 px-4 py-1.5 text-left transition-colors ${tone} ${
                    revealed ? 'cursor-default' : 'cursor-pointer'
                  }`}
                >
                  <span className="w-6 shrink-0 select-none pt-0.5 text-right text-xs tabular-nums text-slate-400">
                    {i + 1}
                  </span>
                  <span
                    className={
                      scenario.kind === 'claims'
                        ? 'text-sm leading-relaxed text-slate-800'
                        : 'code-line overflow-x-auto text-slate-800'
                    }
                  >
                    {line || ' '}
                  </span>
                </button>
              </li>
            );
          })}
        </ol>

        {revealed && (
          <div className="border-t border-slate-200 p-5">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <p
                className={`text-sm font-semibold ${
                  gotIt ? 'text-emerald-700' : 'text-rose-700'
                }`}
              >
                {gotIt
                  ? `Spotted it — line ${scenario.faultyLine + 1}. ${scenario.fault}.`
                  : `Not this one. The problem is on line ${scenario.faultyLine + 1}: ${scenario.fault}.`}
              </p>
              <SpeakButton
                id={`spot-${scenario.id}`}
                text={`${gotIt ? 'Spotted it.' : 'Not this one.'} The problem is on line ${scenario.faultyLine + 1}. ${scenario.fault}. ${scenario.explanation}`}
                autoPlayKey={scenario.id}
              />
            </div>

            {!gotIt && picked !== null && scenario.decoys?.[picked] && (
              <p className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm leading-relaxed text-slate-600">
                <span className="font-medium text-slate-800">
                  Why line {picked + 1} is fine:{' '}
                </span>
                {scenario.decoys[picked]}
              </p>
            )}

            <p className="mt-3 text-sm leading-relaxed text-slate-700">
              {scenario.explanation}
            </p>

            <div className="mt-4 rounded-xl border border-emerald-200 bg-emerald-50 p-4">
              <p className="text-xs font-semibold uppercase tracking-wide text-emerald-800">
                {scenario.kind === 'claims' ? 'What it should say' : 'The fix'}
              </p>
              <pre className="mt-2 overflow-x-auto whitespace-pre-wrap font-[family-name:var(--font-mono)] text-[13px] leading-relaxed text-emerald-900">
                {scenario.fix}
              </pre>
            </div>

            <button className="btn btn-primary mt-5" onClick={next}>
              {index + 1 >= deck.length ? 'Finish session' : 'Next scenario'}
            </button>
          </div>
        )}
      </article>
    </>
  );
}
