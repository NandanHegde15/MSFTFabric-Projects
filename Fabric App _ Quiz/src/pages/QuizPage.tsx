import { useMemo, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';

import { PublishScore } from '@/components/PublishScore';
import { SpeakButton } from '@/components/SpeakButton';
import { Bar, Chip, EmptyState, PageHeader } from '@/components/ui';
import { domainById, domainsFor } from '@/data/exams';
import { QUESTIONS, type Question } from '@/data/questions';
import { presentAll } from '@/lib/present';
import { recordAnswer, recordRun, useExam } from '@/lib/progress';
import { shuffle } from '@/lib/shuffle';

type Phase = 'setup' | 'running' | 'done';

interface Graded {
  question: Question;
  picked: number[];
  correct: boolean;
}

const LENGTHS = [10, 20, 0];

export function QuizPage() {
  const [exam] = useExam();
  const [params] = useSearchParams();
  const domains = domainsFor(exam);

  const [selected, setSelected] = useState<string[]>(() => {
    const preset = params.get('domain');
    return preset ? [preset] : [];
  });
  const [length, setLength] = useState(10);
  const [phase, setPhase] = useState<Phase>('setup');
  const [deck, setDeck] = useState<Question[]>([]);
  const [index, setIndex] = useState(0);
  const [picked, setPicked] = useState<number[]>([]);
  const [submitted, setSubmitted] = useState(false);
  const [graded, setGraded] = useState<Graded[]>([]);

  const pool = useMemo(
    () =>
      QUESTIONS.filter(
        (q) =>
          q.exam === exam &&
          (selected.length === 0 || selected.includes(q.domainId))
      ),
    [exam, selected]
  );

  const start = () => {
    const shuffled = shuffle(pool);
    setDeck(presentAll(length === 0 ? shuffled : shuffled.slice(0, length)));
    setIndex(0);
    setPicked([]);
    setSubmitted(false);
    setGraded([]);
    setPhase('running');
  };

  const question = deck[index];
  const multi = question ? question.answer.length > 1 : false;

  const toggle = (i: number) => {
    if (submitted) return;
    setPicked((p) =>
      multi
        ? p.includes(i)
          ? p.filter((x) => x !== i)
          : [...p, i]
        : [i]
    );
  };

  const submit = () => {
    if (!question || picked.length === 0) return;
    const correct =
      picked.length === question.answer.length &&
      picked.every((p) => question.answer.includes(p));
    setSubmitted(true);
    setGraded((g) => [...g, { question, picked, correct }]);
    recordAnswer({
      id: question.id,
      exam: question.exam,
      domainId: question.domainId,
      correct,
      at: Date.now(),
    });
  };

  const next = () => {
    if (index + 1 >= deck.length) {
      const correct = graded.filter((g) => g.correct).length;
      recordRun({
        at: Date.now(),
        exam,
        mode: 'quiz',
        domainId: selected.length === 1 ? selected[0] : 'mixed',
        correct,
        total: deck.length,
      });
      setPhase('done');
      return;
    }
    setIndex((i) => i + 1);
    setPicked([]);
    setSubmitted(false);
  };

  // ------------------------------------------------------------------ setup
  if (phase === 'setup') {
    return (
      <>
        <PageHeader
          title="Quiz"
          lead="Scenario questions across the exam objective areas. Every answer comes with an explanation of why the distractors are wrong."
        />

        <div className="card p-6">
          <h2 className="text-sm font-semibold text-slate-900">Domains</h2>
          <p className="mt-1 text-sm text-slate-500">
            Leave everything unticked to draw from the whole {exam} bank.
          </p>
          <div className="mt-4 grid gap-2 sm:grid-cols-3">
            {domains.map((d) => {
              const on = selected.includes(d.id);
              const count = QUESTIONS.filter((q) => q.domainId === d.id).length;
              return (
                <label
                  key={d.id}
                  className={`flex cursor-pointer items-start gap-3 rounded-xl border p-3 text-sm transition-colors ${
                    on
                      ? 'accent-border accent-soft-bg'
                      : 'border-slate-200 bg-white hover:bg-slate-50'
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={on}
                    onChange={() =>
                      setSelected((s) =>
                        on ? s.filter((x) => x !== d.id) : [...s, d.id]
                      )
                    }
                    className="mt-0.5"
                  />
                  <span>
                    <span className="block font-medium text-slate-800">
                      {d.short}
                    </span>
                    <span className="block text-xs text-slate-500">
                      {count} question{count === 1 ? '' : 's'} · {d.weight[0]}–
                      {d.weight[1]}% of the exam
                    </span>
                  </span>
                </label>
              );
            })}
          </div>

          <h2 className="mt-6 text-sm font-semibold text-slate-900">Length</h2>
          <div className="mt-3 flex flex-wrap gap-2">
            {LENGTHS.map((n) => (
              <button
                key={n}
                onClick={() => setLength(n)}
                className={`rounded-xl border px-4 py-2 text-sm font-medium transition-colors ${
                  length === n
                    ? 'accent-border accent-soft-bg accent-text'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                {n === 0 ? `Everything (${pool.length})` : `${n} questions`}
              </button>
            ))}
          </div>

          <div className="mt-6 flex items-center gap-3">
            <button
              className="btn btn-primary"
              onClick={start}
              disabled={pool.length === 0}
            >
              Start quiz
            </button>
            <span className="text-xs text-slate-500">
              {pool.length} question{pool.length === 1 ? '' : 's'} available
            </span>
          </div>
        </div>
      </>
    );
  }

  // ------------------------------------------------------------------- done
  if (phase === 'done') {
    const correct = graded.filter((g) => g.correct).length;
    const pct = deck.length ? Math.round((correct / deck.length) * 100) : 0;
    const missed = graded.filter((g) => !g.correct);

    return (
      <>
        <PageHeader
          title="Quiz complete"
          lead={`${correct} of ${deck.length} correct.`}
        />

        <div className="card p-6">
          <Bar value={pct} label="Score" />
          <p className="mt-4 text-sm leading-relaxed text-slate-600">
            {pct >= 80
              ? 'Comfortably above a typical pass mark on this sample. Keep the weaker domains ticking over.'
              : pct >= 60
                ? 'Close. Work the explanations on the ones you missed, then re-run the same domains.'
                : 'Plenty of room here. Try the flashcards for these domains before the next run.'}
          </p>
          <div className="mt-5 flex flex-wrap gap-2">
            <button className="btn btn-primary" onClick={start}>
              Run it again
            </button>
            <button className="btn btn-ghost" onClick={() => setPhase('setup')}>
              Change domains
            </button>
            <Link className="btn btn-ghost" to="/progress">
              See progress
            </Link>
          </div>
        </div>

        <div className="mt-6">
          <PublishScore
            exam={exam}
            mode="quiz"
            domainId={selected.length === 1 ? selected[0] : 'mixed'}
            correct={correct}
            total={deck.length}
          />
        </div>

        {missed.length > 0 && (
          <section className="mt-8">
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
              Review the {missed.length} you missed
            </h2>
            <div className="space-y-4">
              {missed.map((g) => (
                <article key={g.question.id} className="card p-5">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <Chip tone="accent">
                      {domainById(g.question.domainId)?.short}
                    </Chip>
                    <Chip>{g.question.difficulty}</Chip>
                    <span className="ml-auto">
                      <SpeakButton
                        id={`review-${g.question.id}`}
                        text={`${g.question.stem} The correct answer is: ${g.question.answer.map((a) => g.question.choices[a]).join('. And: ')}. ${g.question.explanation}`}
                      />
                    </span>
                  </div>
                  <p className="text-sm leading-relaxed text-slate-800">
                    {g.question.stem}
                  </p>
                  <p className="mt-3 text-sm text-emerald-700">
                    <span className="font-semibold">Correct: </span>
                    {g.question.answer.map((a) => g.question.choices[a]).join(' + ')}
                  </p>
                  <p className="mt-2 text-sm leading-relaxed text-slate-600">
                    {g.question.explanation}
                  </p>
                </article>
              ))}
            </div>
          </section>
        )}
      </>
    );
  }

  // ---------------------------------------------------------------- running
  if (!question) {
    return (
      <EmptyState
        title="No questions matched"
        body="Widen the domain filter and try again."
        action={
          <button className="btn btn-primary" onClick={() => setPhase('setup')}>
            Back to setup
          </button>
        }
      />
    );
  }

  return (
    <>
      <div className="mb-5">
        <Bar
          value={Math.round((index / deck.length) * 100)}
          label={`Question ${index + 1} of ${deck.length}`}
        />
      </div>

      <article className="card p-6">
        <div className="mb-3 flex flex-wrap gap-2">
          <Chip tone="accent">{domainById(question.domainId)?.short}</Chip>
          <Chip>{question.difficulty === 'tricky' ? 'Tricky' : 'Core'}</Chip>
          {multi && <Chip tone="amber">Choose {question.answer.length}</Chip>}
        </div>

        <h2 className="text-[15px] font-medium leading-relaxed text-slate-900">
          {question.stem}
        </h2>

        <ul className="mt-5 space-y-2">
          {question.choices.map((choice, i) => {
            const isPicked = picked.includes(i);
            const isRight = question.answer.includes(i);
            let tone = 'border-slate-200 bg-white hover:bg-slate-50';
            if (submitted && isRight) tone = 'border-emerald-300 bg-emerald-50';
            else if (submitted && isPicked) tone = 'border-rose-300 bg-rose-50';
            else if (isPicked) tone = 'accent-border accent-soft-bg';

            return (
              <li key={i}>
                <label
                  className={`flex cursor-pointer items-start gap-3 rounded-xl border p-3.5 text-sm leading-relaxed transition-colors ${tone}`}
                >
                  <input
                    type={multi ? 'checkbox' : 'radio'}
                    name="choice"
                    checked={isPicked}
                    onChange={() => toggle(i)}
                    disabled={submitted}
                    className="mt-1"
                  />
                  <span className="text-slate-800">{choice}</span>
                </label>
              </li>
            );
          })}
        </ul>

        {submitted && (
          <div
            className={`mt-5 rounded-xl border p-4 ${
              graded[graded.length - 1]?.correct
                ? 'border-emerald-200 bg-emerald-50'
                : 'border-rose-200 bg-rose-50'
            }`}
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="text-sm font-semibold text-slate-900">
                {graded[graded.length - 1]?.correct ? 'Correct' : 'Not quite'}
              </p>
              <SpeakButton
                id={`quiz-${question.id}`}
                text={`${graded[graded.length - 1]?.correct ? 'Correct.' : 'Not quite.'} ${question.explanation}`}
                autoPlayKey={question.id}
              />
            </div>
            <p className="mt-2 text-sm leading-relaxed text-slate-700">
              {question.explanation}
            </p>
          </div>
        )}

        <div className="mt-5 flex gap-3">
          {submitted ? (
            <button className="btn btn-primary" onClick={next}>
              {index + 1 >= deck.length ? 'Finish' : 'Next question'}
            </button>
          ) : (
            <button
              className="btn btn-primary"
              onClick={submit}
              disabled={picked.length === 0}
            >
              Check answer
            </button>
          )}
          <button className="btn btn-ghost" onClick={() => setPhase('setup')}>
            End quiz
          </button>
        </div>
      </article>
    </>
  );
}
