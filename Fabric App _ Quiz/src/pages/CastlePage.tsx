import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';

import { CastleMap } from '@/components/castle/CastleMap';
import { Dragon, Hearts } from '@/components/castle/Dragon';
import { SpeakButton } from '@/components/SpeakButton';
import { Chip, PageHeader } from '@/components/ui';
import { DRAGON_HP, HEARTS, STAGES, TOTAL_QUESTIONS } from '@/data/castle';
import { domainById, domainsFor } from '@/data/exams';
import { QUESTIONS, type Question } from '@/data/questions';
import { nextStep } from '@/lib/castleRun';
import { presentAll } from '@/lib/present';
import { recordAnswer, recordRun, useExam, useProgress } from '@/lib/progress';
import { scoreRun } from '@/lib/scoring';
import { shuffle } from '@/lib/shuffle';
import { stopSpeech } from '@/lib/speech';
import { submitResult } from '@/services/results';

type Phase = 'gate' | 'brief' | 'fight' | 'cleared' | 'victory' | 'defeat';

export function CastlePage() {
  const [exam] = useExam();
  const { learner, settings } = useProgress();
  const domains = domainsFor(exam);

  const [phase, setPhase] = useState<Phase>('gate');
  const [stageIndex, setStageIndex] = useState(0);
  const [deck, setDeck] = useState<Question[][]>([]);
  const [questionIndex, setQuestionIndex] = useState(0);
  const [picked, setPicked] = useState<number[]>([]);
  const [submitted, setSubmitted] = useState(false);
  const [lastCorrect, setLastCorrect] = useState(false);
  const [hearts, setHearts] = useState(HEARTS);
  const [dragonHp, setDragonHp] = useState(DRAGON_HP);
  const [dragonState, setDragonState] = useState<'idle' | 'hit' | 'slain'>('idle');
  const [tally, setTally] = useState({ correct: 0, answered: 0 });
  const [published, setPublished] = useState<'no' | 'sending' | 'yes' | 'failed'>(
    'no'
  );

  const pool = useMemo(() => QUESTIONS.filter((q) => q.exam === exam), [exam]);
  const enoughQuestions = pool.length >= TOTAL_QUESTIONS;

  /**
   * Deal one deck per stage up front, drawing without replacement so a run
   * never asks the same question twice. Stages that ask for a difficulty or a
   * domain fall back to the general pool when that slice runs dry.
   */
  const deal = useCallback(() => {
    const remaining = shuffle(pool);
    const taken = new Set<string>();

    const take = (n: number, prefer: (q: Question) => boolean): Question[] => {
      const out: Question[] = [];
      for (const q of remaining) {
        if (out.length === n) break;
        if (taken.has(q.id) || !prefer(q)) continue;
        taken.add(q.id);
        out.push(q);
      }
      for (const q of remaining) {
        if (out.length === n) break;
        if (taken.has(q.id)) continue;
        taken.add(q.id);
        out.push(q);
      }
      return out;
    };

    return STAGES.map((stage) =>
      presentAll(
      take(stage.questions, (q) => {
        const domainOk =
          stage.domainIndex === null ||
          q.domainId === domains[stage.domainIndex]?.id;
        const difficultyOk =
          stage.difficulty === null || q.difficulty === stage.difficulty;
        return domainOk && difficultyOk;
      })
      )
    );
  }, [pool, domains]);

  const begin = () => {
    stopSpeech();
    setDeck(deal());
    setStageIndex(0);
    setQuestionIndex(0);
    setPicked([]);
    setSubmitted(false);
    setHearts(HEARTS);
    setDragonHp(DRAGON_HP);
    setDragonState('idle');
    setTally({ correct: 0, answered: 0 });
    setPublished('no');
    setPhase('brief');
  };

  // Stop any narration when the run ends or the page unmounts.
  useEffect(() => () => stopSpeech(), []);

  const stage = STAGES[stageIndex];
  const question = deck[stageIndex]?.[questionIndex];
  const multi = question ? question.answer.length > 1 : false;

  const finish = useCallback(
    async (outcome: 'victory' | 'defeat', final: { correct: number; answered: number }) => {
      const points = scoreRun({
        mode: 'castle',
        correct: final.correct,
        total: final.answered,
        dragonSlain: outcome === 'victory',
      });

      recordRun({
        at: Date.now(),
        exam,
        mode: 'castle',
        domainId: 'mixed',
        correct: final.correct,
        total: final.answered,
        points,
        outcome,
      });

      setPhase(outcome);

      if (!learner || !settings.autoPublish) return;

      // Flagged before the await so the summary never claims publishing is off
      // while the request is still in flight.
      setPublished('sending');
      const ok = await submitResult({
        exam,
        mode: 'castle',
        domainId: 'mixed',
        correct: final.correct,
        total: final.answered,
        alias: learner.handle,
        learnerId: learner.id,
        emblem: learner.emblem,
        outcome,
      });
      setPublished(ok ? 'yes' : 'failed');
    },
    [exam, learner, settings.autoPublish]
  );

  const check = () => {
    if (!question || picked.length === 0 || submitted) return;

    const correct =
      picked.length === question.answer.length &&
      picked.every((p) => question.answer.includes(p));

    setSubmitted(true);
    setLastCorrect(correct);
    setTally((t) => ({
      correct: t.correct + (correct ? 1 : 0),
      answered: t.answered + 1,
    }));

    recordAnswer({
      id: question.id,
      exam: question.exam,
      domainId: question.domainId,
      correct,
      at: Date.now(),
    });

    if (stage.boss && correct) {
      setDragonHp((hp) => Math.max(0, hp - 1));
      setDragonState('hit');
      window.setTimeout(() => setDragonState('idle'), 450);
    }
    if (!correct) setHearts((h) => h - 1);
  };

  const advance = () => {
    stopSpeech();

    const { correct, answered } = tally;
    const step = nextStep(
      { questionIndex, hearts, dragonHp },
      {
        questionCount: (deck[stageIndex] ?? []).length,
        isBoss: Boolean(stage.boss),
        isLastStage: stageIndex + 1 >= STAGES.length,
      }
    );

    switch (step.kind) {
      case 'next-question':
        setQuestionIndex((i) => i + 1);
        setPicked([]);
        setSubmitted(false);
        return;
      case 'stage-cleared':
        setPhase('cleared');
        return;
      case 'victory':
        if (stage.boss) setDragonState('slain');
        void finish('victory', { correct, answered });
        return;
      case 'defeat':
        void finish('defeat', { correct, answered });
    }
  };

  const nextStage = () => {
    setStageIndex((i) => i + 1);
    setQuestionIndex(0);
    setPicked([]);
    setSubmitted(false);
    setPhase('brief');
  };

  const header = (
    <PageHeader
      title="Castle journey"
      lead="Six stages, three hearts, one dragon. Every wrong answer costs a heart — lose all three and the run ends where you stand."
    />
  );

  // ------------------------------------------------------------------- gate
  if (phase === 'gate') {
    return (
      <>
        {header}
        <div className="card overflow-hidden">
          <div className="accent-soft-bg px-6 pt-6">
            <CastleMap stageIndex={-1} />
          </div>
          <div className="p-6">
            <h2 className="text-lg font-semibold text-slate-900">
              {exam} — the keep awaits
            </h2>
            <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-600">
              {TOTAL_QUESTIONS} questions drawn from the {exam} objective areas,
              no question repeated. The first three stages sweep the syllabus
              domain by domain; the tower and the lair pull only the hard ones.
            </p>

            <dl className="mt-5 flex flex-wrap gap-x-8 gap-y-3 text-sm">
              <div>
                <dt className="text-xs text-slate-500">Hearts</dt>
                <dd className="mt-1">
                  <Hearts hearts={HEARTS} max={HEARTS} />
                </dd>
              </div>
              <div>
                <dt className="text-xs text-slate-500">Dragon</dt>
                <dd className="mt-1 font-medium text-slate-800">
                  {DRAGON_HP} strikes to fell
                </dd>
              </div>
              <div>
                <dt className="text-xs text-slate-500">Reward</dt>
                <dd className="mt-1 font-medium text-slate-800">
                  100 bonus points for the kill
                </dd>
              </div>
            </dl>

            {!enoughQuestions && (
              <p className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                The {exam} bank has {pool.length} questions and a full run needs{' '}
                {TOTAL_QUESTIONS}. Some will repeat across stages.
              </p>
            )}

            <div className="mt-6 flex flex-wrap gap-3">
              <button className="btn btn-primary" onClick={begin}>
                Lower the drawbridge
              </button>
              {!learner && (
                <Link className="btn btn-ghost" to="/leaderboard">
                  Create a player first
                </Link>
              )}
            </div>
            {!learner && (
              <p className="mt-3 text-xs text-slate-500">
                You can play without a player — the run just will not count
                towards the leaderboard.
              </p>
            )}
          </div>
        </div>
      </>
    );
  }

  // ------------------------------------------------------- victory / defeat
  if (phase === 'victory' || phase === 'defeat') {
    const won = phase === 'victory';
    const points = scoreRun({
      mode: 'castle',
      correct: tally.correct,
      total: tally.answered,
      dragonSlain: won,
    });

    return (
      <>
        {header}
        <div className="card overflow-hidden">
          <div
            className={`px-6 py-10 text-center ${
              won ? 'bg-emerald-50' : 'bg-slate-100'
            }`}
          >
            {won ? (
              <Dragon hp={0} maxHp={DRAGON_HP} state="slain" />
            ) : (
              <p className="text-5xl" aria-hidden>
                🛡️
              </p>
            )}
            <h2 className="mt-5 text-2xl font-semibold tracking-tight text-slate-900">
              {won ? 'The dragon is slain' : `You fell at ${stage.name}`}
            </h2>
            <p className="mx-auto mt-2 max-w-md text-sm leading-relaxed text-slate-600">
              {won
                ? `You crossed all six stages of the ${exam} keep with ${hearts} heart${hearts === 1 ? '' : 's'} to spare.`
                : 'Three mistakes and the keep throws you out. The explanations you just read are the ones worth revisiting.'}
            </p>
          </div>

          <div className="grid gap-4 border-t border-slate-200 p-6 sm:grid-cols-4">
            <Metric label="Points" value={points.toLocaleString()} />
            <Metric label="Correct" value={`${tally.correct}/${tally.answered}`} />
            <Metric
              label="Accuracy"
              value={
                tally.answered
                  ? `${Math.round((tally.correct / tally.answered) * 100)}%`
                  : '—'
              }
            />
            <Metric label="Hearts left" value={hearts} />
          </div>

          <div className="flex flex-wrap gap-3 border-t border-slate-200 p-6">
            <button className="btn btn-primary" onClick={begin}>
              {won ? 'Run it again' : 'Try again'}
            </button>
            <Link className="btn btn-ghost" to="/leaderboard">
              Leaderboard
            </Link>
            <Link className="btn btn-ghost" to="/quiz">
              Practise a domain
            </Link>
          </div>

          <p className="border-t border-slate-200 px-6 py-4 text-xs text-slate-500">
            {published === 'sending'
              ? 'Publishing to the leaderboard…'
              : published === 'yes'
                ? `Published to the leaderboard as ${learner?.handle}.`
                : published === 'failed'
                  ? 'The leaderboard was unreachable, so this run was not published. Your local progress is saved.'
                  : !learner
                    ? 'No player profile, so this run stayed in this browser.'
                    : 'Automatic publishing is off — turn it on from the Leaderboard tab.'}
          </p>
        </div>
      </>
    );
  }

  // ------------------------------------------------------------ stage brief
  if (phase === 'brief' || phase === 'cleared') {
    const cleared = phase === 'cleared';
    const shown = cleared ? STAGES[stageIndex] : stage;

    return (
      <>
        {header}
        <div className="card overflow-hidden">
          <div className="accent-soft-bg px-6 pt-6">
            <CastleMap stageIndex={cleared ? stageIndex + 1 : stageIndex} />
          </div>
          <div className="p-6">
            <div className="flex flex-wrap items-center gap-3">
              <Chip tone="accent">
                Stage {stageIndex + 1} of {STAGES.length}
              </Chip>
              <Hearts hearts={hearts} max={HEARTS} />
            </div>

            <h2 className="mt-4 text-lg font-semibold text-slate-900">
              {cleared ? `${shown.name} — cleared` : shown.name}
            </h2>
            <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-600">
              {cleared
                ? `${tally.correct} of ${tally.answered} right so far. ${STAGES[stageIndex + 1]?.scene ?? ''}`
                : shown.scene}
            </p>

            <button
              className="btn btn-primary mt-6"
              onClick={cleared ? nextStage : () => setPhase('fight')}
            >
              {cleared
                ? `On to ${STAGES[stageIndex + 1]?.name ?? 'the end'}`
                : shown.boss
                  ? 'Draw your sword'
                  : 'Step forward'}
            </button>
          </div>
        </div>
      </>
    );
  }

  // ----------------------------------------------------------------- fight
  if (!question) {
    return (
      <>
        {header}
        <div className="card p-6">
          <p className="text-sm text-slate-600">
            This stage could not be dealt a question. Start a fresh run.
          </p>
          <button className="btn btn-primary mt-4" onClick={begin}>
            Restart
          </button>
        </div>
      </>
    );
  }

  const stageDeck = deck[stageIndex] ?? [];
  const explanation = `${lastCorrect ? 'Correct.' : 'Not quite.'} ${question.explanation}`;

  return (
    <>
      {header}

      <div className="card mb-4 flex flex-wrap items-center gap-x-6 gap-y-3 p-4">
        <Chip tone="accent">{stage.name}</Chip>
        <span className="text-xs text-slate-500">
          Question {questionIndex + 1} of {stageDeck.length}
        </span>
        <span className="text-xs text-slate-500">
          {tally.correct}/{tally.answered} correct
        </span>
        <span className="ml-auto">
          <Hearts hearts={hearts} max={HEARTS} />
        </span>
      </div>

      {stage.boss && (
        <div className="card mb-4 flex justify-center p-6">
          <Dragon hp={dragonHp} maxHp={DRAGON_HP} state={dragonState} />
        </div>
      )}

      <article className="card p-6">
        <div className="mb-3 flex flex-wrap gap-2">
          <Chip>{domainById(question.domainId)?.short}</Chip>
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
                    name="castle-choice"
                    checked={isPicked}
                    disabled={submitted}
                    onChange={() =>
                      setPicked((p) =>
                        multi
                          ? p.includes(i)
                            ? p.filter((x) => x !== i)
                            : [...p, i]
                          : [i]
                      )
                    }
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
              lastCorrect
                ? 'border-emerald-200 bg-emerald-50'
                : 'border-rose-200 bg-rose-50'
            }`}
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="text-sm font-semibold text-slate-900">
                {lastCorrect
                  ? stage.boss
                    ? 'A clean strike!'
                    : 'Correct'
                  : `Not quite — one heart lost (${Math.max(0, hearts)} left)`}
              </p>
              <SpeakButton
                id={`castle-${question.id}`}
                text={explanation}
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
            <button className="btn btn-primary" onClick={advance}>
              {hearts <= 0 ? 'See how it ended' : 'Press on'}
            </button>
          ) : (
            <button
              className="btn btn-primary"
              onClick={check}
              disabled={picked.length === 0}
            >
              Answer
            </button>
          )}
          <button
            className="btn btn-ghost"
            onClick={() => {
              stopSpeech();
              setPhase('gate');
            }}
          >
            Abandon run
          </button>
        </div>
      </article>
    </>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div>
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
        {label}
      </p>
      <p className="mt-1 text-2xl font-semibold tabular-nums text-slate-900">
        {value}
      </p>
    </div>
  );
}
