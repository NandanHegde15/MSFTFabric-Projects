import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';

import type { ExamId } from '@/data/exams';
import { setAlias, useProgress } from '@/lib/progress';
import type { RunMode } from '@/lib/scoring';
import { submitResult } from '@/services/results';

type State = 'idle' | 'sending' | 'sent' | 'failed';

/**
 * Publishes a finished run to the leaderboard.
 *
 * With a player profile and automatic publishing on, this fires once on mount
 * and just reports what happened. Without a profile it stays opt-in, and the
 * only identifier sent is a nickname the learner can edit right here — so the
 * "nothing leaves the browser unless you say so" promise still holds for
 * anyone who has not created a player.
 */
export function PublishScore({
  exam,
  mode,
  domainId,
  correct,
  total,
}: {
  exam: ExamId;
  mode: RunMode;
  domainId: string;
  correct: number;
  total: number;
}) {
  const { alias, learner, settings } = useProgress();
  const [state, setState] = useState<State>('idle');
  const fired = useRef(false);

  const auto = Boolean(learner) && settings.autoPublish;

  const publish = async () => {
    setState('sending');
    const ok = await submitResult({
      exam,
      mode,
      domainId,
      correct,
      total,
      alias: learner?.handle ?? alias,
      learnerId: learner?.id,
      emblem: learner?.emblem,
    });
    setState(ok ? 'sent' : 'failed');
  };

  useEffect(() => {
    if (!auto || total === 0 || fired.current) return;
    fired.current = true;
    void publish();
    // Runs once per mounted result screen; the ref guards against a re-entry
    // from a parent re-render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auto, total]);

  if (total === 0) return null;

  if (auto) {
    return (
      <div className="card flex flex-wrap items-center gap-3 p-4">
        <span className="accent-soft-bg flex h-10 w-10 items-center justify-center rounded-xl text-xl">
          {learner!.emblem}
        </span>
        <p className="min-w-0 flex-1 text-sm text-slate-700">
          {state === 'sent' ? (
            <>
              Published to the leaderboard as{' '}
              <span className="font-medium">{learner!.handle}</span>.
            </>
          ) : state === 'failed' ? (
            'The leaderboard was unreachable, so this run was not published. Your local progress is saved.'
          ) : (
            'Publishing to the leaderboard…'
          )}
        </p>
        <Link className="btn btn-ghost" to="/leaderboard">
          View leaderboard
        </Link>
      </div>
    );
  }

  return (
    <div className="card flex flex-wrap items-center gap-3 p-4">
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-slate-800">
          {learner
            ? 'Automatic publishing is off — send this run to the leaderboard?'
            : 'Add this run to the leaderboard?'}
        </p>
        <p className="mt-1 text-xs leading-relaxed text-slate-500">
          Publishes your score, the exam and this nickname — nothing else.{' '}
          {!learner && (
            <>
              <Link className="accent-text underline" to="/leaderboard">
                Create a player
              </Link>{' '}
              to compete properly.
            </>
          )}
        </p>
      </div>

      {!learner && (
        <label className="text-xs text-slate-500">
          <span className="sr-only">Nickname</span>
          <input
            value={alias}
            onChange={(e) => setAlias(e.target.value)}
            maxLength={40}
            disabled={state === 'sent'}
            className="w-40 rounded-lg border border-slate-200 px-2.5 py-1.5 text-sm text-slate-800"
            placeholder="Nickname"
          />
        </label>
      )}

      <button
        className="btn btn-ghost"
        onClick={() => void publish()}
        disabled={state === 'sending' || state === 'sent'}
      >
        {state === 'sent'
          ? 'Published'
          : state === 'sending'
            ? 'Publishing…'
            : 'Publish score'}
      </button>

      {state === 'failed' && (
        <p className="w-full text-xs text-amber-700">
          The leaderboard is not reachable right now. Your local progress is
          saved either way.
        </p>
      )}
    </div>
  );
}
