import { useState } from 'react';

import { EXAM_IDS, type ExamId } from '@/data/exams';
import { setLearner, useProgress } from '@/lib/progress';
import { createLearner } from '@/services/results';

const EMBLEMS = ['🐉', '🏰', '⚔️', '🛡️', '🔥', '🦉', '🦅', '🐺', '🦊', '🐋', '⚡', '🌊'];

export function ProfileForm({ onDone }: { onDone?: () => void }) {
  const { exam } = useProgress();
  const [handle, setHandle] = useState('');
  const [emblem, setEmblem] = useState(EMBLEMS[0]);
  const [target, setTarget] = useState<ExamId>(exam);
  const [state, setState] = useState<'idle' | 'saving' | 'offline'>('idle');

  const trimmed = handle.trim();
  const valid = trimmed.length >= 2 && trimmed.length <= 32;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!valid || state === 'saving') return;
    setState('saving');

    const created = await createLearner({
      handle: trimmed,
      emblem,
      exam: target,
    });

    // If the backend is unreachable we still keep a local profile so the game
    // and the run history work; it simply will not appear on the board until a
    // later run publishes successfully.
    setLearner({
      id: created?.id ?? crypto.randomUUID(),
      handle: trimmed,
      emblem,
      exam: target,
      createdAt: Date.now(),
    });

    if (!created) setState('offline');
    onDone?.();
  };

  return (
    <form onSubmit={(e) => void submit(e)} className="card p-6">
      <h2 className="text-base font-semibold text-slate-900">
        Create your player
      </h2>
      <p className="mt-1.5 max-w-2xl text-sm leading-relaxed text-slate-600">
        Pick a name and an emblem to appear on the leaderboard. No email, no
        password, nothing personal.
      </p>

      <label className="mt-5 block text-sm font-medium text-slate-800">
        Player name
        <input
          value={handle}
          onChange={(e) => setHandle(e.target.value)}
          maxLength={32}
          placeholder="Direct Lake Dan"
          autoFocus
          className="mt-1.5 block w-full max-w-sm rounded-xl border border-slate-200 px-3 py-2.5 text-sm text-slate-900"
        />
      </label>

      <fieldset className="mt-5">
        <legend className="text-sm font-medium text-slate-800">Emblem</legend>
        <div className="mt-2 flex flex-wrap gap-2">
          {EMBLEMS.map((e) => (
            <button
              key={e}
              type="button"
              onClick={() => setEmblem(e)}
              aria-pressed={emblem === e}
              aria-label={`Emblem ${e}`}
              className={`flex h-11 w-11 items-center justify-center rounded-xl border text-xl transition-colors ${
                emblem === e
                  ? 'accent-border accent-soft-bg'
                  : 'border-slate-200 bg-white hover:bg-slate-50'
              }`}
            >
              {e}
            </button>
          ))}
        </div>
      </fieldset>

      <fieldset className="mt-5">
        <legend className="text-sm font-medium text-slate-800">
          Competing on
        </legend>
        <div className="mt-2 flex gap-2">
          {EXAM_IDS.map((id) => (
            <button
              key={id}
              type="button"
              onClick={() => setTarget(id)}
              aria-pressed={target === id}
              className={`rounded-xl border px-4 py-2 text-sm font-medium transition-colors ${
                target === id
                  ? 'accent-border accent-soft-bg accent-text'
                  : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
              }`}
            >
              {id}
            </button>
          ))}
        </div>
      </fieldset>

      <div className="mt-6 flex flex-wrap items-center gap-3">
        <button
          type="submit"
          className="btn btn-primary"
          disabled={!valid || state === 'saving'}
        >
          {state === 'saving' ? 'Creating…' : 'Create player'}
        </button>
        {handle.length > 0 && !valid && (
          <span className="text-xs text-amber-700">
            Names are between 2 and 32 characters.
          </span>
        )}
      </div>

      <p className="mt-4 max-w-2xl text-xs leading-relaxed text-slate-500">
        This is a profile, not an account. There is no password and handles are
        not reserved, so treat the leaderboard as a friendly scoreboard rather
        than a verified ranking. Your player id is kept in this browser — export
        your progress from the Progress tab to carry it to another machine.
      </p>

      {state === 'offline' && (
        <p className="mt-3 text-xs text-amber-700">
          Saved locally, but the leaderboard was unreachable. Your scores will
          publish once it is back.
        </p>
      )}
    </form>
  );
}
