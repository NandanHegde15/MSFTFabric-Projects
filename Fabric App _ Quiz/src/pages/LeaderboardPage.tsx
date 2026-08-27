import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';

import { ProfileForm } from '@/components/ProfileForm';
import { Chip, PageHeader, Stat } from '@/components/ui';
import { EXAM_IDS, type ExamId } from '@/data/exams';
import { setLearner, useExam, useProgress, updateSettings } from '@/lib/progress';
import type { LeaderboardRow } from '@/lib/scoring';
import { getLeaderboard, getLearners } from '@/services/results';

type Scope = ExamId | 'all';

export function LeaderboardPage() {
  const [exam] = useExam();
  const { learner, settings } = useProgress();
  const [scope, setScope] = useState<Scope>(exam);
  const [rows, setRows] = useState<LeaderboardRow[] | null>(null);
  const [registered, setRegistered] = useState<number | null>(null);
  const [editing, setEditing] = useState(false);

  const refresh = useCallback(async (s: Scope) => {
    setRows(null);
    const [board, learners] = await Promise.all([
      getLeaderboard(s === 'all' ? undefined : s),
      getLearners(),
    ]);
    setRows(board);
    setRegistered(
      s === 'all' ? learners.length : learners.filter((l) => l.exam === s).length
    );
  }, []);

  useEffect(() => {
    void refresh(scope);
  }, [scope, refresh]);

  const mine = rows?.findIndex((r) => r.key === learner?.id) ?? -1;
  const myRow = mine >= 0 ? rows![mine] : null;

  return (
    <>
      <PageHeader
        title="Leaderboard"
        lead="Points from every published quiz, drill and castle run. Correct answers score 10, spot-the-error scenarios 15, a flawless run of five or more adds 25, and a slain dragon is worth 100."
        actions={
          <button className="btn btn-ghost" onClick={() => void refresh(scope)}>
            Refresh
          </button>
        }
      />

      {!learner || editing ? (
        <div className="mb-6">
          <ProfileForm onDone={() => { setEditing(false); void refresh(scope); }} />
        </div>
      ) : (
        <section className="card mb-6 flex flex-wrap items-center gap-4 p-5">
          <span className="accent-soft-bg flex h-14 w-14 items-center justify-center rounded-2xl text-3xl">
            {learner.emblem}
          </span>
          <div className="min-w-0">
            <p className="text-base font-semibold text-slate-900">
              {learner.handle}
            </p>
            <p className="mt-0.5 text-xs text-slate-500">
              Competing on {learner.exam}
              {myRow
                ? ` · ${myRow.points.toLocaleString()} points · rank ${mine + 1}`
                : ' · no published runs yet'}
            </p>
          </div>

          <label className="ml-auto flex cursor-pointer items-center gap-2 text-sm text-slate-600">
            <input
              type="checkbox"
              checked={settings.autoPublish}
              onChange={(e) => updateSettings({ autoPublish: e.target.checked })}
            />
            Publish my runs automatically
          </label>

          <div className="flex gap-2">
            <button className="btn btn-ghost" onClick={() => setEditing(true)}>
              New player
            </button>
            <button
              className="btn btn-ghost"
              onClick={() => setLearner(null)}
              title="Keeps your study progress; only removes the leaderboard identity from this browser"
            >
              Sign out
            </button>
          </div>
        </section>
      )}

      {myRow && (
        <section className="mb-6 grid gap-4 sm:grid-cols-4">
          <Stat label="Rank" value={`#${mine + 1}`} hint={`of ${rows!.length}`} />
          <Stat label="Points" value={myRow.points.toLocaleString()} />
          <Stat
            label="Accuracy"
            value={`${Math.round(myRow.accuracy * 100)}%`}
            hint={`${myRow.correct}/${myRow.answered} answered`}
          />
          <Stat
            label="Dragons slain"
            value={myRow.dragons}
            hint={`${myRow.runs} runs published`}
          />
        </section>
      )}

      <div className="mb-4 flex flex-wrap items-center gap-4">
        <div className="flex rounded-xl border border-slate-200 bg-slate-100 p-0.5">
        {([...EXAM_IDS, 'all'] as Scope[]).map((s) => (
          <button
            key={s}
            onClick={() => setScope(s)}
            className={`rounded-[10px] px-3 py-1.5 text-sm font-medium transition-colors ${
              scope === s
                ? 'bg-white text-slate-900 shadow-sm'
                : 'text-slate-500 hover:text-slate-700'
            }`}
          >
            {s === 'all' ? 'Both exams' : s}
          </button>
          ))}
        </div>
        {registered !== null && (
          <span className="text-xs text-slate-500">
            {registered} player{registered === 1 ? '' : 's'} registered
            {rows && rows.length < registered
              ? ` · ${rows.length} with a published run`
              : ''}
          </span>
        )}
      </div>

      <div className="card overflow-x-auto">
        {rows === null ? (
          <p className="px-5 py-10 text-center text-sm text-slate-400">Loading…</p>
        ) : rows.length === 0 ? (
          <div className="px-5 py-12 text-center">
            <p className="text-sm font-medium text-slate-800">
              Nobody has published a run yet.
            </p>
            <p className="mx-auto mt-2 max-w-sm text-sm text-slate-500">
              Finish a quiz or take on the castle and you will be first on the
              board.
            </p>
            <Link className="btn btn-primary mx-auto mt-4" to="/castle">
              Enter the castle
            </Link>
          </div>
        ) : (
          <table className="w-full min-w-[38rem] text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
                <th className="px-4 py-3 font-medium">#</th>
                <th className="px-2 py-3 font-medium">Player</th>
                <th className="px-2 py-3 text-right font-medium">Points</th>
                <th className="px-2 py-3 text-right font-medium">Accuracy</th>
                <th className="px-2 py-3 text-right font-medium">Runs</th>
                <th className="px-4 py-3 text-right font-medium">Dragons</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {rows.map((row, i) => {
                const isMe = row.key === learner?.id;
                return (
                  <tr key={row.key} className={isMe ? 'accent-soft-bg' : ''}>
                    <td className="px-4 py-2.5 tabular-nums text-slate-500">
                      {i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : i + 1}
                    </td>
                    <td className="px-2 py-2.5">
                      <span className="flex items-center gap-2">
                        <span className="text-lg" aria-hidden>
                          {row.emblem}
                        </span>
                        <span className="truncate font-medium text-slate-800">
                          {row.handle}
                        </span>
                        {isMe && <Chip tone="accent">you</Chip>}
                        {scope === 'all' && <Chip>{row.exam}</Chip>}
                      </span>
                    </td>
                    <td className="px-2 py-2.5 text-right font-semibold tabular-nums text-slate-900">
                      {row.points.toLocaleString()}
                    </td>
                    <td className="px-2 py-2.5 text-right tabular-nums text-slate-600">
                      {Math.round(row.accuracy * 100)}%
                    </td>
                    <td className="px-2 py-2.5 text-right tabular-nums text-slate-600">
                      {row.runs}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums text-slate-600">
                      {row.dragons > 0 ? `🐉 ${row.dragons}` : '—'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <p className="mt-4 text-xs leading-relaxed text-slate-500">
        Points are recalculated from each run when the board is built, so the
        stored value on a row cannot inflate a score. Profiles are not
        authenticated — this is a friendly scoreboard, not a verified ranking.
      </p>
    </>
  );
}
