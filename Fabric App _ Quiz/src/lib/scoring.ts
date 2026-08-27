export type RunMode = 'quiz' | 'spot-the-error' | 'castle';

export interface ScorableRun {
  mode: RunMode;
  correct: number;
  total: number;
  /** Castle only: the dragon went down. */
  dragonSlain?: boolean;
}

/**
 * Points for a finished run.
 *
 * Deliberately simple and legible, because a leaderboard nobody can reason
 * about is a leaderboard nobody trusts:
 *
 *   - 10 points per correct answer
 *   - spot-the-error is worth 1.5x, since each scenario needs a diagnosis
 *     rather than a recall
 *   - a flawless run adds a 25-point bonus, but only from 5 items up, so a
 *     one-question run cannot be farmed
 *   - slaying the dragon adds 100
 */
export function scoreRun(run: ScorableRun): number {
  if (run.total <= 0) return 0;

  const perCorrect = run.mode === 'spot-the-error' ? 15 : 10;
  let points = run.correct * perCorrect;

  if (run.total >= 5 && run.correct === run.total) points += 25;
  if (run.dragonSlain) points += 100;

  return points;
}

export interface LeaderboardRow {
  key: string;
  handle: string;
  emblem: string;
  exam: string;
  points: number;
  runs: number;
  correct: number;
  answered: number;
  accuracy: number;
  dragons: number;
}

interface AggregatableResult {
  exam: string;
  mode: string;
  correct: number;
  total: number;
  alias: string;
  learnerId?: string;
  emblem?: string;
  points?: number;
  outcome?: string;
}

/**
 * Fold published runs into a ranked board.
 *
 * Rows are grouped by learner id where one is present, and by handle otherwise
 * so the runs published before profiles existed still appear. `points` is
 * recomputed from the run rather than trusted from the row, so a hand-crafted
 * insert cannot award itself an arbitrary score.
 */
export function buildLeaderboard(
  results: AggregatableResult[],
  exam?: string
): LeaderboardRow[] {
  const rows = new Map<string, LeaderboardRow>();

  for (const r of results) {
    if (exam && r.exam !== exam) continue;
    if (!Number.isFinite(r.correct) || !Number.isFinite(r.total)) continue;
    if (r.total <= 0 || r.correct < 0 || r.correct > r.total) continue;

    const key = r.learnerId || `alias:${r.alias}`;
    const mode: RunMode =
      r.mode === 'castle' || r.mode === 'spot-the-error' ? r.mode : 'quiz';
    const dragonSlain = r.mode === 'castle' && r.outcome === 'victory';

    const row =
      rows.get(key) ??
      {
        key,
        handle: r.alias,
        emblem: r.emblem || '🎓',
        exam: r.exam,
        points: 0,
        runs: 0,
        correct: 0,
        answered: 0,
        accuracy: 0,
        dragons: 0,
      };

    row.points += scoreRun({ mode, correct: r.correct, total: r.total, dragonSlain });
    row.runs += 1;
    row.correct += r.correct;
    row.answered += r.total;
    if (dragonSlain) row.dragons += 1;
    // Latest row wins for display, so a renamed profile shows its current name.
    row.handle = r.alias;
    if (r.emblem) row.emblem = r.emblem;

    rows.set(key, row);
  }

  return [...rows.values()]
    .map((r) => ({
      ...r,
      accuracy: r.answered > 0 ? r.correct / r.answered : 0,
    }))
    .sort((a, b) => b.points - a.points || b.accuracy - a.accuracy);
}
