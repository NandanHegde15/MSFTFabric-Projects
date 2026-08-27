import type { ExamId } from '@/data/exams';
import { buildLeaderboard, scoreRun, type LeaderboardRow, type RunMode } from '@/lib/scoring';

import { getRayfinClient, isLocalBackend } from './rayfinClient';

export interface CommunityResult {
  id: string;
  exam: string;
  mode: string;
  domainId: string;
  correct: number;
  total: number;
  alias: string;
  learnerId?: string;
  emblem?: string;
  points?: number;
  outcome?: string;
  createdAt: Date;
}

export interface SubmitResultInput {
  exam: ExamId;
  mode: RunMode;
  domainId: string;
  correct: number;
  total: number;
  alias: string;
  learnerId?: string;
  emblem?: string;
  outcome?: 'victory' | 'defeat';
}

export interface CreateLearnerInput {
  handle: string;
  emblem: string;
  exam: ExamId;
}

export interface LearnerRecord {
  id: string;
  handle: string;
  emblem: string;
  exam: string;
  createdAt: Date;
}

/**
 * Local-dev fallback. When the app runs against `http://localhost` there is no
 * Rayfin backend, so recent runs are kept in memory for the session.
 */
let inMemoryResults: CommunityResult[] = [];
let inMemoryLearners: LearnerRecord[] = [];

const RESULT_FIELDS = [
  'id',
  'exam',
  'mode',
  'domainId',
  'correct',
  'total',
  'alias',
  'learnerId',
  'emblem',
  'points',
  'outcome',
  'createdAt',
] as const;

const LEARNER_FIELDS = ['id', 'handle', 'emblem', 'exam', 'createdAt'] as const;

/**
 * Claim a handle and store the profile.
 *
 * Returns the created row, or null when the backend is unreachable — the caller
 * decides whether to keep a local-only profile.
 */
export async function createLearner(
  input: CreateLearnerInput
): Promise<LearnerRecord | null> {
  const row = { ...input, createdAt: new Date() };

  if (isLocalBackend()) {
    const learner = { id: crypto.randomUUID(), ...row };
    inMemoryLearners = [learner, ...inMemoryLearners];
    return learner;
  }

  try {
    const created = await getRayfinClient().data.Learner.create(row);
    return created as LearnerRecord;
  } catch (err) {
    console.warn('Could not register the profile with the leaderboard.', err);
    return null;
  }
}

export async function getLearners(limit = 200): Promise<LearnerRecord[]> {
  if (isLocalBackend()) return inMemoryLearners.slice(0, limit);

  try {
    const rows = await getRayfinClient()
      .data.Learner.select([...LEARNER_FIELDS])
      .orderBy({ createdAt: 'desc' })
      .first(limit)
      .execute();
    return rows as LearnerRecord[];
  } catch (err) {
    console.warn('Could not read the learner roster.', err);
    return [];
  }
}

/**
 * Post a completed run.
 *
 * Every call is best-effort: the study app is fully usable with no backend, so
 * a failure here must never surface as an error to the learner.
 */
export async function submitResult(input: SubmitResultInput): Promise<boolean> {
  const row = {
    ...input,
    points: scoreRun({
      mode: input.mode,
      correct: input.correct,
      total: input.total,
      dragonSlain: input.outcome === 'victory',
    }),
    createdAt: new Date(),
  };

  if (isLocalBackend()) {
    inMemoryResults = [{ id: crypto.randomUUID(), ...row }, ...inMemoryResults].slice(
      0,
      200
    );
    return true;
  }

  try {
    await getRayfinClient().data.QuizResult.create(row);
    return true;
  } catch (err) {
    console.warn('Could not publish result to the leaderboard.', err);
    return false;
  }
}

export async function getCommunityResults(
  limit = 20
): Promise<CommunityResult[]> {
  if (isLocalBackend()) return inMemoryResults.slice(0, limit);

  try {
    const rows = await getRayfinClient()
      .data.QuizResult.select([...RESULT_FIELDS])
      .orderBy({ createdAt: 'desc' })
      .first(limit)
      .execute();
    return rows as CommunityResult[];
  } catch (err) {
    console.warn('Could not read the community board.', err);
    return [];
  }
}

/**
 * Build the ranked board.
 *
 * DAB has no aggregate support and the fluent client has no count(), so the
 * fold happens client-side over the most recent runs. 500 is comfortably inside
 * the 100,000-row page cap and keeps the payload small.
 */
export async function getLeaderboard(
  exam?: ExamId,
  sample = 500
): Promise<LeaderboardRow[]> {
  const results = await getCommunityResults(sample);
  return buildLeaderboard(results, exam);
}
