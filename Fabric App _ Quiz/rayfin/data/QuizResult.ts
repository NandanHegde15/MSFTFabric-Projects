import { anonymous, date, entity, int, text, uuid } from '@microsoft/rayfin-core';

/**
 * A single completed quiz, drill or castle run, submitted anonymously.
 *
 * The learner's handle and emblem are denormalised onto every row on purpose:
 * the leaderboard is a pure aggregation over this one table with no join, which
 * keeps the read path simple and sidesteps relationship limitations in DAB.
 *
 * `learnerId` is nullable so rows written before profiles existed still read.
 */
@entity()
@anonymous(['create', 'read'])
export class QuizResult {
  @uuid() id!: string;
  /** 'DP-600' | 'DP-700' */
  @text({ min: 1, max: 10 }) exam!: string;
  /** 'quiz' | 'spot-the-error' | 'castle' */
  @text({ min: 1, max: 20 }) mode!: string;
  /** Domain id, or 'mixed' when the run spanned several domains. */
  @text({ min: 1, max: 60 }) domainId!: string;
  @int() correct!: number;
  @int() total!: number;
  /** Display name at the time of the run. Not authenticated, not unique. */
  @text({ min: 1, max: 40 }) alias!: string;
  /** Learner.id, when the run was published from a profile. */
  @text({ max: 40, optional: true }) learnerId?: string;
  /** Emoji copied from the profile, so the board renders without a join. */
  @text({ max: 16, optional: true }) emblem?: string;
  /** Points awarded for the run — see scoreRun() in src/lib/scoring.ts. */
  @int({ optional: true }) points?: number;
  /** Castle runs only: 'victory' when the dragon fell, 'defeat' otherwise. */
  @text({ max: 10, optional: true }) outcome?: string;
  @date() createdAt!: Date;
}
