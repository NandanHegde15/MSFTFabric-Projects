import { anonymous, date, entity, text, uuid } from '@microsoft/rayfin-core';

/**
 * A player profile.
 *
 * This is deliberately NOT authentication. Deployed Fabric apps support Fabric
 * SSO only, which requires the Fabric portal — FabricQuiz is public, so there
 * is no sign-in to hang an identity off. A learner instead claims a handle,
 * and the browser keeps the generated id in localStorage.
 *
 * Consequences, which the UI states plainly: profiles are not secret, handles
 * are not reserved, and nothing here should be treated as an identity claim.
 * No email, no password, no PII.
 */
@entity()
@anonymous(['create', 'read'])
export class Learner {
  @uuid() id!: string;
  /** Display name chosen by the learner. */
  @text({ min: 1, max: 32 }) handle!: string;
  /** Emoji shown beside the handle on the leaderboard. */
  @text({ min: 1, max: 16 }) emblem!: string;
  /** The exam they are primarily working towards: 'DP-600' | 'DP-700'. */
  @text({ min: 1, max: 10 }) exam!: string;
  @date() createdAt!: Date;
}
