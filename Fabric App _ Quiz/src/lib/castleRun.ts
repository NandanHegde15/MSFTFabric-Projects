export interface RunState {
  /** Questions already answered in the current stage, including the last one. */
  questionIndex: number;
  hearts: number;
  dragonHp: number;
}

export interface StageContext {
  questionCount: number;
  isBoss: boolean;
  isLastStage: boolean;
}

export type RunStep =
  | { kind: 'next-question' }
  | { kind: 'stage-cleared' }
  | { kind: 'victory' }
  | { kind: 'defeat' };

/**
 * Decide what happens after an answer has been graded.
 *
 * Order matters and is the whole point of pulling this out of the component:
 *
 *  1. No hearts left ends the run immediately, wherever you are.
 *  2. A dead dragon is a win even with lair questions to spare.
 *  3. Otherwise continue through the stage.
 *  4. Running out of lair questions with the dragon alive is a loss, not a win.
 */
export function nextStep(state: RunState, stage: StageContext): RunStep {
  if (state.hearts <= 0) return { kind: 'defeat' };

  if (stage.isBoss && state.dragonHp <= 0) return { kind: 'victory' };

  if (state.questionIndex + 1 < stage.questionCount) {
    return { kind: 'next-question' };
  }

  if (stage.isBoss) return { kind: 'defeat' };
  if (stage.isLastStage) return { kind: 'victory' };

  return { kind: 'stage-cleared' };
}
