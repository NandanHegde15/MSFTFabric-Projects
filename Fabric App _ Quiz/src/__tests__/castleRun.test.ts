import { describe, expect, it } from 'vitest';

import { DRAGON_HP, HEARTS, STAGES } from '@/data/castle';
import { nextStep, type StageContext } from '@/lib/castleRun';

const mid: StageContext = { questionCount: 3, isBoss: false, isLastStage: false };
const lair: StageContext = {
  questionCount: STAGES[STAGES.length - 1].questions,
  isBoss: true,
  isLastStage: true,
};

const healthy = { questionIndex: 0, hearts: HEARTS, dragonHp: DRAGON_HP };

describe('nextStep', () => {
  it('moves to the next question mid-stage', () => {
    expect(nextStep(healthy, mid)).toEqual({ kind: 'next-question' });
  });

  it('clears the stage on its last question', () => {
    expect(nextStep({ ...healthy, questionIndex: 2 }, mid)).toEqual({
      kind: 'stage-cleared',
    });
  });

  it('ends the run the moment hearts hit zero, mid-stage', () => {
    expect(nextStep({ ...healthy, hearts: 0 }, mid)).toEqual({ kind: 'defeat' });
  });

  it('ends the run on zero hearts even if the dragon is already dead', () => {
    // Hearts are checked first: you cannot die and win on the same answer.
    expect(
      nextStep({ questionIndex: 4, hearts: 0, dragonHp: 0 }, lair)
    ).toEqual({ kind: 'defeat' });
  });

  it('wins immediately when the dragon falls, with questions to spare', () => {
    expect(
      nextStep({ questionIndex: 4, hearts: 1, dragonHp: 0 }, lair)
    ).toEqual({ kind: 'victory' });
  });

  it('keeps fighting while the dragon still has health', () => {
    expect(
      nextStep({ questionIndex: 2, hearts: 2, dragonHp: 3 }, lair)
    ).toEqual({ kind: 'next-question' });
  });

  it('loses when the lair runs out of questions and the dragon survives', () => {
    expect(
      nextStep(
        { questionIndex: lair.questionCount - 1, hearts: 3, dragonHp: 1 },
        lair
      )
    ).toEqual({ kind: 'defeat' });
  });

  it('wins on the final non-boss stage, as a safety net', () => {
    expect(
      nextStep({ ...healthy, questionIndex: 2 }, { ...mid, isLastStage: true })
    ).toEqual({ kind: 'victory' });
  });

  it('ignores dragon health outside the lair', () => {
    expect(nextStep({ ...healthy, dragonHp: 0 }, mid)).toEqual({
      kind: 'next-question',
    });
  });

  it('never reports victory in the lair on a full-health dragon', () => {
    for (let q = 0; q < lair.questionCount; q++) {
      const step = nextStep({ questionIndex: q, hearts: 3, dragonHp: DRAGON_HP }, lair);
      expect(step.kind).not.toBe('victory');
    }
  });
});
