import { describe, expect, it } from 'vitest';

import { DRAGON_HP, HEARTS, STAGES, TOTAL_QUESTIONS } from '@/data/castle';
import { DOMAINS, EXAM_IDS } from '@/data/exams';
import { QUESTIONS } from '@/data/questions';

describe('castle configuration', () => {
  it('ends on exactly one boss stage', () => {
    const bosses = STAGES.filter((s) => s.boss);
    expect(bosses).toHaveLength(1);
    expect(STAGES[STAGES.length - 1].boss).toBe(true);
  });

  it('gives the lair more questions than the dragon has health', () => {
    const lair = STAGES[STAGES.length - 1];
    expect(lair.questions).toBeGreaterThan(DRAGON_HP);
    // ...but not so many that hearts stop mattering.
    expect(lair.questions - DRAGON_HP).toBeLessThanOrEqual(HEARTS);
  });

  it('opens with one stage per exam domain, in order', () => {
    const domainStages = STAGES.filter((s) => s.domainIndex !== null);
    expect(domainStages.map((s) => s.domainIndex)).toEqual([0, 1, 2]);
    for (const exam of EXAM_IDS) {
      expect(DOMAINS.filter((d) => d.exam === exam)).toHaveLength(3);
    }
  });

  it('can be dealt without repeats from either exam bank', () => {
    for (const exam of EXAM_IDS) {
      const available = QUESTIONS.filter((q) => q.exam === exam).length;
      expect(available).toBeGreaterThanOrEqual(TOTAL_QUESTIONS);
    }
  });

  it('has enough tricky questions for the tower and the lair', () => {
    const trickyNeeded = STAGES.filter((s) => s.difficulty === 'tricky').reduce(
      (t, s) => t + s.questions,
      0
    );
    for (const exam of EXAM_IDS) {
      const tricky = QUESTIONS.filter(
        (q) => q.exam === exam && q.difficulty === 'tricky'
      ).length;
      expect(tricky).toBeGreaterThanOrEqual(trickyNeeded);
    }
  });

  it('gives every stage at least one question and a scene', () => {
    for (const stage of STAGES) {
      expect(stage.questions).toBeGreaterThan(0);
      expect(stage.scene.length).toBeGreaterThan(30);
    }
  });
});
