import { describe, expect, it } from 'vitest';

import { QUESTIONS } from '@/data/questions';
import { presentQuestion } from '@/lib/present';

describe('presentQuestion', () => {
  it('keeps the same set of choices', () => {
    for (const q of QUESTIONS) {
      const shown = presentQuestion(q);
      expect([...shown.choices].sort()).toEqual([...q.choices].sort());
      expect(shown.choices).toHaveLength(q.choices.length);
    }
  });

  it('remaps every answer index to the same choice text', () => {
    for (const q of QUESTIONS) {
      const shown = presentQuestion(q);
      const before = q.answer.map((i) => q.choices[i]).sort();
      const after = shown.answer.map((i) => shown.choices[i]).sort();
      expect(after).toEqual(before);
    }
  });

  it('returns answer indexes in range and in order', () => {
    for (const q of QUESTIONS) {
      const shown = presentQuestion(q);
      expect(shown.answer).toEqual([...shown.answer].sort((a, b) => a - b));
      for (const a of shown.answer) {
        expect(a).toBeGreaterThanOrEqual(0);
        expect(a).toBeLessThan(shown.choices.length);
      }
    }
  });

  it('does not mutate the source question', () => {
    const original = QUESTIONS[0];
    const snapshot = JSON.stringify(original);
    presentQuestion(original);
    expect(JSON.stringify(original)).toBe(snapshot);
  });

  it('actually moves the correct answer off position zero', () => {
    // The bank is authored answer-first. Over many deals the correct choice
    // must land elsewhere most of the time, or the shuffle is not working.
    const single = QUESTIONS.find((q) => q.answer.length === 1)!;
    const positions = new Set<number>();
    for (let i = 0; i < 200; i++) {
      positions.add(presentQuestion(single).answer[0]);
    }
    expect(positions.size).toBe(single.choices.length);
  });
});
