import { describe, expect, it } from 'vitest';

import { DOMAINS, EXAM_IDS } from '@/data/exams';
import { FLASHCARDS } from '@/data/flashcards';
import { QUESTIONS } from '@/data/questions';
import { SCENARIOS } from '@/data/spotTheError';

const domainIds = new Set(DOMAINS.map((d) => d.id));
const objectiveIds = new Set(DOMAINS.flatMap((d) => d.objectives.map((o) => o.id)));

function duplicates(ids: string[]): string[] {
  const seen = new Set<string>();
  return ids.filter((id) => (seen.has(id) ? true : (seen.add(id), false)));
}

describe('exam metadata', () => {
  it('covers both exams with domains that sum to a plausible weighting', () => {
    for (const exam of EXAM_IDS) {
      const ds = DOMAINS.filter((d) => d.exam === exam);
      expect(ds.length).toBeGreaterThan(0);
      const low = ds.reduce((t, d) => t + d.weight[0], 0);
      const high = ds.reduce((t, d) => t + d.weight[1], 0);
      expect(low).toBeLessThanOrEqual(100);
      expect(high).toBeGreaterThanOrEqual(100);
    }
  });

  it('has unique domain and objective ids', () => {
    expect(duplicates(DOMAINS.map((d) => d.id))).toEqual([]);
    expect(duplicates([...objectiveIds])).toEqual([]);
  });
});

describe('flashcards', () => {
  it('have unique ids and resolve to a domain of the same exam', () => {
    expect(duplicates(FLASHCARDS.map((c) => c.id))).toEqual([]);
    for (const card of FLASHCARDS) {
      expect(domainIds.has(card.domainId)).toBe(true);
      const domain = DOMAINS.find((d) => d.id === card.domainId)!;
      expect(domain.exam).toBe(card.exam);
    }
  });

  it('give every domain something to study', () => {
    for (const domain of DOMAINS) {
      expect(
        FLASHCARDS.filter((c) => c.domainId === domain.id).length
      ).toBeGreaterThan(0);
    }
  });
});

describe('questions', () => {
  it('have unique ids and valid domain and objective references', () => {
    expect(duplicates(QUESTIONS.map((q) => q.id))).toEqual([]);
    for (const q of QUESTIONS) {
      expect(domainIds.has(q.domainId)).toBe(true);
      expect(objectiveIds.has(q.objectiveId)).toBe(true);
      expect(DOMAINS.find((d) => d.id === q.domainId)!.exam).toBe(q.exam);
    }
  });

  it('have at least one answer, all in range, with no duplicates', () => {
    for (const q of QUESTIONS) {
      expect(q.choices.length).toBeGreaterThanOrEqual(2);
      expect(q.answer.length).toBeGreaterThan(0);
      expect(q.answer.length).toBeLessThan(q.choices.length);
      expect(new Set(q.answer).size).toBe(q.answer.length);
      for (const a of q.answer) {
        expect(a).toBeGreaterThanOrEqual(0);
        expect(a).toBeLessThan(q.choices.length);
      }
    }
  });

  it('state the count when more than one choice is correct', () => {
    for (const q of QUESTIONS.filter((x) => x.answer.length > 1)) {
      expect(q.stem.toLowerCase()).toContain('choose');
    }
  });

  it('always explain the answer', () => {
    for (const q of QUESTIONS) {
      expect(q.explanation.length).toBeGreaterThan(40);
    }
  });
});

describe('spot-the-error scenarios', () => {
  it('have unique ids and a faulty line that exists', () => {
    expect(duplicates(SCENARIOS.map((s) => s.id))).toEqual([]);
    for (const s of SCENARIOS) {
      expect(domainIds.has(s.domainId)).toBe(true);
      expect(DOMAINS.find((d) => d.id === s.domainId)!.exam).toBe(s.exam);
      expect(s.lines.length).toBeGreaterThan(1);
      expect(s.faultyLine).toBeGreaterThanOrEqual(0);
      expect(s.faultyLine).toBeLessThan(s.lines.length);
    }
  });

  it('never mark a decoy note on the faulty line itself', () => {
    for (const s of SCENARIOS) {
      for (const key of Object.keys(s.decoys ?? {})) {
        const line = Number(key);
        expect(line).not.toBe(s.faultyLine);
        expect(line).toBeGreaterThanOrEqual(0);
        expect(line).toBeLessThan(s.lines.length);
      }
    }
  });
});
