import { beforeEach, describe, expect, it } from 'vitest';

import { DOMAINS } from '@/data/exams';
import { FLASHCARDS } from '@/data/flashcards';
import { QUESTIONS } from '@/data/questions';
import {
  BOX_INTERVALS,
  domainStats,
  isDue,
  MAX_BOX,
  overallMastery,
  recordAnswer,
  resetProgress,
  reviewCard,
  streak,
} from '@/lib/progress';

// The store is a module singleton reading from localStorage, so each test
// starts from a clean slate.
beforeEach(() => {
  resetProgress();
});

describe('Leitner scheduling', () => {
  it('treats an unseen card as due', () => {
    expect(isDue(undefined)).toBe(true);
  });

  it('holds a card back for the interval of its box', () => {
    const box3 = { box: 3, last: Date.now(), seen: 1, kept: 1 };
    expect(isDue(box3)).toBe(false);

    const aged = {
      ...box3,
      last: Date.now() - (BOX_INTERVALS[3] + 1) * 86_400_000,
    };
    expect(isDue(aged)).toBe(true);
  });

  it('promotes on a keep and drops straight back to box 1 on a miss', () => {
    const card = FLASHCARDS[0];
    reviewCard(card.id, true);
    reviewCard(card.id, true);

    const { cards } = JSON.parse(
      localStorage.getItem('fabricquiz.progress.v1')!
    ) as { cards: Record<string, { box: number }> };
    expect(cards[card.id].box).toBe(3);

    reviewCard(card.id, false);
    const after = JSON.parse(
      localStorage.getItem('fabricquiz.progress.v1')!
    ) as { cards: Record<string, { box: number }> };
    expect(after.cards[card.id].box).toBe(1);
  });

  it('never promotes past the last box', () => {
    const card = FLASHCARDS[1];
    for (let i = 0; i < MAX_BOX + 3; i++) reviewCard(card.id, true);
    const state = JSON.parse(
      localStorage.getItem('fabricquiz.progress.v1')!
    ) as { cards: Record<string, { box: number }> };
    expect(state.cards[card.id].box).toBe(MAX_BOX);
  });
});

describe('domain statistics', () => {
  it('counts only the latest attempt at a question', () => {
    const q = QUESTIONS.find((x) => x.exam === 'DP-600')!;
    recordAnswer({
      id: q.id,
      exam: q.exam,
      domainId: q.domainId,
      correct: false,
      at: 1_000,
    });
    recordAnswer({
      id: q.id,
      exam: q.exam,
      domainId: q.domainId,
      correct: true,
      at: 2_000,
    });

    const stats = domainStats(
      JSON.parse(localStorage.getItem('fabricquiz.progress.v1')!),
      'DP-600'
    );
    const stat = stats.find((s) => s.domainId === q.domainId)!;
    expect(stat.questionsSeen).toBe(1);
    expect(stat.questionsCorrect).toBe(1);
    expect(stat.accuracy).toBe(1);
  });

  it('returns a stat row per domain of the exam', () => {
    const stats = domainStats(
      JSON.parse(localStorage.getItem('fabricquiz.progress.v1')!),
      'DP-700'
    );
    expect(stats.map((s) => s.domainId).sort()).toEqual(
      DOMAINS.filter((d) => d.exam === 'DP-700')
        .map((d) => d.id)
        .sort()
    );
  });

  it('reports zero mastery for an untouched exam', () => {
    const stats = domainStats(
      JSON.parse(localStorage.getItem('fabricquiz.progress.v1')!),
      'DP-700'
    );
    expect(overallMastery(stats)).toBe(0);
  });
});

describe('streak', () => {
  const iso = (offsetDays: number) => {
    const d = new Date();
    d.setDate(d.getDate() - offsetDays);
    return d.toISOString().slice(0, 10);
  };

  it('counts consecutive days ending today', () => {
    expect(streak([iso(2), iso(1), iso(0)])).toBe(3);
  });

  it('survives a day that has not started yet', () => {
    expect(streak([iso(2), iso(1)])).toBe(2);
  });

  it('breaks on a gap', () => {
    expect(streak([iso(5), iso(4), iso(0)])).toBe(1);
  });

  it('is zero with no history', () => {
    expect(streak([])).toBe(0);
  });
});
