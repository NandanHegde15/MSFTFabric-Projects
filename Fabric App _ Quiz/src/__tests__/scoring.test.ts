import { describe, expect, it } from 'vitest';

import { buildLeaderboard, scoreRun } from '@/lib/scoring';

describe('scoreRun', () => {
  it('awards 10 a correct answer for a quiz', () => {
    expect(scoreRun({ mode: 'quiz', correct: 7, total: 10 })).toBe(70);
  });

  it('pays a premium for spot-the-error', () => {
    expect(scoreRun({ mode: 'spot-the-error', correct: 4, total: 10 })).toBe(60);
  });

  it('adds a perfect-run bonus from five items up', () => {
    expect(scoreRun({ mode: 'quiz', correct: 5, total: 5 })).toBe(75);
  });

  it('does not pay the perfect bonus on a short run', () => {
    expect(scoreRun({ mode: 'quiz', correct: 4, total: 4 })).toBe(40);
  });

  it('adds 100 for the dragon', () => {
    expect(
      scoreRun({ mode: 'castle', correct: 12, total: 20, dragonSlain: true })
    ).toBe(220);
  });

  it('scores an empty run at zero', () => {
    expect(scoreRun({ mode: 'quiz', correct: 0, total: 0 })).toBe(0);
  });
});

describe('buildLeaderboard', () => {
  const run = (over: Partial<Parameters<typeof buildLeaderboard>[0][number]> = {}) => ({
    exam: 'DP-600',
    mode: 'quiz',
    correct: 8,
    total: 10,
    alias: 'Otter',
    learnerId: 'learner-1',
    emblem: '🦉',
    ...over,
  });

  it('groups runs by learner id and totals their points', () => {
    const rows = buildLeaderboard([run(), run({ correct: 5 })]);
    expect(rows).toHaveLength(1);
    expect(rows[0].points).toBe(80 + 50);
    expect(rows[0].runs).toBe(2);
    expect(rows[0].accuracy).toBeCloseTo(13 / 20);
  });

  it('falls back to the alias for rows written before profiles existed', () => {
    const rows = buildLeaderboard([
      run({ learnerId: undefined, alias: 'Legacy' }),
      run({ learnerId: undefined, alias: 'Legacy' }),
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0].key).toBe('alias:Legacy');
    expect(rows[0].runs).toBe(2);
  });

  it('keeps separate learners apart even when handles collide', () => {
    const rows = buildLeaderboard([
      run({ learnerId: 'a', alias: 'Twin' }),
      run({ learnerId: 'b', alias: 'Twin' }),
    ]);
    expect(rows).toHaveLength(2);
  });

  it('filters to one exam when asked', () => {
    const rows = buildLeaderboard(
      [run(), run({ exam: 'DP-700', learnerId: 'learner-2', alias: 'Other' })],
      'DP-700'
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].handle).toBe('Other');
  });

  it('ranks by points, breaking ties on accuracy', () => {
    const rows = buildLeaderboard([
      run({ learnerId: 'low', alias: 'Low', correct: 3, total: 10 }),
      run({ learnerId: 'high', alias: 'High', correct: 9, total: 10 }),
    ]);
    expect(rows.map((r) => r.handle)).toEqual(['High', 'Low']);
  });

  it('recomputes points and ignores a value supplied on the row', () => {
    const rows = buildLeaderboard([run({ points: 999_999 })]);
    expect(rows[0].points).toBe(80);
  });

  it('drops rows whose score is impossible', () => {
    const rows = buildLeaderboard([
      run({ correct: 50, total: 10 }),
      run({ correct: -5 }),
      run({ total: 0 }),
    ]);
    expect(rows).toEqual([]);
  });

  it('counts a dragon only on a castle victory', () => {
    const rows = buildLeaderboard([
      run({ mode: 'castle', outcome: 'victory' }),
      run({ mode: 'castle', outcome: 'defeat' }),
    ]);
    expect(rows[0].dragons).toBe(1);
  });
});
