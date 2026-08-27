import { useCallback, useSyncExternalStore } from 'react';

import { DOMAINS, type ExamId } from '@/data/exams';
import { FLASHCARDS } from '@/data/flashcards';
import { QUESTIONS } from '@/data/questions';
import { SCENARIOS } from '@/data/spotTheError';

import type { RunMode } from './scoring';

// Still v1: every field added since is optional and defaulted on load, so an
// existing learner's progress survives the upgrade untouched.
const STORAGE_KEY = 'fabricquiz.progress.v1';

/** Leitner box intervals in days. Box 1 is always due. */
export const BOX_INTERVALS = [0, 0, 1, 3, 7, 21];
export const MAX_BOX = 5;

export interface CardState {
  /** Leitner box, 1..5. 5 means retired for three weeks at a time. */
  box: number;
  /** Epoch ms of the last review. */
  last: number;
  seen: number;
  kept: number;
}

export interface Attempt {
  id: string;
  exam: ExamId;
  domainId: string;
  correct: boolean;
  at: number;
}

export interface RunRecord {
  at: number;
  exam: ExamId;
  mode: RunMode;
  domainId: string;
  correct: number;
  total: number;
  points?: number;
  outcome?: 'victory' | 'defeat';
}

/** The locally-held half of a player profile. See rayfin/data/Learner.ts. */
export interface LearnerProfile {
  id: string;
  handle: string;
  emblem: string;
  exam: ExamId;
  createdAt: number;
}

export interface Settings {
  /** Read explanations aloud as soon as they are revealed. */
  autoSpeak: boolean;
  /** Speech rate, 0.5 to 2. */
  speechRate: number;
  /** Publish finished runs to the leaderboard without asking. */
  autoPublish: boolean;
}

export const DEFAULT_SETTINGS: Settings = {
  autoSpeak: false,
  speechRate: 1,
  autoPublish: true,
};

export interface ProgressState {
  version: 1;
  alias: string;
  exam: ExamId;
  learner: LearnerProfile | null;
  settings: Settings;
  cards: Record<string, CardState>;
  answers: Attempt[];
  spots: Attempt[];
  /** ISO yyyy-mm-dd strings on which the learner did something. */
  days: string[];
  runs: RunRecord[];
}

const ADJECTIVES = [
  'Delta',
  'Lakehouse',
  'Vertipaq',
  'Medallion',
  'Kusto',
  'Parquet',
  'Shortcut',
  'Onelake',
];
const NOUNS = ['Owl', 'Otter', 'Falcon', 'Badger', 'Heron', 'Ibis', 'Lynx', 'Marlin'];

function randomAlias(): string {
  const a = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const n = NOUNS[Math.floor(Math.random() * NOUNS.length)];
  return `${a} ${n}`;
}

function emptyState(): ProgressState {
  return {
    version: 1,
    alias: randomAlias(),
    exam: 'DP-600',
    learner: null,
    settings: { ...DEFAULT_SETTINGS },
    cards: {},
    answers: [],
    spots: [],
    days: [],
    runs: [],
  };
}

/** Merge a stored blob over the defaults, nested objects included. */
function hydrate(parsed: Partial<ProgressState>): ProgressState {
  const base = emptyState();
  return {
    ...base,
    ...parsed,
    version: 1,
    settings: { ...base.settings, ...(parsed.settings ?? {}) },
    learner: parsed.learner ?? null,
  };
}

function load(): ProgressState {
  if (typeof window === 'undefined') return emptyState();
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return emptyState();
    return hydrate(JSON.parse(raw) as Partial<ProgressState>);
  } catch {
    return emptyState();
  }
}

let state: ProgressState = load();
const listeners = new Set<() => void>();

function persist() {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Private browsing or a full quota — the app keeps working in memory.
  }
}

function emit() {
  persist();
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function getSnapshot() {
  return state;
}

export function today(): string {
  return new Date().toISOString().slice(0, 10);
}

function withToday(s: ProgressState): ProgressState {
  const d = today();
  return s.days.includes(d) ? s : { ...s, days: [...s.days, d] };
}

function update(fn: (s: ProgressState) => ProgressState) {
  state = withToday(fn(state));
  emit();
}

// ---------------------------------------------------------------- mutations

export function setExam(exam: ExamId) {
  update((s) => ({ ...s, exam }));
}

export function setAlias(alias: string) {
  update((s) => ({ ...s, alias: alias.slice(0, 40) || randomAlias() }));
}

export function setLearner(learner: LearnerProfile | null) {
  update((s) => ({
    ...s,
    learner,
    alias: learner ? learner.handle : s.alias,
  }));
}

export function updateSettings(changes: Partial<Settings>) {
  update((s) => ({ ...s, settings: { ...s.settings, ...changes } }));
}

export function reviewCard(cardId: string, kept: boolean) {
  update((s) => {
    const prev = s.cards[cardId] ?? { box: 1, last: 0, seen: 0, kept: 0 };
    const box = kept ? Math.min(MAX_BOX, prev.box + 1) : 1;
    return {
      ...s,
      cards: {
        ...s.cards,
        [cardId]: {
          box,
          last: Date.now(),
          seen: prev.seen + 1,
          kept: prev.kept + (kept ? 1 : 0),
        },
      },
    };
  });
}

export function recordAnswer(a: Attempt) {
  update((s) => ({ ...s, answers: [...s.answers, a].slice(-2000) }));
}

export function recordSpot(a: Attempt) {
  update((s) => ({ ...s, spots: [...s.spots, a].slice(-2000) }));
}

export function recordRun(run: RunRecord) {
  update((s) => ({ ...s, runs: [...s.runs, run].slice(-200) }));
}

export function resetProgress() {
  // The profile and preferences survive a progress reset — wiping study history
  // should not silently orphan the learner's leaderboard identity.
  state = {
    ...emptyState(),
    alias: state.alias,
    exam: state.exam,
    learner: state.learner,
    settings: { ...state.settings },
  };
  emit();
}

export function exportProgress(): string {
  return JSON.stringify(state, null, 2);
}

export function importProgress(json: string): boolean {
  try {
    const parsed = JSON.parse(json) as Partial<ProgressState>;
    if (!parsed || typeof parsed !== 'object') return false;
    state = hydrate(parsed);
    emit();
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------- selectors

export function useProgress(): ProgressState {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

export function useExam(): [ExamId, (e: ExamId) => void] {
  const { exam } = useProgress();
  return [exam, useCallback((e: ExamId) => setExam(e), [])];
}

export function isDue(card: CardState | undefined): boolean {
  if (!card) return true;
  const days = BOX_INTERVALS[card.box] ?? 0;
  return Date.now() - card.last >= days * 86_400_000;
}

export interface DomainStat {
  domainId: string;
  short: string;
  title: string;
  weight: [number, number];
  cardsTotal: number;
  cardsMastered: number;
  questionsTotal: number;
  questionsSeen: number;
  questionsCorrect: number;
  accuracy: number | null;
  scenariosTotal: number;
  scenariosSeen: number;
  /** 0-100. Coverage times accuracy, so both breadth and correctness count. */
  mastery: number;
}

/** Latest attempt wins, so a re-answered question reflects current knowledge. */
function latestByItem(attempts: Attempt[]): Map<string, Attempt> {
  const map = new Map<string, Attempt>();
  for (const a of attempts) {
    const prev = map.get(a.id);
    if (!prev || a.at >= prev.at) map.set(a.id, a);
  }
  return map;
}

export function domainStats(s: ProgressState, exam: ExamId): DomainStat[] {
  const answers = latestByItem(s.answers.filter((a) => a.exam === exam));
  const spots = latestByItem(s.spots.filter((a) => a.exam === exam));

  return DOMAINS.filter((d) => d.exam === exam).map((d) => {
    const cards = FLASHCARDS.filter((c) => c.domainId === d.id);
    const cardsMastered = cards.filter((c) => (s.cards[c.id]?.box ?? 0) >= 3).length;

    const qs = QUESTIONS.filter((q) => q.domainId === d.id);
    const seen = qs.filter((q) => answers.has(q.id));
    const correct = seen.filter((q) => answers.get(q.id)!.correct);

    const sc = SCENARIOS.filter((x) => x.domainId === d.id);
    const scSeen = sc.filter((x) => spots.has(x.id));
    const scCorrect = scSeen.filter((x) => spots.get(x.id)!.correct);

    const attempted = seen.length + scSeen.length;
    const gotRight = correct.length + scCorrect.length;
    const total = qs.length + sc.length;

    const accuracy = attempted > 0 ? gotRight / attempted : null;
    const coverage = total > 0 ? attempted / total : 0;
    const cardShare = cards.length > 0 ? cardsMastered / cards.length : 0;

    // Two thirds practice, one third recall — both have to move for mastery to climb.
    const mastery = Math.round(
      100 * ((accuracy ?? 0) * coverage * 0.67 + cardShare * 0.33)
    );

    return {
      domainId: d.id,
      short: d.short,
      title: d.title,
      weight: d.weight,
      cardsTotal: cards.length,
      cardsMastered,
      questionsTotal: qs.length,
      questionsSeen: seen.length,
      questionsCorrect: correct.length,
      accuracy,
      scenariosTotal: sc.length,
      scenariosSeen: scSeen.length,
      mastery,
    };
  });
}

export function overallMastery(stats: DomainStat[]): number {
  if (stats.length === 0) return 0;
  // Weight each domain by the midpoint of its exam weighting band.
  const totalWeight = stats.reduce((t, s) => t + (s.weight[0] + s.weight[1]) / 2, 0);
  if (totalWeight === 0) return 0;
  return Math.round(
    stats.reduce((t, s) => t + s.mastery * ((s.weight[0] + s.weight[1]) / 2), 0) /
      totalWeight
  );
}

export function streak(days: string[]): number {
  if (days.length === 0) return 0;
  const set = new Set(days);
  let count = 0;
  const cursor = new Date();
  // Allow the streak to be "alive" if yesterday counted but today has not started.
  if (!set.has(cursor.toISOString().slice(0, 10))) {
    cursor.setDate(cursor.getDate() - 1);
    if (!set.has(cursor.toISOString().slice(0, 10))) return 0;
  }
  for (;;) {
    if (!set.has(cursor.toISOString().slice(0, 10))) break;
    count += 1;
    cursor.setDate(cursor.getDate() - 1);
  }
  return count;
}

export function dueCount(s: ProgressState, exam: ExamId): number {
  return FLASHCARDS.filter((c) => c.exam === exam && isDue(s.cards[c.id])).length;
}
