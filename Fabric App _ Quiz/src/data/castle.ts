import type { Difficulty } from './questions';

export interface CastleStage {
  id: string;
  name: string;
  /** Flavour shown on the stage intro card. */
  scene: string;
  /** How many questions must be answered to clear the stage. */
  questions: number;
  /** Draw from this domain index within the exam, or null for any domain. */
  domainIndex: number | null;
  /** Restrict to this difficulty, or null for any. */
  difficulty: Difficulty | null;
  boss?: boolean;
}

export const HEARTS = 3;
export const DRAGON_HP = 5;

/**
 * Six stages, mapped onto whichever exam is selected: the first three draw from
 * that exam's three domains in order, so a run is always a sweep of the whole
 * syllabus before the difficulty ramps.
 */
export const STAGES: CastleStage[] = [
  {
    id: 'drawbridge',
    name: 'The Drawbridge',
    scene:
      'The chains are rusted and the gatekeeper will not lower them for anyone who cannot answer plainly. Two questions stand between you and the courtyard.',
    questions: 2,
    domainIndex: 0,
    difficulty: null,
  },
  {
    id: 'gatehouse',
    name: 'The Gatehouse',
    scene:
      'Arrow slits on both sides. The guard captain tests whoever crosses — get it wrong here and the portcullis drops.',
    questions: 2,
    domainIndex: 1,
    difficulty: null,
  },
  {
    id: 'hall',
    name: 'The Great Hall',
    scene:
      'Long tables, cold hearth, and a steward who has clearly been asked these questions before. He does not repeat himself.',
    questions: 3,
    domainIndex: 2,
    difficulty: null,
  },
  {
    id: 'armoury',
    name: 'The Armoury',
    scene:
      'Racks of blades, none of them yours yet. Prove you understand the whole keep and you may take one to the tower.',
    questions: 3,
    domainIndex: null,
    difficulty: null,
  },
  {
    id: 'tower',
    name: 'The Spiral Tower',
    scene:
      'Two hundred steps, and something above is awake. The questions get sharper the higher you climb.',
    questions: 3,
    domainIndex: null,
    difficulty: 'tricky',
  },
  {
    id: 'lair',
    name: "The Dragon's Lair",
    scene:
      'Heat off the stone. Five strikes will fell it — every correct answer lands a blow, every mistake costs you a heart. You get a little room to miss, but not much.',
    // Two more questions than the dragon has health, so a single slip in the
    // lair is survivable rather than instantly fatal to the run.
    questions: DRAGON_HP + 2,
    domainIndex: null,
    difficulty: 'tricky',
    boss: true,
  },
];

export const TOTAL_QUESTIONS = STAGES.reduce((t, s) => t + s.questions, 0);
