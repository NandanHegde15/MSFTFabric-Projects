# FabricQuiz

A public, no-sign-in study app for the Microsoft Fabric certifications **DP-600**
(Fabric Analytics Engineer Associate) and **DP-700** (Fabric Data Engineer Associate).

Built on Rayfin and deployed as a Fabric data app: a static React frontend served
from OneLake, plus two anonymous-access Rayfin entities — `Learner` and
`QuizResult` — behind the leaderboard.

> Unofficial community study aid. Not affiliated with or endorsed by Microsoft.
> Exam objectives and weightings change — always check the official skills
> outline before you book.

<img width="987" height="1142" alt="Rayfin1" src="https://github.com/user-attachments/assets/c27c46c1-0b9b-4a41-b131-ae97f46787bb" />

<img width="982" height="1110" alt="Rayfin2" src="https://github.com/user-attachments/assets/b98e149d-291f-4997-aafa-dc3aab246c24" />

<img width="1462" height="1000" alt="Rayfin3" src="https://github.com/user-attachments/assets/9bdfaa26-8c21-4d4b-b2bb-c3686355072a" />

<img width="1450" height="982" alt="Rayfin4" src="https://github.com/user-attachments/assets/e657d4b8-0a2f-46bf-bd5f-74906a6a279f" />

<img width="1492" height="1277" alt="Rayfin5" src="https://github.com/user-attachments/assets/b1a47ff6-dd45-4014-b85a-8b53cb876842" />

<img width="1455" height="935" alt="Rayfin6" src="https://github.com/user-attachments/assets/8176eddf-6a53-4223-8337-440e55d461b9" />

<img width="1005" height="1580" alt="Rayfin7" src="https://github.com/user-attachments/assets/b05eb89b-5c2d-481a-ad69-e0dedeb48441" />

<img width="1930" height="1055" alt="Rayfin8" src="https://github.com/user-attachments/assets/724b84ed-b479-44e4-b4d1-ea5168eaa106" />

<img width="995" height="1322" alt="Rayfin9" src="https://github.com/user-attachments/assets/2e9c4074-431d-4946-a55c-e85c8a7818c0" />




## What it does

| Tool | What it is for |
|------|----------------|
| **Ecosystem model** | The whole Fabric platform as an interactive 3D model. Click a component to inspect it, double-click to take it apart. Fabric → Data Factory → Data pipelines → activities, connections, triggers, and so on down to individual capabilities. Each node carries exam-relevant facts and links to the flashcards covering it. |
| **Flashcards** | Leitner spaced repetition over terms, numbers and gotchas — SKUs, CU maths, workspace roles, storage modes, Delta maintenance. Keep a card and it moves up a box; miss it and it goes back to box 1. |
| **Quiz** | Scenario questions drawn per exam objective area, single and multi-select, with an explanation on every answer covering why the distractors are wrong. |
| **Spot the error** | A broken query, config or briefing note. Click the one line that is wrong. Picking a plausible-but-fine line explains why it is fine. |
| **Capacity lab** | An interactive CU calculator: SKU to capacity units, CU-seconds per day, a workload builder that reports utilisation and the smallest SKU that fits, and a throttling-stage explorer driven by carry-forward minutes. |
| **Castle journey** | The quiz as a game. Six stages of the keep, three hearts, and a dragon with five health at the end. A wrong answer costs a heart; a correct one in the lair lands a blow. Run out of hearts and it ends where you stand. |
| **Leaderboard** | Create a player, publish your runs, and rank against everyone else revising. Points are recomputed from each run when the board is built. |
| **Progress** | Readiness per exam domain, weighted the way the exam weights it, plus session history, voice settings, export/import and reset. |

Every explanation, flashcard and scenario has a **Listen** button that reads it aloud
using the browser's own speech synthesis — nothing is sent anywhere. Turn on
autoplay and adjust the speed from the Progress tab.

Content lives in `src/data/` as plain TypeScript, so adding a card, a question or
a scenario is a one-file change and is validated by the test suite.

## Design decisions

- **Public by default.** Nothing is behind a sign-in. `bootstrapAuth()` is called
  only to initialise the Rayfin client for the community board, and it is wrapped
  in a `try`/`catch` so a missing backend degrades to a fully working study app.
- **Share the hosting URL, not the portal link.** `rayfin up` prints both. The
  `*.webapp.fabricapps.net` URL is the public app; the
  `app.fabric.microsoft.com/groups/...` deep link is the Fabric portal item and
  always requires sign-in.
- **Progress is local.** All learner state lives in one `localStorage` key
  (`fabricquiz.progress.v1`). Nothing is uploaded unless the learner presses
  *Publish score*.
- **Hash routing.** Static hosting has no SPA rewrite guarantee, so `#/quiz`
  is used instead of `/quiz` — deep links always resolve.
- **Profiles, not accounts.** Deployed Fabric apps support Fabric SSO only, and
  SSO needs the Fabric portal — a public app has no sign-in to hang an identity
  off. So `Learner` is a claimed handle plus an emblem, with the generated id
  kept in `localStorage`. There is no password, handles are not reserved, and
  the UI says so plainly. Treat the board as a friendly scoreboard.
- **Anonymous data.** `Learner` and `QuizResult` are both
  `@anonymous(['create', 'read'])` and hold no PII. Anonymous insert is a
  deliberate trade-off for a public leaderboard; drop the `create` action if you
  would rather it were read-only.
- **Denormalised results.** Handle, emblem and learner id are copied onto every
  `QuizResult` row, so the board is a single-table aggregation with no join —
  which also sidesteps DAB relationship limits and the absence of `count()`.
- **Points are recomputed, never trusted.** `buildLeaderboard` recalculates the
  score from each run and discards rows whose `correct`/`total` are impossible,
  so a hand-crafted insert into the public table cannot award itself a score.
- **The 3D scene is imperative.** `EcosystemScene` owns three.js directly and
  repositions DOM labels itself each frame, so React never re-renders during
  animation. Canvas sizing is driven by a ResizeObserver *and* re-checked every
  frame — observer deliveries are suspended while a tab is hidden, and a resize
  missed in that window would otherwise never be corrected.
- **three.js is not in the main bundle.** The `/explore` route is `React.lazy`'d
  and the canvas is lazy'd again inside it, so three.js (~139 kB gzipped) and
  the component tree only load for people who open the explorer.
- **Choices are shuffled at deal time.** The bank is authored correct-answer-first
  for readability; `presentQuestion` randomises the order so position carries no
  signal.

## Getting started

```bash
npm install
```

Local development without touching Fabric — the app is fully functional, and the
community board falls back to in-memory storage:

```bash
npx vite
```

Deploy the backend and run against it:

```bash
npm run dev
```


## Project structure

```text
├── rayfin/
│   ├── rayfin.yml              # Fabric service configuration
│   └── data/
│       ├── Learner.ts          # Anonymous player profile
│       ├── QuizResult.ts       # Anonymous-access run record behind the leaderboard
│       └── schema.ts           # Schema export consumed by the typed client
├── src/
│   ├── main.tsx                # Entry point + Rayfin client bootstrap
│   ├── App.tsx                 # Hash routes
│   ├── data/
│   │   ├── exams.ts            # Exams, domains, weightings, objectives
│   │   ├── flashcards.ts       # Flashcard bank
│   │   ├── questions.ts        # Quiz bank
│   │   ├── spotTheError.ts     # Spot-the-error scenarios
│   │   ├── castle.ts           # Castle stages, hearts, dragon health
│   │   └── ecosystem.ts        # The Fabric component tree behind the 3D model
│   ├── lib/
│   │   ├── progress.ts         # localStorage store, Leitner scheduling, domain stats
│   │   ├── scoring.ts          # Points per run, leaderboard fold
│   │   ├── castleRun.ts        # Pure next-step decision for a castle run
│   │   ├── present.ts          # Choice shuffling
│   │   ├── speech.ts           # Single-speaker Web Speech wrapper
│   │   └── shuffle.ts
│   ├── three/
│   │   ├── EcosystemScene.ts   # Imperative three.js scene, labels and interaction
│   │   └── EcosystemCanvas.tsx # Lazy-loaded React wrapper around the scene
│   ├── components/             # Layout, shared UI, profile form, Listen button, castle art
│   ├── pages/                  # Dashboard, Explorer, Flashcards, Quiz, Spot, Castle, Capacity, Leaderboard, Progress
│   └── services/
│       ├── rayfinClient.ts     # Typed Rayfin client singleton
│       ├── bootstrap.ts        # Reads env, initialises the client
│       └── results.ts          # Community board reads/writes (best-effort)
└── package.json
```

## Adding content

Append to the relevant array in `src/data/`. The test suite enforces that:

- ids are unique
- every item resolves to a domain belonging to its own exam
- every question has at least one answer, all indexes in range, and an explanation
- multi-answer questions say how many to choose
- every scenario's `faultyLine` exists, and no decoy note points at it
- the castle can be dealt from either exam bank without repeating a question,
  and has enough tricky questions for the tower and the lair
- shuffling choices never changes which text is correct
- every ecosystem node has a unique id, a summary and at least one fact
- ecosystem nodes only reference flashcards and domains that exist
- the model goes at least three levels deep and no ring exceeds nine components

```bash
npm test
```

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Deploy the backend to Fabric, then start the local dev server |
| `npm run build` | Production build |
| `npm run build:fabric` | Build for Fabric deployment (used by `rayfin up`) |
| `npm run lint` | Lint with ESLint |
| `npm test` | Run the Vitest suite |
| `npm run rayfin:db` | Apply database migrations only |

## Deploy

```bash
npx rayfin login
npx rayfin up --workspace "<your Fabric workspace name>"
```

`rayfin up` builds the static bundle, uploads it, applies pending schema
migrations, and appends the live hosting URL to `allowedRedirectUris`.
