import { Suspense, lazy } from 'react';
import { HashRouter, Navigate, Route, Routes } from 'react-router-dom';

import { Layout } from '@/components/Layout';
import { CapacityLabPage } from '@/pages/CapacityLabPage';
import { CastlePage } from '@/pages/CastlePage';
import { DashboardPage } from '@/pages/DashboardPage';
import { FlashcardsPage } from '@/pages/FlashcardsPage';
import { LeaderboardPage } from '@/pages/LeaderboardPage';
import { ProgressPage } from '@/pages/ProgressPage';
import { QuizPage } from '@/pages/QuizPage';
import { SpotErrorPage } from '@/pages/SpotErrorPage';

// The ecosystem explorer pulls in three.js and the full component tree. Neither
// belongs in the bundle every other page pays for.
const ExplorerPage = lazy(() =>
  import('@/pages/ExplorerPage').then((m) => ({ default: m.ExplorerPage }))
);

function PageFallback() {
  return (
    <p className="py-20 text-center text-sm text-slate-400">Loading…</p>
  );
}

/**
 * Hash routing on purpose: the app is served as static content, so a deep link
 * such as /quiz would 404 unless the host rewrites unknown paths to index.html.
 * #/quiz always resolves.
 */
function App() {
  return (
    <HashRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<DashboardPage />} />
          <Route
            path="explore"
            element={
              <Suspense fallback={<PageFallback />}>
                <ExplorerPage />
              </Suspense>
            }
          />
          <Route path="flashcards" element={<FlashcardsPage />} />
          <Route path="quiz" element={<QuizPage />} />
          <Route path="spot" element={<SpotErrorPage />} />
          <Route path="castle" element={<CastlePage />} />
          <Route path="capacity" element={<CapacityLabPage />} />
          <Route path="leaderboard" element={<LeaderboardPage />} />
          <Route path="progress" element={<ProgressPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </HashRouter>
  );
}

export default App;
