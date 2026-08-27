import { createRoot } from 'react-dom/client';

import App from '@/App';
import { bootstrapAuth } from '@/services/bootstrap';

import './main.css';

// FabricQuiz is public: nothing here is gated behind sign-in. bootstrapAuth is
// called only for its side effect of initialising the Rayfin client, which the
// optional community board uses. If configuration is missing we carry on — the
// study app is fully functional without a backend.
try {
  bootstrapAuth();
} catch (err) {
  console.warn('Rayfin backend not configured; community board disabled.', err);
}

createRoot(document.getElementById('root')!).render(<App />);
