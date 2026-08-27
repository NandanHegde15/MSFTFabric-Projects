import { useEffect } from 'react';

import { useProgress } from '@/lib/progress';
import {
  getSpeakingId,
  speechSupported,
  stopSpeech,
  toggleSpeak,
  useSpeakingId,
} from '@/lib/speech';

/**
 * Read a block of text aloud.
 *
 * Renders nothing when the browser has no speech synthesis, so callers do not
 * need to feature-detect. `id` must be unique per speakable block — it is what
 * lets one button show "Stop" while the others stay idle.
 */
export function SpeakButton({
  id,
  text,
  label = 'Listen',
  /** Speak as soon as this becomes true, if the learner enabled autoplay. */
  autoPlayKey,
  className = '',
}: {
  id: string;
  text: string;
  label?: string;
  autoPlayKey?: string | number | null;
  className?: string;
}) {
  const { settings } = useProgress();
  const speakingId = useSpeakingId();
  const supported = speechSupported();
  const active = speakingId === id;

  useEffect(() => {
    if (!supported || !settings.autoSpeak || autoPlayKey == null) return;
    toggleSpeak(id, text, settings.speechRate);
    // Intentionally keyed on autoPlayKey alone: re-speaking whenever the text
    // identity changes would fire twice on a single reveal.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoPlayKey]);

  // Never leave audio playing after the block that owns it has gone — but only
  // stop our own utterance, never one another button started.
  useEffect(() => {
    return () => {
      if (getSpeakingId() === id) stopSpeech();
    };
  }, [id]);

  if (!supported) return null;

  return (
    <button
      type="button"
      onClick={() => toggleSpeak(id, text, settings.speechRate)}
      aria-pressed={active}
      aria-label={active ? 'Stop reading aloud' : `${label} — read this aloud`}
      title={active ? 'Stop' : 'Read aloud'}
      className={`inline-flex shrink-0 items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors ${
        active
          ? 'accent-bg border-transparent text-white'
          : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-slate-900'
      } ${className}`}
    >
      {active ? <StopIcon /> : <SpeakerIcon />}
      <span>{active ? 'Stop' : label}</span>
    </button>
  );
}

function SpeakerIcon() {
  return (
    <svg
      className="h-3.5 w-3.5"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      <path d="M11 5 6 9H3v6h3l5 4V5Z" />
      <path d="M15.5 8.5a5 5 0 0 1 0 7" />
      <path d="M18.5 5.5a9 9 0 0 1 0 13" />
    </svg>
  );
}

function StopIcon() {
  return (
    <svg
      className="h-3.5 w-3.5"
      viewBox="0 0 24 24"
      fill="currentColor"
      aria-hidden
    >
      <rect x="6" y="6" width="12" height="12" rx="2" />
    </svg>
  );
}
