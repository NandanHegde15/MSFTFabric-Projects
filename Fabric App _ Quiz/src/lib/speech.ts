import { useSyncExternalStore } from 'react';

/**
 * A thin, single-speaker wrapper over the Web Speech API.
 *
 * Only one utterance plays at a time across the whole app, so every Listen
 * button shares this module-level state and can render its own play/stop.
 */

let currentId: string | null = null;
let keepAlive: ReturnType<typeof setInterval> | null = null;
const listeners = new Set<() => void>();

function emit() {
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function speechSupported(): boolean {
  return (
    typeof window !== 'undefined' &&
    'speechSynthesis' in window &&
    typeof window.SpeechSynthesisUtterance === 'function'
  );
}

function pickVoice(): SpeechSynthesisVoice | null {
  // getVoices() is empty until the engine has loaded them. When that happens we
  // simply let the browser pick its default rather than blocking on
  // voiceschanged, which never fires in some embedded webviews.
  const voices = window.speechSynthesis.getVoices();
  if (voices.length === 0) return null;
  return (
    voices.find((v) => v.lang === 'en-GB' && v.localService) ??
    voices.find((v) => v.lang === 'en-GB') ??
    voices.find((v) => v.lang === 'en-US') ??
    voices.find((v) => v.lang.startsWith('en')) ??
    null
  );
}

/**
 * Chrome stops synthesising after roughly 15 seconds of continuous speech.
 * Splitting on sentence boundaries keeps each utterance short enough to finish,
 * and the queue plays them back to back.
 */
function chunk(text: string, limit = 180): string[] {
  const sentences = text.replace(/\s+/g, ' ').trim().match(/[^.!?]+[.!?]*\s*/g);
  if (!sentences) return [];

  const out: string[] = [];
  let buffer = '';
  for (const sentence of sentences) {
    if (buffer && buffer.length + sentence.length > limit) {
      out.push(buffer.trim());
      buffer = '';
    }
    // A single sentence longer than the limit is left whole — breaking mid
    // clause sounds worse than risking the cutoff.
    buffer += sentence;
  }
  if (buffer.trim()) out.push(buffer.trim());
  return out;
}

function startKeepAlive() {
  stopKeepAlive();
  // Chrome suspends a long queue; a periodic resume keeps it moving.
  keepAlive = setInterval(() => {
    const synth = window.speechSynthesis;
    if (!synth.speaking) return;
    synth.pause();
    synth.resume();
  }, 9_000);
}

function stopKeepAlive() {
  if (keepAlive !== null) {
    clearInterval(keepAlive);
    keepAlive = null;
  }
}

function finish(id: string) {
  if (currentId !== id) return;
  currentId = null;
  stopKeepAlive();
  emit();
}

export function stopSpeech() {
  if (!speechSupported()) return;
  window.speechSynthesis.cancel();
  currentId = null;
  stopKeepAlive();
  emit();
}

/** Speak `text`, or stop if `id` is already the one speaking. */
export function toggleSpeak(id: string, text: string, rate = 1) {
  if (!speechSupported()) return;

  const synth = window.speechSynthesis;
  if (currentId === id) {
    stopSpeech();
    return;
  }

  synth.cancel();
  const parts = chunk(text);
  if (parts.length === 0) return;

  const voice = pickVoice();
  currentId = id;

  parts.forEach((part, i) => {
    const utterance = new SpeechSynthesisUtterance(part);
    if (voice) utterance.voice = voice;
    utterance.rate = Math.min(2, Math.max(0.5, rate));
    utterance.pitch = 1;
    if (i === parts.length - 1) utterance.onend = () => finish(id);
    utterance.onerror = () => finish(id);
    synth.speak(utterance);
  });

  startKeepAlive();
  emit();
}

/** The id currently being spoken, or null. */
export function useSpeakingId(): string | null {
  return useSyncExternalStore(
    subscribe,
    () => currentId,
    () => null
  );
}

/** Non-reactive read, for cleanup paths that must not stop someone else's audio. */
export function getSpeakingId(): string | null {
  return currentId;
}
