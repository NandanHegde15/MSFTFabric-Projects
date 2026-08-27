import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { SpeakButton } from '@/components/SpeakButton';
import { Bar, Chip, EmptyState, PageHeader } from '@/components/ui';
import { domainsFor } from '@/data/exams';
import { FLASHCARDS, type Flashcard } from '@/data/flashcards';
import { isDue, MAX_BOX, reviewCard, useExam, useProgress } from '@/lib/progress';

type Scope = 'due' | 'all';

export function FlashcardsPage() {
  const [exam] = useExam();
  const progress = useProgress();
  const [params, setParams] = useSearchParams();

  const domainFilter = params.get('domain') ?? '';
  const [scope, setScope] = useState<Scope>('due');
  const [index, setIndex] = useState(0);
  const [flipped, setFlipped] = useState(false);
  const [reviewed, setReviewed] = useState(0);

  const domains = domainsFor(exam);

  const deck = useMemo(() => {
    const base = FLASHCARDS.filter(
      (c) => c.exam === exam && (!domainFilter || c.domainId === domainFilter)
    );
    if (scope === 'all') return base;
    const due = base.filter((c) => isDue(progress.cards[c.id]));
    return due.length > 0 ? due : base;
    // The deck is intentionally recomputed only when the filters change, not on
    // every review — otherwise grading a card would reshuffle the deck underfoot.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [exam, domainFilter, scope]);

  // Reset the session whenever the deck definition changes.
  useEffect(() => {
    setIndex(0);
    setFlipped(false);
    setReviewed(0);
  }, [exam, domainFilter, scope]);

  const card: Flashcard | undefined = deck[index];

  const grade = useCallback(
    (kept: boolean) => {
      if (!card) return;
      reviewCard(card.id, kept);
      setReviewed((r) => r + 1);
      setFlipped(false);
      setIndex((i) => i + 1);
    },
    [card]
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement) {
        return;
      }
      if (e.code === 'Space' || e.code === 'Enter') {
        e.preventDefault();
        setFlipped((f) => !f);
      } else if (flipped && (e.key === '1' || e.key === 'ArrowLeft')) {
        grade(false);
      } else if (flipped && (e.key === '2' || e.key === 'ArrowRight')) {
        grade(true);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [flipped, grade]);

  const setDomain = (value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set('domain', value);
    else next.delete('domain');
    setParams(next, { replace: true });
  };

  const box = card ? (progress.cards[card.id]?.box ?? 1) : 1;

  return (
    <>
      <PageHeader
        title="Flashcards"
        lead="Leitner-style spaced repetition. Cards you keep get shown less often; cards you miss come straight back to the front of the queue."
      />

      <div className="card mb-6 flex flex-wrap items-center gap-3 p-4">
        <label className="text-sm text-slate-600">
          Domain
          <select
            value={domainFilter}
            onChange={(e) => setDomain(e.target.value)}
            className="ml-2 rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-sm text-slate-800"
          >
            <option value="">All domains</option>
            {domains.map((d) => (
              <option key={d.id} value={d.id}>
                {d.short}
              </option>
            ))}
          </select>
        </label>

        <div className="flex rounded-xl border border-slate-200 bg-slate-100 p-0.5">
          {(['due', 'all'] as Scope[]).map((s) => (
            <button
              key={s}
              onClick={() => setScope(s)}
              className={`rounded-[10px] px-3 py-1.5 text-sm font-medium transition-colors ${
                scope === s
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {s === 'due' ? 'Due now' : 'Whole deck'}
            </button>
          ))}
        </div>

        <span className="ml-auto text-xs text-slate-500">
          {reviewed} reviewed this session
        </span>
      </div>

      {deck.length === 0 ? (
        <EmptyState
          title="Nothing to review here"
          body="There are no cards for this filter. Try another domain, or switch to the whole deck."
        />
      ) : !card ? (
        <div className="card px-6 py-14 text-center">
          <p className="text-sm font-medium text-slate-800">Deck finished.</p>
          <p className="mx-auto mt-2 max-w-md text-sm text-slate-500">
            You reviewed {reviewed} card{reviewed === 1 ? '' : 's'}. Cards you
            kept move up a box and will come back later.
          </p>
          <button
            className="btn btn-primary mx-auto mt-4"
            onClick={() => {
              setIndex(0);
              setFlipped(false);
              setReviewed(0);
            }}
          >
            Go round again
          </button>
        </div>
      ) : (
        <>
          <div className="mb-4">
            <Bar
              value={Math.round((index / deck.length) * 100)}
              label={`Card ${index + 1} of ${deck.length}`}
            />
          </div>

          <div className="flip-scene">
            <div
              className={`flip-inner min-h-[19rem] ${flipped ? 'is-flipped' : ''}`}
            >
              <button
                type="button"
                onClick={() => setFlipped(true)}
                className="flip-face card flex min-h-[19rem] w-full flex-col items-center justify-center gap-4 px-6 py-10 text-center"
                aria-hidden={flipped}
                tabIndex={flipped ? -1 : 0}
              >
                <div className="flex flex-wrap justify-center gap-2">
                  <Chip tone="accent">Box {box} of {MAX_BOX}</Chip>
                  {card.tags.slice(0, 3).map((t) => (
                    <Chip key={t}>{t}</Chip>
                  ))}
                </div>
                <h2 className="max-w-2xl text-2xl font-semibold leading-snug tracking-tight text-slate-900">
                  {card.term}
                </h2>
                <p className="text-xs text-slate-400">
                  Click, or press Space, to reveal
                </p>
              </button>

              <div
                className="flip-face flip-face-back card px-6 py-8"
                aria-hidden={!flipped}
              >
                <div className="flex items-start justify-between gap-3">
                  <h2 className="accent-text text-sm font-semibold">
                    {card.term}
                  </h2>
                  {flipped && (
                    <SpeakButton
                      id={`card-${card.id}`}
                      text={`${card.term}. ${card.definition}${card.gotcha ? ` Watch out: ${card.gotcha}` : ''}`}
                      autoPlayKey={card.id}
                    />
                  )}
                </div>
                <p className="mt-3 text-[15px] leading-relaxed text-slate-800">
                  {card.definition}
                </p>
                {card.gotcha && (
                  <p className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-relaxed text-amber-900">
                    <span className="font-semibold">Watch out: </span>
                    {card.gotcha}
                  </p>
                )}
              </div>
            </div>
          </div>

          <div className="mt-5 flex flex-wrap justify-center gap-3">
            {flipped ? (
              <>
                <button className="btn btn-ghost" onClick={() => grade(false)}>
                  Again <kbd className="text-[10px] text-slate-400">1</kbd>
                </button>
                <button className="btn btn-primary" onClick={() => grade(true)}>
                  Got it <kbd className="text-[10px] opacity-70">2</kbd>
                </button>
              </>
            ) : (
              <button className="btn btn-primary" onClick={() => setFlipped(true)}>
                Reveal answer
              </button>
            )}
            <button
              className="btn btn-ghost"
              onClick={() => {
                setFlipped(false);
                setIndex((i) => i + 1);
              }}
            >
              Skip
            </button>
          </div>
        </>
      )}
    </>
  );
}
