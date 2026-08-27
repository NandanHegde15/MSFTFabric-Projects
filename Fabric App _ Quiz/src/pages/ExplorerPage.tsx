import { Suspense, lazy, useCallback, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';

import { SpeakButton } from '@/components/SpeakButton';
import { Chip, PageHeader } from '@/components/ui';
import {
  ECOSYSTEM,
  countDescendants,
  findNode,
  type EcoNode,
} from '@/data/ecosystem';
import { FLASHCARDS } from '@/data/flashcards';

const EcosystemCanvas = lazy(() => import('@/three/EcosystemCanvas'));

/** First sentence, with exactly one full stop however the source was punctuated. */
function firstSentence(text: string): string {
  const [head] = text.split('. ');
  return `${head.replace(/\.+$/, '')}.`;
}

const KIND_LABEL: Record<EcoNode['kind'], string> = {
  root: 'Platform',
  workload: 'Workload',
  item: 'Item',
  feature: 'Capability',
};

export function ExplorerPage() {
  const [centerId, setCenterId] = useState(ECOSYSTEM.id);
  const [selectedId, setSelectedId] = useState<string | null>(ECOSYSTEM.id);
  const [resetNonce, setResetNonce] = useState(0);

  const centerEntry = findNode(centerId)!;
  const selectedEntry = selectedId ? findNode(selectedId) : undefined;
  const detail = selectedEntry ?? centerEntry;

  const drill = useCallback((id: string) => {
    const entry = findNode(id);
    if (!entry) return;
    // A leaf has nothing to come apart into — just select it.
    if (!entry.node.children?.length) {
      setSelectedId(id);
      return;
    }
    setCenterId(id);
    setSelectedId(id);
  }, []);

  const goTo = useCallback((id: string) => {
    setCenterId(id);
    setSelectedId(id);
  }, []);

  const parentId = centerEntry.path.length > 1
    ? centerEntry.path[centerEntry.path.length - 2].id
    : null;

  const cards = useMemo(
    () =>
      (detail.node.cards ?? [])
        .map((id) => FLASHCARDS.find((c) => c.id === id))
        .filter((c): c is (typeof FLASHCARDS)[number] => Boolean(c)),
    [detail.node.cards]
  );

  const childCount = detail.node.children?.length ?? 0;
  const totalBelow = countDescendants(detail.node);

  const spoken = [
    detail.node.name,
    detail.node.summary,
    ...(detail.node.facts ?? []),
  ].join('. ');

  return (
    <>
      <PageHeader
        title="Fabric ecosystem"
        lead="The whole platform as one model. Click a component to inspect it, double-click to take it apart, and keep going until you reach the individual capabilities."
        actions={
          <>
            <button className="btn btn-ghost" onClick={() => setResetNonce((n) => n + 1)}>
              Reset view
            </button>
            <button
              className="btn btn-ghost"
              onClick={() => goTo(ECOSYSTEM.id)}
              disabled={centerId === ECOSYSTEM.id}
            >
              Back to top
            </button>
          </>
        }
      />

      <nav aria-label="Breadcrumb" className="mb-3 flex flex-wrap items-center gap-1 text-sm">
        {centerEntry.path.map((node, i) => (
          <span key={node.id} className="flex items-center gap-1">
            {i > 0 && <span className="text-slate-300">/</span>}
            <button
              onClick={() => goTo(node.id)}
              className={
                i === centerEntry.path.length - 1
                  ? 'accent-text font-medium'
                  : 'text-slate-500 hover:text-slate-800'
              }
            >
              {node.name}
            </button>
          </span>
        ))}
      </nav>

      <div className="grid gap-4 lg:grid-cols-[1fr_22rem]">
        <div className="card relative overflow-hidden bg-gradient-to-b from-slate-50 to-white">
          <div className="h-[26rem] w-full sm:h-[32rem]">
            <Suspense
              fallback={
                <div className="flex h-full items-center justify-center text-sm text-slate-400">
                  Loading the model…
                </div>
              }
            >
              <EcosystemCanvas
                centerId={centerId}
                selectedId={selectedId}
                onSelect={setSelectedId}
                onDrill={drill}
                resetNonce={resetNonce}
              />
            </Suspense>
          </div>

          <div className="pointer-events-none absolute bottom-0 left-0 right-0 flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-slate-200 bg-white/85 px-4 py-2 text-[11px] text-slate-500 backdrop-blur">
            <span>Drag to orbit</span>
            <span>Scroll to zoom</span>
            <span>Click to inspect</span>
            <span>Double-click to dismantle</span>
            {parentId && (
              <button
                onClick={() => goTo(parentId)}
                className="pointer-events-auto ml-auto font-medium text-slate-700 underline"
              >
                ← Back up a level
              </button>
            )}
          </div>
        </div>

        <aside className="card flex flex-col p-5">
          <div className="flex flex-wrap items-center gap-2">
            <span
              className="h-3 w-3 rounded-full"
              style={{ background: detail.color }}
              aria-hidden
            />
            <Chip>{KIND_LABEL[detail.node.kind]}</Chip>
            {detail.node.exam?.map((e) => (
              <Chip key={e} tone="accent">
                {e}
              </Chip>
            ))}
            <span className="ml-auto">
              <SpeakButton id={`eco-${detail.node.id}`} text={spoken} />
            </span>
          </div>

          <h2 className="mt-3 text-lg font-semibold leading-snug text-slate-900">
            {detail.node.name}
          </h2>
          <p className="mt-2 text-sm leading-relaxed text-slate-600">
            {detail.node.summary}
          </p>

          {detail.node.facts.length > 0 && (
            <ul className="mt-4 space-y-2">
              {detail.node.facts.map((fact) => (
                <li key={fact} className="flex gap-2 text-sm leading-relaxed text-slate-700">
                  <span className="accent-text mt-0.5 shrink-0" aria-hidden>
                    ▸
                  </span>
                  <span>{fact}</span>
                </li>
              ))}
            </ul>
          )}

          {childCount > 0 && (
            <button
              className="btn btn-primary mt-5"
              onClick={() => drill(detail.node.id)}
              disabled={centerId === detail.node.id}
            >
              {centerId === detail.node.id
                ? `Showing ${childCount} components`
                : `Take apart — ${childCount} components`}
            </button>
          )}

          {childCount > 0 && (
            <p className="mt-2 text-xs text-slate-500">
              {totalBelow} component{totalBelow === 1 ? '' : 's'} below this
              point in total.
            </p>
          )}

          {cards.length > 0 && (
            <div className="mt-5 border-t border-slate-100 pt-4">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Flashcards covering this
              </h3>
              <ul className="mt-2 space-y-1.5">
                {cards.map((card) => (
                  <li key={card.id} className="text-sm leading-snug text-slate-700">
                    {card.term}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {detail.node.domainId && (
            <div className="mt-5 flex flex-wrap gap-2 border-t border-slate-100 pt-4">
              <Link
                className="btn btn-ghost"
                to={`/flashcards?domain=${detail.node.domainId}`}
              >
                Revise
              </Link>
              <Link
                className="btn btn-ghost"
                to={`/quiz?domain=${detail.node.domainId}`}
              >
                Practise
              </Link>
            </div>
          )}
        </aside>
      </div>

      <section className="card mt-4 p-5">
        <h2 className="text-sm font-semibold text-slate-900">
          What is in this level
        </h2>
        <p className="mt-1 text-xs text-slate-500">
          The same components as the model above, if you would rather read than
          orbit.
        </p>
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {(centerEntry.node.children ?? []).map((child) => {
            const entry = findNode(child.id)!;
            const kids = child.children?.length ?? 0;
            return (
              <button
                key={child.id}
                onClick={() => setSelectedId(child.id)}
                onDoubleClick={() => drill(child.id)}
                className={`rounded-xl border p-3 text-left transition-colors ${
                  selectedId === child.id
                    ? 'accent-border accent-soft-bg'
                    : 'border-slate-200 bg-white hover:bg-slate-50'
                }`}
              >
                <span className="flex items-center gap-2">
                  <span
                    className="h-2.5 w-2.5 shrink-0 rounded-full"
                    style={{ background: entry.color }}
                    aria-hidden
                  />
                  <span className="text-sm font-medium text-slate-800">
                    {child.name}
                  </span>
                </span>
                <span className="mt-1 block text-xs leading-relaxed text-slate-500">
                  {kids > 0 ? `${kids} components · ` : ''}
                  {firstSentence(child.summary)}
                </span>
              </button>
            );
          })}
        </div>
      </section>
    </>
  );
}
