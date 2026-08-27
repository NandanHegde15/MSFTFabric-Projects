import { useMemo, useState } from 'react';

import { Bar, Chip, PageHeader } from '@/components/ui';

const SKUS = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

/** Power BI Premium P-SKU equivalence, for the SKUs where one exists. */
const P_EQUIVALENT: Record<number, string> = {
  64: 'P1',
  128: 'P2',
  256: 'P3',
  512: 'P4',
  1024: 'P5',
};

const SECONDS_PER_DAY = 86_400;

interface Workload {
  id: number;
  name: string;
  runsPerDay: number;
  cuSecondsPerRun: number;
}

const DEFAULT_WORKLOADS: Workload[] = [
  { id: 1, name: 'Nightly Spark ingest', runsPerDay: 1, cuSecondsPerRun: 480_000 },
  { id: 2, name: 'Hourly warehouse load', runsPerDay: 24, cuSecondsPerRun: 9_000 },
  { id: 3, name: 'Semantic model refresh', runsPerDay: 4, cuSecondsPerRun: 25_000 },
  { id: 4, name: 'Interactive report queries', runsPerDay: 3_000, cuSecondsPerRun: 90 },
];

function throttleStage(minutes: number): {
  label: string;
  detail: string;
  tone: 'green' | 'amber' | 'red';
} {
  if (minutes < 10) {
    return {
      label: 'Overage protection',
      detail:
        'Under 10 minutes of future smoothed consumption. Everything still runs; the overage is simply carried forward.',
      tone: 'green',
    };
  }
  if (minutes < 60) {
    return {
      label: 'Interactive delay',
      detail:
        'Between 10 and 60 minutes. Interactive requests are delayed by roughly 20 seconds. Background jobs are untouched.',
      tone: 'amber',
    };
  }
  if (minutes < 24 * 60) {
    return {
      label: 'Interactive rejection',
      detail:
        'Between 60 minutes and 24 hours. Interactive requests are rejected outright — reports break. Background jobs still run.',
      tone: 'red',
    };
  }
  return {
    label: 'Background rejection',
    detail:
      'Beyond 24 hours of carry-forward. Everything is rejected, background jobs included. The capacity is effectively down.',
    tone: 'red',
  };
}

export function CapacityLabPage() {
  const [sku, setSku] = useState(64);
  const [workloads, setWorkloads] = useState<Workload[]>(DEFAULT_WORKLOADS);
  const [carryForward, setCarryForward] = useState(35);
  const [nextId, setNextId] = useState(5);

  const perDay = sku * SECONDS_PER_DAY;
  const per30s = sku * 30;

  const consumed = useMemo(
    () =>
      workloads.reduce(
        (t, w) => t + Math.max(0, w.runsPerDay) * Math.max(0, w.cuSecondsPerRun),
        0
      ),
    [workloads]
  );

  const utilisation = perDay > 0 ? (consumed / perDay) * 100 : 0;
  const smallestFit = SKUS.find((s) => consumed <= s * SECONDS_PER_DAY);
  const stage = throttleStage(carryForward);

  const patch = (id: number, changes: Partial<Workload>) =>
    setWorkloads((ws) => ws.map((w) => (w.id === id ? { ...w, ...changes } : w)));

  return (
    <>
      <PageHeader
        title="Capacity lab"
        lead="The CU arithmetic both exams keep coming back to. Change the numbers and watch what happens — the relationships are what you need to hold in your head, not the specific figures."
      />

      <section className="grid gap-4 lg:grid-cols-3">
        <div className="card p-5 lg:col-span-2">
          <h2 className="text-sm font-semibold text-slate-900">
            1. Pick a capacity
          </h2>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {SKUS.map((s) => (
              <button
                key={s}
                onClick={() => setSku(s)}
                className={`rounded-lg border px-3 py-1.5 text-sm font-medium tabular-nums transition-colors ${
                  s === sku
                    ? 'accent-border accent-soft-bg accent-text'
                    : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                }`}
              >
                F{s}
              </button>
            ))}
          </div>

          <dl className="mt-5 grid gap-4 sm:grid-cols-3">
            <div>
              <dt className="text-xs text-slate-500">Capacity units</dt>
              <dd className="text-lg font-semibold tabular-nums text-slate-900">
                {sku} CU
              </dd>
            </div>
            <div>
              <dt className="text-xs text-slate-500">CU-seconds per 30s window</dt>
              <dd className="text-lg font-semibold tabular-nums text-slate-900">
                {per30s.toLocaleString()}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-slate-500">CU-seconds per day</dt>
              <dd className="text-lg font-semibold tabular-nums text-slate-900">
                {perDay.toLocaleString()}
              </dd>
            </div>
          </dl>

          <div className="mt-5 flex flex-wrap gap-2">
            {P_EQUIVALENT[sku] && (
              <Chip tone="accent">
                Power BI equivalent: {P_EQUIVALENT[sku]}
              </Chip>
            )}
            <Chip tone={sku >= 64 ? 'green' : 'amber'}>
              {sku >= 64
                ? 'Free-licence users can view content'
                : 'Every viewer needs a Pro or PPU licence'}
            </Chip>
          </div>
        </div>

        <div className="card p-5">
          <h2 className="text-sm font-semibold text-slate-900">The formula</h2>
          <p className="mt-3 text-sm leading-relaxed text-slate-600">
            An F SKU delivers its number in capacity units every second. So an
            F{sku} supplies:
          </p>
          <p className="code-line mt-3 rounded-lg bg-slate-50 p-3 text-slate-800">
            {sku} CU x 86,400 s{'\n'}= {perDay.toLocaleString()} CU-seconds/day
          </p>
          <p className="mt-3 text-xs leading-relaxed text-slate-500">
            Every utilisation percentage in the Capacity Metrics app is
            CU-seconds consumed over CU-seconds available in the window. It is
            not a CPU percentage.
          </p>
        </div>
      </section>

      <section className="card mt-6 p-5">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-900">
            2. Add up a day of work
          </h2>
          <span className="text-xs text-slate-500">
            Figures are illustrative — read your own from the Capacity Metrics app.
          </span>
        </div>

        <div className="mt-4 overflow-x-auto">
          <table className="w-full min-w-[38rem] text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
                <th className="pb-2 font-medium">Workload</th>
                <th className="pb-2 text-right font-medium">Runs / day</th>
                <th className="pb-2 text-right font-medium">CU-seconds / run</th>
                <th className="pb-2 text-right font-medium">CU-seconds / day</th>
                <th className="pb-2" />
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {workloads.map((w) => (
                <tr key={w.id}>
                  <td className="py-2 pr-3">
                    <input
                      value={w.name}
                      onChange={(e) => patch(w.id, { name: e.target.value })}
                      className="w-full rounded-lg border border-slate-200 px-2.5 py-1.5 text-sm"
                      aria-label="Workload name"
                    />
                  </td>
                  <td className="py-2 pr-3">
                    <input
                      type="number"
                      min={0}
                      value={w.runsPerDay}
                      onChange={(e) =>
                        patch(w.id, { runsPerDay: Number(e.target.value) })
                      }
                      className="w-24 rounded-lg border border-slate-200 px-2.5 py-1.5 text-right text-sm tabular-nums"
                      aria-label="Runs per day"
                    />
                  </td>
                  <td className="py-2 pr-3">
                    <input
                      type="number"
                      min={0}
                      value={w.cuSecondsPerRun}
                      onChange={(e) =>
                        patch(w.id, { cuSecondsPerRun: Number(e.target.value) })
                      }
                      className="w-32 rounded-lg border border-slate-200 px-2.5 py-1.5 text-right text-sm tabular-nums"
                      aria-label="CU-seconds per run"
                    />
                  </td>
                  <td className="py-2 text-right tabular-nums text-slate-700">
                    {(w.runsPerDay * w.cuSecondsPerRun).toLocaleString()}
                  </td>
                  <td className="py-2 pl-2 text-right">
                    <button
                      onClick={() =>
                        setWorkloads((ws) => ws.filter((x) => x.id !== w.id))
                      }
                      className="text-slate-300 transition-colors hover:text-rose-500"
                      aria-label={`Remove ${w.name}`}
                    >
                      ×
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <button
          className="btn btn-ghost mt-3"
          onClick={() => {
            setWorkloads((ws) => [
              ...ws,
              { id: nextId, name: 'New workload', runsPerDay: 1, cuSecondsPerRun: 1000 },
            ]);
            setNextId((n) => n + 1);
          }}
        >
          Add a workload
        </button>

        <div className="mt-6 grid gap-5 sm:grid-cols-2">
          <div>
            <Bar
              value={Math.min(100, Math.round(utilisation))}
              label={`Daily utilisation of F${sku}`}
            />
            <p className="mt-2 text-sm tabular-nums text-slate-600">
              {consumed.toLocaleString()} of {perDay.toLocaleString()} CU-seconds
              {' — '}
              <span
                className={
                  utilisation > 100
                    ? 'font-semibold text-rose-600'
                    : utilisation > 80
                      ? 'font-semibold text-amber-600'
                      : 'font-semibold text-emerald-600'
                }
              >
                {utilisation.toFixed(1)}%
              </span>
            </p>
          </div>

          <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
              Smallest SKU that fits this day
            </p>
            <p className="mt-1 text-2xl font-semibold text-slate-900">
              {smallestFit ? `F${smallestFit}` : 'Beyond F2048'}
            </p>
            <p className="mt-2 text-xs leading-relaxed text-slate-500">
              Fitting on a daily average is necessary but not sufficient —
              smoothing means a burst can still push you into throttling, and
              dropping below F64 costs you free-licence viewing.
            </p>
          </div>
        </div>
      </section>

      <section className="card mt-6 p-5">
        <h2 className="text-sm font-semibold text-slate-900">
          3. Smoothing and throttling
        </h2>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-600">
          Fabric spreads recorded consumption over time: background operations
          over 24 hours, interactive operations over a minimum of five minutes.
          What decides throttling is how much future smoothed consumption you
          have already committed — the carry-forward.
        </p>

        <label className="mt-5 block text-sm text-slate-600">
          Carry-forward: <span className="font-semibold tabular-nums">{carryForward}</span>{' '}
          minute{carryForward === 1 ? '' : 's'}
          <input
            type="range"
            min={0}
            max={1600}
            value={carryForward}
            onChange={(e) => setCarryForward(Number(e.target.value))}
            className="mt-2 w-full max-w-xl"
          />
        </label>

        <div
          className={`mt-4 rounded-xl border p-4 ${
            stage.tone === 'green'
              ? 'border-emerald-200 bg-emerald-50'
              : stage.tone === 'amber'
                ? 'border-amber-200 bg-amber-50'
                : 'border-rose-200 bg-rose-50'
          }`}
        >
          <p className="text-sm font-semibold text-slate-900">{stage.label}</p>
          <p className="mt-1.5 text-sm leading-relaxed text-slate-700">
            {stage.detail}
          </p>
        </div>

        <ol className="mt-5 space-y-2 text-sm text-slate-600">
          <li>
            <span className="font-medium text-slate-800">Under 10 minutes</span> —
            overage protection, nothing throttled.
          </li>
          <li>
            <span className="font-medium text-slate-800">10 to 60 minutes</span> —
            interactive delay of about 20 seconds.
          </li>
          <li>
            <span className="font-medium text-slate-800">60 minutes to 24 hours</span>{' '}
            — interactive requests rejected.
          </li>
          <li>
            <span className="font-medium text-slate-800">Over 24 hours</span> —
            background jobs rejected too.
          </li>
        </ol>
      </section>

      <section className="card mt-6 overflow-x-auto p-5">
        <h2 className="text-sm font-semibold text-slate-900">SKU reference</h2>
        <table className="mt-3 w-full min-w-[34rem] text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
              <th className="pb-2 font-medium">SKU</th>
              <th className="pb-2 text-right font-medium">CU</th>
              <th className="pb-2 text-right font-medium">CU-seconds / day</th>
              <th className="pb-2 font-medium">Power BI equivalent</th>
              <th className="pb-2 font-medium">Free viewers</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {SKUS.map((s) => (
              <tr key={s} className={s === sku ? 'accent-soft-bg' : ''}>
                <td className="py-1.5 font-medium text-slate-800">F{s}</td>
                <td className="py-1.5 text-right tabular-nums text-slate-600">{s}</td>
                <td className="py-1.5 text-right tabular-nums text-slate-600">
                  {(s * SECONDS_PER_DAY).toLocaleString()}
                </td>
                <td className="py-1.5 text-slate-600">{P_EQUIVALENT[s] ?? '—'}</td>
                <td className="py-1.5 text-slate-600">{s >= 64 ? 'Yes' : 'No'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-3 text-xs leading-relaxed text-slate-500">
          The Fabric trial gives a 60-day capacity with the compute of an F64.
          Pausing a capacity stops compute billing but not OneLake storage
          billing, and settles outstanding smoothed consumption immediately.
        </p>
      </section>
    </>
  );
}
