import { STAGES } from '@/data/castle';

/**
 * The run's progress bar, drawn as a path through the keep.
 *
 * Six waypoints on a rising path: cleared ones fill with the accent colour,
 * the current one pulses, the rest stay outlined.
 */
export function CastleMap({ stageIndex }: { stageIndex: number }) {
  const width = 720;
  const height = 130;
  const step = width / (STAGES.length + 0.6);

  const points = STAGES.map((_, i) => ({
    x: step * (i + 0.8),
    // Rise towards the tower, then dip into the lair.
    y: 92 - i * 9 + (i === STAGES.length - 1 ? 14 : 0),
  }));

  const path = points
    .map((p, i) => (i === 0 ? `M ${p.x} ${p.y}` : `L ${p.x} ${p.y}`))
    .join(' ');

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="h-auto w-full"
      role="img"
      aria-label={`Castle progress: stage ${Math.min(stageIndex + 1, STAGES.length)} of ${STAGES.length}, ${STAGES[Math.min(stageIndex, STAGES.length - 1)]?.name}`}
    >
      <defs>
        <linearGradient id="sky" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="var(--accent-soft)" />
          <stop offset="100%" stopColor="#ffffff" />
        </linearGradient>
      </defs>

      <rect x="0" y="0" width={width} height={height} fill="url(#sky)" rx="12" />

      {/* Silhouette of the keep behind the path */}
      <g fill="var(--accent-border)" opacity="0.5">
        <rect x={width - 190} y="18" width="26" height="86" />
        <rect x={width - 158} y="34" width="70" height="70" />
        <rect x={width - 82} y="10" width="30" height="94" />
        <path d={`M ${width - 190} 18 l 6 -10 l 7 10 z`} />
        <path d={`M ${width - 82} 10 l 7 -11 l 8 11 z`} />
      </g>

      <path
        d={path}
        fill="none"
        stroke="var(--color-slate-300, #cbd5e1)"
        strokeWidth="3"
        strokeDasharray="6 7"
        strokeLinecap="round"
      />

      {points.map((p, i) => {
        const cleared = i < stageIndex;
        const current = i === stageIndex;
        const stage = STAGES[i];

        return (
          <g key={stage.id}>
            {current && (
              <circle
                cx={p.x}
                cy={p.y}
                r="17"
                fill="var(--accent)"
                opacity="0.16"
                className="castle-pulse"
              />
            )}
            <circle
              cx={p.x}
              cy={p.y}
              r="11"
              fill={cleared || current ? 'var(--accent)' : '#ffffff'}
              stroke={cleared || current ? 'var(--accent)' : 'var(--color-slate-300, #cbd5e1)'}
              strokeWidth="2"
            />
            {cleared && (
              <path
                d={`M ${p.x - 4.5} ${p.y} l 3 3.5 l 6 -7`}
                fill="none"
                stroke="#fff"
                strokeWidth="2.2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}
            {stage.boss && !cleared && (
              <text
                x={p.x}
                y={p.y + 4.5}
                textAnchor="middle"
                fontSize="12"
                aria-hidden
              >
                🐉
              </text>
            )}
            <text
              x={p.x}
              y={p.y + 27}
              textAnchor="middle"
              fontSize="10.5"
              fontWeight={current ? 600 : 400}
              fill={
                current
                  ? 'var(--accent)'
                  : cleared
                    ? 'var(--color-slate-500, #64748b)'
                    : 'var(--color-slate-400, #94a3b8)'
              }
            >
              {stage.name.replace(/^The /, '')}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
