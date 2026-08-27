/**
 * The boss. `state` drives its expression so a hit reads instantly:
 * 'idle' waiting, 'hit' recoiling, 'slain' toppled.
 */
export function Dragon({
  hp,
  maxHp,
  state,
}: {
  hp: number;
  maxHp: number;
  state: 'idle' | 'hit' | 'slain';
}) {
  const wounded = hp <= maxHp / 2;

  return (
    <div className="flex flex-col items-center">
      <svg
        viewBox="0 0 220 170"
        className={`h-40 w-auto ${state === 'hit' ? 'dragon-hit' : ''} ${
          state === 'slain' ? 'dragon-slain' : 'dragon-breathe'
        }`}
        role="img"
        aria-label={
          state === 'slain'
            ? 'The dragon has fallen'
            : `Dragon, ${hp} of ${maxHp} strikes remaining`
        }
      >
        {/* Wings */}
        <path
          d="M108 74 L44 30 Q30 46 40 72 Q52 96 96 96 Z"
          fill={wounded ? '#9f1239' : '#be123c'}
          opacity="0.85"
        />
        <path
          d="M124 74 L188 30 Q202 46 192 72 Q180 96 136 96 Z"
          fill={wounded ? '#9f1239' : '#be123c'}
          opacity="0.85"
        />

        {/* Tail */}
        <path
          d="M104 128 Q66 138 52 158 Q78 150 100 146"
          fill="none"
          stroke="#7f1d1d"
          strokeWidth="10"
          strokeLinecap="round"
        />

        {/* Body */}
        <ellipse cx="116" cy="112" rx="34" ry="30" fill="#991b1b" />
        <ellipse cx="116" cy="118" rx="20" ry="19" fill="#f59e0b" opacity="0.8" />

        {/* Neck and head */}
        <path d="M108 92 Q104 68 118 56" stroke="#991b1b" strokeWidth="20" fill="none" strokeLinecap="round" />
        <ellipse cx="126" cy="50" rx="27" ry="21" fill="#b91c1c" />
        <path d="M150 48 q14 3 20 10 q-14 2 -21 -1 z" fill="#b91c1c" />

        {/* Horns */}
        <path d="M112 33 l-7 -17 l 14 10 z" fill="#fbbf24" />
        <path d="M132 31 l 2 -18 l 10 15 z" fill="#fbbf24" />

        {/* Eye */}
        {state === 'slain' ? (
          <>
            <path d="M126 45 l 9 9 M135 45 l -9 9" stroke="#0f172a" strokeWidth="2.6" strokeLinecap="round" />
          </>
        ) : (
          <>
            <ellipse cx="132" cy="47" rx="6" ry="7" fill="#fde68a" />
            <ellipse cx="133" cy="47" rx="2.2" ry="6" fill="#0f172a" />
          </>
        )}

        {/* Fire, only while it still has fight in it */}
        {state !== 'slain' && (
          <g className="dragon-flame" opacity="0.9">
            <path d="M172 56 q22 -6 40 4 q-20 10 -40 4 z" fill="#f97316" />
            <path d="M176 57 q16 -3 28 2 q-14 6 -28 1 z" fill="#fbbf24" />
          </g>
        )}
      </svg>

      <div className="mt-3 w-52">
        <div className="mb-1 flex justify-between text-[11px] font-medium text-slate-500">
          <span>Dragon</span>
          <span className="tabular-nums">
            {hp}/{maxHp}
          </span>
        </div>
        <div className="h-2.5 overflow-hidden rounded-full bg-slate-200">
          <div
            className="h-full rounded-full bg-rose-600 transition-[width] duration-500"
            style={{ width: `${(hp / maxHp) * 100}%` }}
          />
        </div>
      </div>
    </div>
  );
}

export function Hearts({ hearts, max }: { hearts: number; max: number }) {
  return (
    <span
      className="flex items-center gap-1"
      role="img"
      aria-label={`${hearts} of ${max} hearts remaining`}
    >
      {Array.from({ length: max }, (_, i) => (
        <svg
          key={i}
          viewBox="0 0 24 24"
          className={`h-5 w-5 ${i < hearts ? 'text-rose-500' : 'text-slate-200'}`}
          fill="currentColor"
          aria-hidden
        >
          <path d="M12 21s-7.5-4.7-9.3-9A5.2 5.2 0 0 1 12 6.5 5.2 5.2 0 0 1 21.3 12c-1.8 4.3-9.3 9-9.3 9Z" />
        </svg>
      ))}
    </span>
  );
}
