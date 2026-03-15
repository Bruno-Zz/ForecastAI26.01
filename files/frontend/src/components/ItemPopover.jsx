/**
 * ItemPopover
 *
 * Wraps any element and shows a floating card when the user hovers.
 * Uses a React portal so the card is never clipped by overflow:hidden parents.
 *
 * Props:
 *   name       — item display name
 *   imageUrl   — optional thumbnail URL
 *   stats      — optional { mean, observations, bestMethod }
 *   children   — the trigger element (usually the item name <span>)
 *   delay      — ms before appearing (default 320)
 */

import React, { useState, useRef, useCallback, useEffect } from 'react';
import { createPortal } from 'react-dom';

const CARD_W = 220;
const OFFSET = 8;   // gap between trigger and card

export default function ItemPopover({ name, imageUrl, stats, children, delay = 320 }) {
  const [visible, setVisible]   = useState(false);
  const [pos, setPos]           = useState({ top: 0, left: 0 });
  const timerRef                = useRef(null);
  const triggerRef              = useRef(null);

  const clearTimer = () => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  };

  const show = useCallback(() => {
    clearTimer();
    timerRef.current = setTimeout(() => {
      const el = triggerRef.current;
      if (!el) return;
      const rect = el.getBoundingClientRect();
      const vpW  = window.innerWidth;
      const vpH  = window.innerHeight;

      // Default: below-right of the trigger
      let top  = rect.bottom + OFFSET;
      let left = rect.left;

      // Flip left if card would overflow right edge
      if (left + CARD_W > vpW - 8) left = Math.max(8, vpW - CARD_W - 8);

      // Flip above if card would overflow bottom (estimate card height 160px)
      if (top + 160 > vpH - 8) top = rect.top - 160 - OFFSET;

      setPos({ top, left });
      setVisible(true);
    }, delay);
  }, [delay]);

  const hide = useCallback(() => {
    clearTimer();
    setVisible(false);
  }, []);

  // Clean up on unmount
  useEffect(() => () => clearTimer(), []);

  if (!name) return children;

  return (
    <>
      <span ref={triggerRef} onMouseEnter={show} onMouseLeave={hide} style={{ display: 'contents' }}>
        {children}
      </span>
      {visible && createPortal(
        <div
          onMouseEnter={show}
          onMouseLeave={hide}
          style={{
            position: 'fixed',
            top: pos.top,
            left: pos.left,
            width: CARD_W,
            zIndex: 9999,
            pointerEvents: 'auto',
          }}
          className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl shadow-xl overflow-hidden text-sm animate-in fade-in duration-150"
        >
          {/* Image */}
          {imageUrl && (
            <div className="w-full h-28 bg-gray-100 dark:bg-gray-700 overflow-hidden flex items-center justify-center">
              <img
                src={imageUrl}
                alt={name}
                className="w-full h-full object-cover"
                onError={e => { e.currentTarget.parentElement.style.display = 'none'; }}
              />
            </div>
          )}
          {!imageUrl && (
            <div className="w-full h-16 bg-gradient-to-br from-green-50 to-emerald-100 dark:from-emerald-900/30 dark:to-green-900/20 flex items-center justify-center text-4xl select-none">
              🌿
            </div>
          )}

          {/* Content */}
          <div className="px-3 py-2.5">
            <p className="font-semibold text-gray-900 dark:text-white truncate" title={name}>
              {name}
            </p>
            {stats && (
              <div className="mt-1.5 space-y-0.5 text-xs text-gray-500 dark:text-gray-400">
                {stats.observations != null && (
                  <div className="flex justify-between">
                    <span>Observations</span>
                    <span className="font-medium text-gray-700 dark:text-gray-300">{stats.observations}</span>
                  </div>
                )}
                {stats.mean != null && (
                  <div className="flex justify-between">
                    <span>Avg demand</span>
                    <span className="font-medium text-gray-700 dark:text-gray-300">
                      {typeof stats.mean === 'number' ? stats.mean.toFixed(1) : stats.mean}
                    </span>
                  </div>
                )}
                {stats.bestMethod && (
                  <div className="flex justify-between">
                    <span>Best method</span>
                    <span className="font-medium text-emerald-600 dark:text-emerald-400 truncate max-w-[110px]">
                      {stats.bestMethod}
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
