import { useState, useRef, useEffect, useMemo, useCallback } from 'react';

/* ─── helpers ─── */
const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const DAY_NAMES = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];

/** Return YYYY-MM-DD string from a Date (UTC). */
const fmt = (d) => d.toISOString().slice(0, 10);

/** Return a Date from a YYYY-MM-DD string. */
const parse = (s) => s ? new Date(s + 'T00:00:00Z') : null;

/** Days in a given month (1-based). */
const daysInMonth = (year, month) => new Date(year, month + 1, 0).getDate();

/** Day-of-week for the 1st of the month (0=Mon … 6=Sun, ISO). */
const startDow = (year, month) => {
  const d = new Date(Date.UTC(year, month, 1)).getUTCDay();
  return d === 0 ? 6 : d - 1; // shift Sunday from 0 to 6
};

/** Quick presets: returns [label, startDate, endDate] relative to `maxDate`. */
const makePresets = (minDateStr, maxDateStr) => {
  const max = parse(maxDateStr);
  const min = parse(minDateStr);
  if (!max || !min) return [];
  const presets = [];
  const sub = (months) => {
    const d = new Date(max);
    d.setUTCMonth(d.getUTCMonth() - months);
    return d < min ? min : d;
  };
  presets.push(['3M', sub(3), max]);
  presets.push(['6M', sub(6), max]);
  presets.push(['1Y', sub(12), max]);
  presets.push(['2Y', sub(24), max]);
  presets.push(['3Y', sub(36), max]);
  presets.push(['5Y', sub(60), max]);
  presets.push(['All', min, max]);
  return presets.filter(([, s]) => s >= min);
};


/* ─── MonthGrid: single calendar month ─── */
const MonthGrid = ({ year, month, rangeStart, rangeEnd, hoverDate, minDate, maxDate, onDayClick, onDayHover }) => {
  const dim = daysInMonth(year, month);
  const offset = startDow(year, month);
  const cells = [];

  for (let i = 0; i < offset; i++) cells.push(null); // leading blanks
  for (let d = 1; d <= dim; d++) cells.push(d);

  const isDisabled = (day) => {
    const s = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    return (minDate && s < minDate) || (maxDate && s > maxDate);
  };

  const dateStr = (day) => `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;

  // effective visual range: if hovering, show preview of what would be selected
  const effStart = rangeStart;
  const effEnd = rangeEnd || hoverDate;

  const isInRange = (day) => {
    if (!effStart || !effEnd) return false;
    const s = dateStr(day);
    const lo = effStart <= effEnd ? effStart : effEnd;
    const hi = effStart <= effEnd ? effEnd : effStart;
    return s >= lo && s <= hi;
  };

  const isRangeEdge = (day) => {
    const s = dateStr(day);
    return s === effStart || s === effEnd;
  };

  return (
    <div className="select-none">
      <div className="grid grid-cols-7 gap-0 mb-1">
        {DAY_NAMES.map(d => (
          <div key={d} className="text-center text-[10px] font-medium text-gray-400 dark:text-gray-500 py-0.5">{d}</div>
        ))}
      </div>
      <div className="grid grid-cols-7 gap-0">
        {cells.map((day, i) => {
          if (day === null) return <div key={`blank-${i}`} className="h-7" />;
          const disabled = isDisabled(day);
          const ds = dateStr(day);
          const inRange = isInRange(day);
          const isEdge = isRangeEdge(day);
          const isToday = ds === fmt(new Date());

          return (
            <button
              key={day}
              disabled={disabled}
              onClick={() => !disabled && onDayClick(ds)}
              onMouseEnter={() => !disabled && onDayHover(ds)}
              className={`h-7 text-xs font-medium rounded-sm transition-colors
                ${disabled ? 'text-gray-300 dark:text-gray-600 cursor-not-allowed' : 'cursor-pointer hover:bg-blue-100 dark:hover:bg-blue-900/40'}
                ${inRange && !isEdge ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300' : ''}
                ${isEdge ? 'bg-blue-500 text-white font-semibold' : ''}
                ${!inRange && !isEdge && !disabled ? 'text-gray-700 dark:text-gray-300' : ''}
                ${isToday && !isEdge ? 'ring-1 ring-blue-400' : ''}
              `}
            >
              {day}
            </button>
          );
        })}
      </div>
    </div>
  );
};


/* ─── Main DateRangePicker ─── */

const DateRangePicker = ({ startDate, endDate, minDate, maxDate, onChange }) => {
  const [open, setOpen] = useState(false);
  const [picking, setPicking] = useState(null); // null | 'start' — tracks click stage
  const [tempStart, setTempStart] = useState(startDate || minDate);
  const [tempEnd, setTempEnd] = useState(endDate || maxDate);
  const [hoverDate, setHoverDate] = useState(null);
  const ref = useRef(null);

  // Calendar navigation: left/right month
  const initRight = useMemo(() => {
    const d = parse(endDate || maxDate);
    return d ? { year: d.getUTCFullYear(), month: d.getUTCMonth() } : { year: 2026, month: 0 };
  }, [endDate, maxDate]);

  const [rightYear, setRightYear] = useState(initRight.year);
  const [rightMonth, setRightMonth] = useState(initRight.month);

  // Left calendar is always the month before right
  const leftYear = rightMonth === 0 ? rightYear - 1 : rightYear;
  const leftMonth = rightMonth === 0 ? 11 : rightMonth - 1;

  // Sync when panel opens
  useEffect(() => {
    if (open) {
      setTempStart(startDate || minDate);
      setTempEnd(endDate || maxDate);
      setPicking(null);
      setHoverDate(null);
      const d = parse(endDate || maxDate);
      if (d) { setRightYear(d.getUTCFullYear()); setRightMonth(d.getUTCMonth()); }
    }
  }, [open, startDate, endDate, minDate, maxDate]);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    const escHandler = (e) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', handler);
    document.addEventListener('keydown', escHandler);
    return () => { document.removeEventListener('mousedown', handler); document.removeEventListener('keydown', escHandler); };
  }, [open]);

  const navigateMonth = useCallback((delta) => {
    setRightMonth(prev => {
      let m = prev + delta;
      let y = rightYear;
      if (m > 11) { m = 0; y++; }
      if (m < 0) { m = 11; y--; }
      setRightYear(y);
      return m;
    });
  }, [rightYear]);

  const handleDayClick = useCallback((ds) => {
    if (!picking) {
      // First click: set start, wait for end
      setTempStart(ds);
      setTempEnd(null);
      setPicking('start');
    } else {
      // Second click: set end and apply
      let s = tempStart, e = ds;
      if (s > e) [s, e] = [e, s];
      setTempStart(s);
      setTempEnd(e);
      setPicking(null);
      onChange(s, e);
      setOpen(false);
    }
  }, [picking, tempStart, onChange]);

  const handlePreset = useCallback(([, s, e]) => {
    const sf = fmt(s), ef = fmt(e);
    setTempStart(sf);
    setTempEnd(ef);
    setPicking(null);
    onChange(sf, ef);
    setOpen(false);
  }, [onChange]);

  const presets = useMemo(() => makePresets(minDate, maxDate), [minDate, maxDate]);

  // Display text
  const displayText = useMemo(() => {
    if (!startDate && !endDate) return 'All dates';
    const s = startDate || minDate;
    const e = endDate || maxDate;
    if (s === minDate && e === maxDate) return 'All dates';
    // Format as "Jan 2023 – Dec 2025"
    const sd = parse(s), ed = parse(e);
    if (!sd || !ed) return 'All dates';
    return `${MONTH_NAMES[sd.getUTCMonth()]} ${sd.getUTCFullYear()} – ${MONTH_NAMES[ed.getUTCMonth()]} ${ed.getUTCFullYear()}`;
  }, [startDate, endDate, minDate, maxDate]);

  return (
    <div className="relative" ref={ref}>
      {/* Trigger button */}
      <button
        onClick={() => setOpen(o => !o)}
        className={`flex items-center gap-1.5 px-3 py-2 text-sm border rounded-lg transition-colors
          ${open
            ? 'border-blue-400 dark:border-blue-500 ring-2 ring-blue-200 dark:ring-blue-800 bg-white dark:bg-gray-700'
            : 'border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 hover:border-gray-300 dark:hover:border-gray-500'}
          text-gray-800 dark:text-gray-100`}
      >
        <svg className="w-4 h-4 text-gray-400 dark:text-gray-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <span className="whitespace-nowrap">{displayText}</span>
        <svg className={`w-3 h-3 text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Popup */}
      {open && (
        <div className="absolute top-full mt-1 right-0 z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-xl shadow-xl p-4 min-w-[540px]">
          {/* Picking hint */}
          <div className="text-xs text-gray-400 dark:text-gray-500 mb-3 text-center">
            {picking ? 'Click an end date' : 'Click a start date'}
          </div>

          <div className="flex gap-4">
            {/* Presets sidebar */}
            <div className="flex flex-col gap-1 pr-3 border-r border-gray-100 dark:border-gray-700 min-w-[4rem]">
              {presets.map(([label, s, e]) => {
                const sf = fmt(s), ef = fmt(e);
                const isActive = tempStart === sf && tempEnd === ef;
                return (
                  <button
                    key={label}
                    onClick={() => handlePreset([label, s, e])}
                    className={`px-2 py-1.5 text-xs font-medium rounded-lg transition-colors whitespace-nowrap
                      ${isActive
                        ? 'bg-blue-500 text-white'
                        : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700'}`}
                  >
                    {label}
                  </button>
                );
              })}
            </div>

            {/* Dual calendars */}
            <div className="flex gap-4">
              {/* Left calendar */}
              <div className="w-[13rem]">
                <div className="flex items-center justify-between mb-2">
                  <button onClick={() => navigateMonth(-1)}
                    className="w-6 h-6 flex items-center justify-center rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400">
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                    </svg>
                  </button>
                  <span className="text-sm font-semibold text-gray-700 dark:text-gray-200">
                    {MONTH_NAMES[leftMonth]} {leftYear}
                  </span>
                  <div className="w-6" /> {/* spacer */}
                </div>
                <MonthGrid
                  year={leftYear} month={leftMonth}
                  rangeStart={tempStart} rangeEnd={tempEnd}
                  hoverDate={picking ? hoverDate : null}
                  minDate={minDate} maxDate={maxDate}
                  onDayClick={handleDayClick}
                  onDayHover={setHoverDate}
                />
              </div>

              {/* Right calendar */}
              <div className="w-[13rem]">
                <div className="flex items-center justify-between mb-2">
                  <div className="w-6" /> {/* spacer */}
                  <span className="text-sm font-semibold text-gray-700 dark:text-gray-200">
                    {MONTH_NAMES[rightMonth]} {rightYear}
                  </span>
                  <button onClick={() => navigateMonth(1)}
                    className="w-6 h-6 flex items-center justify-center rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400">
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                  </button>
                </div>
                <MonthGrid
                  year={rightYear} month={rightMonth}
                  rangeStart={tempStart} rangeEnd={tempEnd}
                  hoverDate={picking ? hoverDate : null}
                  minDate={minDate} maxDate={maxDate}
                  onDayClick={handleDayClick}
                  onDayHover={setHoverDate}
                />
              </div>
            </div>
          </div>

          {/* Footer: selected range */}
          <div className="mt-3 pt-3 border-t border-gray-100 dark:border-gray-700 flex items-center justify-between">
            <div className="text-xs text-gray-500 dark:text-gray-400">
              {tempStart && <span className="font-mono font-medium text-gray-700 dark:text-gray-300">{tempStart}</span>}
              {tempStart && tempEnd && <span className="mx-1.5">→</span>}
              {tempEnd && <span className="font-mono font-medium text-gray-700 dark:text-gray-300">{tempEnd}</span>}
              {!tempStart && !tempEnd && 'No range selected'}
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => { onChange(null, null); setOpen(false); }}
                className="px-2.5 py-1 text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
              >
                Clear
              </button>
              {tempStart && tempEnd && (
                <button
                  onClick={() => { onChange(tempStart, tempEnd); setOpen(false); }}
                  className="px-3 py-1 text-xs font-medium bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
                >
                  Apply
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DateRangePicker;
