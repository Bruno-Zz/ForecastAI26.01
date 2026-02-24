import { useState, useCallback } from 'react';
import { useLocale } from '../contexts/LocaleContext';
import { parseDateInput, formatDate } from '../utils/formatting';

/**
 * DateInput - A text input that accepts dates in the user's locale format.
 * Shows validation feedback and converts to ISO on change.
 *
 * Props:
 *   value       - ISO YYYY-MM-DD string (controlled)
 *   onChange     - Called with ISO string when valid date is entered
 *   className   - Additional classes
 *   placeholder - Override placeholder (defaults to locale format)
 */
export default function DateInput({ value, onChange, className = '', placeholder, ...rest }) {
  const { locale, preset } = useLocale();

  // Display the value formatted in the user's locale, or raw input while typing
  const [display, setDisplay] = useState(() =>
    value ? formatDate(value, locale) : ''
  );
  const [error, setError] = useState(false);

  const handleBlur = useCallback(() => {
    if (!display.trim()) {
      setError(false);
      onChange?.('');
      return;
    }
    const iso = parseDateInput(display, locale);
    if (iso) {
      setError(false);
      setDisplay(formatDate(iso, locale));
      onChange?.(iso);
    } else {
      setError(true);
    }
  }, [display, locale, onChange]);

  const handleChange = useCallback((e) => {
    setDisplay(e.target.value);
    setError(false);
  }, []);

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter') {
      e.target.blur();
    }
  }, []);

  return (
    <div className="relative inline-block">
      <input
        type="text"
        value={display}
        onChange={handleChange}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
        placeholder={placeholder || preset.datePlaceholder}
        className={`border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 transition-colors
          ${error
            ? 'border-red-400 focus:ring-red-400 bg-red-50 dark:bg-red-900/20 dark:border-red-500'
            : 'border-gray-300 dark:border-gray-600 focus:ring-blue-400 dark:bg-gray-700 dark:text-gray-100 bg-white'
          } ${className}`}
        {...rest}
      />
      {error && (
        <p className="text-xs text-red-500 dark:text-red-400 mt-1">
          Invalid date. Expected: {preset.dateFormat}
        </p>
      )}
    </div>
  );
}
