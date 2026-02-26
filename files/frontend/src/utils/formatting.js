/**
 * Centralized date and number formatting utilities.
 * All functions accept a locale string (BCP 47 tag) from the LocaleContext.
 */

/**
 * Format an ISO date string (YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss)
 * into the user's locale format.
 */
export function formatDate(isoDate, locale, opts = {}) {
  if (!isoDate) return '-';
  try {
    const d = new Date(isoDate.endsWith('Z') ? isoDate : isoDate + 'T00:00:00Z');
    if (isNaN(d.getTime())) return isoDate;
    const defaults = { year: 'numeric', month: '2-digit', day: '2-digit', timeZone: 'UTC' };
    return new Intl.DateTimeFormat(locale, { ...defaults, ...opts }).format(d);
  } catch {
    return isoDate;
  }
}

/**
 * Format a date as year-month only (e.g., "Feb 2026" or "2026-02").
 */
export function formatYearMonth(isoDate, locale) {
  if (!isoDate) return '-';
  try {
    const d = new Date(isoDate.endsWith('Z') ? isoDate : isoDate + 'T00:00:00Z');
    if (isNaN(d.getTime())) return isoDate;
    return new Intl.DateTimeFormat(locale, {
      year: 'numeric', month: 'short', timeZone: 'UTC'
    }).format(d);
  } catch {
    return isoDate;
  }
}

/**
 * Format a timestamp (date + time) into the user's locale.
 */
export function formatDateTime(isoDatetime, locale) {
  if (!isoDatetime) return '-';
  try {
    const s = String(isoDatetime);
    const d = new Date(s.includes('T') || s.includes('Z') ? s : s + 'T00:00:00Z');
    if (isNaN(d.getTime())) return isoDatetime;
    return new Intl.DateTimeFormat(locale, {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    }).format(d);
  } catch {
    return isoDatetime;
  }
}

/**
 * Format a time-only value from an ISO timestamp.
 */
export function formatTime(isoDatetime, locale) {
  if (!isoDatetime) return '-';
  try {
    const s = String(isoDatetime);
    const d = new Date(s.includes('T') || s.includes('Z') ? s : s + 'T00:00:00Z');
    if (isNaN(d.getTime())) return isoDatetime;
    return new Intl.DateTimeFormat(locale, {
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    }).format(d);
  } catch {
    return isoDatetime;
  }
}

/**
 * Format a number with locale-appropriate grouping and decimal separators.
 */
export function formatNumber(value, locale, maxDecimals = 1, opts = {}) {
  if (value == null || !isFinite(value)) return '-';
  try {
    return new Intl.NumberFormat(locale, {
      maximumFractionDigits: maxDecimals,
      ...opts,
    }).format(value);
  } catch {
    return String(value);
  }
}

/**
 * Format a percentage value (input already in 0-100 range).
 */
export function formatPercent(value, locale, decimals = 1) {
  if (value == null || !isFinite(value)) return '-';
  return formatNumber(value, locale, decimals) + '%';
}

/**
 * Format a ratio as percentage (input is 0-1 range, multiply by 100).
 */
export function formatRatioAsPercent(value, locale, decimals = 1) {
  if (value == null || !isFinite(value)) return '-';
  return formatNumber(value * 100, locale, decimals) + '%';
}

/**
 * Parse a user-entered date string from a locale-specific format
 * into an ISO YYYY-MM-DD string for the API.
 *
 * Supports:
 *   MM/DD/YYYY (en-US)
 *   DD/MM/YYYY (en-GB, fr-FR, pt-BR)
 *   DD.MM.YYYY (de-DE)
 *   YYYY/MM/DD (ja-JP)
 *   YYYY-MM-DD (ISO, always accepted)
 */
export function parseDateInput(input, locale) {
  if (!input || typeof input !== 'string') return null;
  const trimmed = input.trim();

  // Always accept ISO format
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const d = new Date(trimmed + 'T00:00:00Z');
    return isNaN(d.getTime()) ? null : trimmed;
  }

  let day, month, year;

  if (locale === 'ja-JP') {
    // YYYY/MM/DD
    const match = trimmed.match(/^(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})$/);
    if (!match) return null;
    [, year, month, day] = match;
  } else if (locale === 'en-US') {
    // MM/DD/YYYY
    const match = trimmed.match(/^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})$/);
    if (!match) return null;
    [, month, day, year] = match;
  } else {
    // DD/MM/YYYY or DD.MM.YYYY (en-GB, de-DE, fr-FR, pt-BR)
    const match = trimmed.match(/^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})$/);
    if (!match) return null;
    [, day, month, year] = match;
  }

  const y = parseInt(year, 10);
  const m = parseInt(month, 10);
  const d = parseInt(day, 10);

  if (m < 1 || m > 12 || d < 1 || d > 31) return null;

  const iso = `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
  const dateObj = new Date(iso + 'T00:00:00Z');
  if (isNaN(dateObj.getTime())) return null;
  // Validate that the date components match (catches Feb 30, etc.)
  if (dateObj.getUTCFullYear() !== y || dateObj.getUTCMonth() + 1 !== m || dateObj.getUTCDate() !== d) return null;

  return iso;
}

/**
 * Convert a Date object to ISO YYYY-MM-DD string (UTC).
 * Replaces the old fmtDate helper.
 */
export function toISODate(d) {
  if (!d || !(d instanceof Date) || isNaN(d.getTime())) return '';
  return d.toISOString().split('T')[0];
}
