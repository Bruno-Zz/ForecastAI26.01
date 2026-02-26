import { createContext, useContext, useState, useCallback, useMemo } from 'react';

const LocaleContext = createContext(undefined);

const STORAGE_KEY = 'forecastai_locale';

// Supported locale presets
export const LOCALE_PRESETS = {
  'en-US': {
    label: 'English (US)',
    dateFormat: 'MM/DD/YYYY',
    datePlaceholder: 'MM/DD/YYYY',
    dateExample: '02/24/2026',
    numberLocale: 'en-US',
    dateLocale: 'en-US',
  },
  'en-GB': {
    label: 'English (UK)',
    dateFormat: 'DD/MM/YYYY',
    datePlaceholder: 'DD/MM/YYYY',
    dateExample: '24/02/2026',
    numberLocale: 'en-GB',
    dateLocale: 'en-GB',
  },
  'de-DE': {
    label: 'Deutsch',
    dateFormat: 'DD.MM.YYYY',
    datePlaceholder: 'TT.MM.JJJJ',
    dateExample: '24.02.2026',
    numberLocale: 'de-DE',
    dateLocale: 'de-DE',
  },
  'fr-FR': {
    label: 'Fran\u00e7ais',
    dateFormat: 'DD/MM/YYYY',
    datePlaceholder: 'JJ/MM/AAAA',
    dateExample: '24/02/2026',
    numberLocale: 'fr-FR',
    dateLocale: 'fr-FR',
  },
  'pt-BR': {
    label: 'Portugu\u00eas (Brasil)',
    dateFormat: 'DD/MM/YYYY',
    datePlaceholder: 'DD/MM/AAAA',
    dateExample: '24/02/2026',
    numberLocale: 'pt-BR',
    dateLocale: 'pt-BR',
  },
  'ja-JP': {
    label: 'Japanese',
    dateFormat: 'YYYY/MM/DD',
    datePlaceholder: 'YYYY/MM/DD',
    dateExample: '2026/02/24',
    numberLocale: 'ja-JP',
    dateLocale: 'ja-JP',
  },
};

function loadStoredLocale() {
  try {
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
    if (stored && LOCALE_PRESETS[stored.locale]) return stored;
  } catch { /* ignore */ }
  // Default: detect from browser
  const browserLocale = navigator.language || 'en-US';
  const match = Object.keys(LOCALE_PRESETS).find(k => browserLocale.startsWith(k.split('-')[0]));
  return { locale: match || 'en-US', numberDecimals: 1 };
}

export function LocaleProvider({ children }) {
  const [settings, setSettingsState] = useState(loadStoredLocale);

  const setLocale = useCallback((locale) => {
    setSettingsState(prev => {
      const next = { ...prev, locale };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  }, []);

  const setNumberDecimals = useCallback((numberDecimals) => {
    setSettingsState(prev => {
      const next = { ...prev, numberDecimals };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  }, []);

  const preset = LOCALE_PRESETS[settings.locale] || LOCALE_PRESETS['en-US'];

  const value = useMemo(() => ({
    locale: settings.locale,
    numberDecimals: settings.numberDecimals ?? 1,
    preset,
    setLocale,
    setNumberDecimals,
    allPresets: LOCALE_PRESETS,
  }), [settings.locale, settings.numberDecimals, preset, setLocale, setNumberDecimals]);

  return (
    <LocaleContext.Provider value={value}>
      {children}
    </LocaleContext.Provider>
  );
}

export function useLocale() {
  const ctx = useContext(LocaleContext);
  if (!ctx) throw new Error('useLocale must be used within a LocaleProvider');
  return ctx;
}
