/**
 * Tour step definitions, keyed by route.
 * Each step uses DOM element IDs added to the components.
 */

const backNext = [
  { text: 'Back', action: 'back', secondary: true },
  { text: 'Next', action: 'next' },
];

const backDone = [
  { text: 'Back', action: 'back', secondary: true },
  { text: 'Done', action: 'complete' },
];

const skipNext = [
  { text: 'Skip', action: 'cancel', secondary: true },
  { text: 'Next', action: 'next' },
];

export function getStepsForRoute(pathname) {
  const welcome = {
    id: 'welcome',
    title: 'Welcome to ForecastAI',
    text: 'This quick tour walks you through the key features of this page. Use the buttons below to navigate.',
    buttons: skipNext,
  };

  // ── Dashboard ──
  if (pathname === '/') {
    return [
      welcome,
      {
        id: 'sidebar',
        attachTo: { element: '#sidebar-nav', on: 'right' },
        title: 'Navigation',
        text: 'Use the sidebar to switch between the Dashboard, a specific Time Series, the Pipeline runner, Process Log, and Settings.',
        buttons: backNext,
      },
      {
        id: 'summary',
        attachTo: { element: '#dash-summary', on: 'bottom' },
        title: 'Summary Metrics',
        text: 'Key metrics at a glance: total series count, backtested count, seasonal/trending/intermittent splits, average observations, and outlier adjustments.',
        buttons: backNext,
      },
      {
        id: 'charts',
        attachTo: { element: '#dash-charts', on: 'bottom' },
        title: 'Analytics Charts',
        text: 'The complexity distribution pie chart and the best-method bar chart summarize your entire forecasting population.',
        buttons: backNext,
      },
      {
        id: 'filters',
        attachTo: { element: '#dash-filters', on: 'bottom' },
        title: 'Search & Filter',
        text: 'Search series by ID, filter by complexity level, or filter intermittent vs continuous. The table updates instantly.',
        buttons: backNext,
      },
      {
        id: 'table',
        attachTo: { element: '#dash-table', on: 'top' },
        title: 'Series Table',
        text: 'Click any row to drill into that series. Columns are sortable. Each row includes a sparkline showing historical demand and forecast. Use the Previous / Next buttons at the bottom to paginate.',
        scrollTo: true,
        buttons: backDone,
      },
    ];
  }

  // ── Time Series Viewer ──
  if (pathname.startsWith('/series/')) {
    return [
      welcome,
      {
        id: 'selector',
        attachTo: { element: '#tsv-selector', on: 'bottom' },
        title: 'Item & Site Selection',
        text: 'Select one or more items and sites. Selecting multiple creates an aggregated multi-series view. Recently accessed items appear first.',
        buttons: backNext,
      },
      {
        id: 'header',
        attachTo: { element: '#tsv-header', on: 'bottom' },
        title: 'Series Characteristics',
        text: 'Badges show whether the series is seasonal, trending, intermittent, its complexity level, outlier count, and the winning forecast method.',
        buttons: backNext,
      },
      {
        id: 'toggles',
        attachTo: { element: '#tsv-toggles', on: 'bottom' },
        title: 'Method Toggles & Reordering',
        text: 'Toggle individual forecast methods on/off. Each section has a drag handle on the left — drag and drop to reorder them. Your preferred layout is saved automatically.',
        buttons: backNext,
      },
      {
        id: 'main-chart',
        attachTo: { element: '#tsv-main-chart', on: 'top' },
        title: 'Historical Data & Forecasts',
        text: 'The main chart plots historical demand and all forecast methods. Shaded bands show 50% and 90% prediction intervals. Use the slider below to zoom a date range.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'scoring',
        attachTo: { element: '#tsv-scoring', on: 'top' },
        title: 'Accuracy vs Precision & Composite Score',
        text: 'The scatter chart plots each method by bias (accuracy) vs RMSE (precision). The green quadrant highlights the best performers. The composite score ranking bar chart shows the weighted score for each method — lower is better.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'forecast-table',
        attachTo: { element: '#tsv-forecast-table', on: 'top' },
        title: 'Forecast Values & Adjustments',
        text: 'Point forecast values for each method and horizon month. For the best method, expand adjustment rows to apply additive deltas or full overrides. The Consensus row shows final adjusted values.',
        scrollTo: true,
        buttons: backDone,
      },
    ];
  }

  // ── Pipeline Runner ──
  if (pathname === '/pipeline') {
    return [
      welcome,
      {
        id: 'full-pipeline',
        attachTo: { element: '#pipeline-full', on: 'bottom' },
        title: 'Run Full Pipeline',
        text: 'Click "Run All" to execute all 6 pipeline steps in order. The numbered circles track progress. If any step fails, the pipeline stops automatically. The state persists even if you navigate away.',
        buttons: backNext,
      },
      {
        id: 'individual-steps',
        attachTo: { element: '#pipeline-steps', on: 'top' },
        title: 'Individual Steps',
        text: 'Run any single step independently. Each card shows its status, timing, and a collapsible log viewer with color-coded output. Use the Stop button to interrupt a running step.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'pipeline-notes',
        attachTo: { element: '#pipeline-notes', on: 'top' },
        title: 'Important Notes',
        text: 'Key details about step dependencies, database connections, forecast duration, and when to restart the API.',
        scrollTo: true,
        buttons: backDone,
      },
    ];
  }

  // ── Process Log ──
  if (pathname === '/logs') {
    return [
      welcome,
      {
        id: 'logs-header',
        attachTo: { element: '#logs-header', on: 'bottom' },
        title: 'Process Log Overview',
        text: 'This page shows all pipeline execution history. Live running processes appear at the top with a pulsing indicator. Use the Refresh button to update.',
        buttons: backNext,
      },
      {
        id: 'logs-history',
        attachTo: { element: '#logs-history', on: 'top' },
        title: 'Run History',
        text: 'Expand any historical run to see its individual steps, status, duration, and log output. Click a step to view its detailed console logs.',
        scrollTo: true,
        buttons: backDone,
      },
    ];
  }

  // ── Settings ──
  if (pathname === '/settings') {
    return [
      welcome,
      {
        id: 'settings-tabs',
        attachTo: { element: '#settings-tabs', on: 'bottom' },
        title: 'Settings Tabs',
        text: 'Switch between Appearance (theme, dark mode), Locale (regional format, number precision), and System Config (pipeline and model parameters).',
        buttons: backNext,
      },
      {
        id: 'settings-content',
        attachTo: { element: '#settings-content', on: 'top' },
        title: 'Edit Configuration',
        text: 'Change theme or regional format in the first two tabs. In System Config, edit any pipeline parameter directly — arrays and objects open in a table editor popup. Changes are saved to config.yaml on the server.',
        scrollTo: true,
        buttons: backDone,
      },
    ];
  }

  // ── Fallback ──
  return [welcome];
}
