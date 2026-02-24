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
        text: 'Use the sidebar to switch between the Dashboard, a specific Time Series, the Pipeline runner, and the Process Log.',
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
        text: 'Click any row to drill into that series. Columns are sortable. Each row includes a sparkline showing historical demand and forecast.',
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
        id: 'main-chart',
        attachTo: { element: '#tsv-main-chart', on: 'top' },
        title: 'Historical Data & Forecasts',
        text: 'The main chart plots historical demand and all forecast methods. Shaded bands show 50% and 90% prediction intervals. Use the slider below to zoom a date range.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'forecast-table',
        attachTo: { element: '#tsv-forecast-table', on: 'top' },
        title: 'Forecast Values & Adjustments',
        text: 'Point forecast values for each method and horizon month. For the best method, expand adjustment rows to apply additive deltas or full overrides. The Consensus row shows final adjusted values.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'scoring',
        attachTo: { element: '#tsv-scoring', on: 'top' },
        title: 'Accuracy vs Precision',
        text: 'The scatter chart plots each method by bias (accuracy) vs RMSE (precision). The green quadrant highlights the best performers.',
        scrollTo: true,
        buttons: backNext,
      },
      {
        id: 'drag-reorder',
        attachTo: { element: '#tsv-toggles', on: 'top' },
        title: 'Drag to Reorder Sections',
        text: 'Every section has a drag handle on the left. Drag and drop sections to reorder them — your preferred layout is saved automatically.',
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
        text: 'Click "Run All" to execute all 6 pipeline steps in order. The numbered circles track progress. If any step fails, the pipeline stops automatically.',
        buttons: backNext,
      },
      {
        id: 'individual-steps',
        attachTo: { element: '#pipeline-steps', on: 'top' },
        title: 'Individual Steps',
        text: 'Run any single step independently. Each card shows its status, timing, and a collapsible log viewer with color-coded output.',
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

  // ── Fallback (e.g. /logs) ──
  return [welcome];
}
