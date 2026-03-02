/**
 * Tour step definitions, keyed by route.
 * Supports both single-page tours and a cross-page "full app tour".
 * Each step uses DOM element IDs added to the components.
 */

// ── Button presets ──

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

// ── Page order for the full app tour ──
// navId is the sidebar link id used for the "bridge" step highlight.
// pathFn allows dynamic routes (Time Series needs the last viewed series).
export const TOUR_PAGES = [
  { key: 'dashboard',  path: '/',          label: 'Dashboard',     navId: '#nav-dashboard' },
  { key: 'series',     pathFn: () => {
      const ls = localStorage.getItem('last_series');
      return ls ? `/series/${encodeURIComponent(ls)}` : null;
    },                                      label: 'Time Series',   navId: '#nav-series' },
  { key: 'segments',   path: '/segments',   label: 'Segments',      navId: '#nav-segments' },
  { key: 'pipeline',   path: '/pipeline',   label: 'Pipeline',      navId: '#nav-pipeline' },
  { key: 'logs',       path: '/logs',       label: 'Process Log',   navId: '#nav-logs' },
  { key: 'settings',   path: '/settings',   label: 'Settings',      navId: '#nav-settings' },
  { key: 'users',      path: '/users',      label: 'Users',         navId: '#nav-users', adminOnly: true },
];

/**
 * Build the list of available pages for a full tour.
 * Skips Time Series if no series was ever visited, and Users if not admin.
 */
export function getAvailableTourPages(isAdmin = false) {
  return TOUR_PAGES.filter(p => {
    if (p.adminOnly && !isAdmin) return false;
    if (p.pathFn && !p.pathFn()) return false;
    return true;
  }).map(p => ({
    ...p,
    path: p.pathFn ? p.pathFn() : p.path,
  }));
}

// ── Per-page step definitions (without welcome / bridge steps) ──

function dashboardSteps() {
  return [
    {
      id: 'sidebar',
      attachTo: { element: '#sidebar-nav', on: 'right' },
      title: 'Navigation',
      text: 'Use the sidebar to switch between the Dashboard, a specific Time Series, Segments, the Pipeline runner, Process Log, Settings, and User Management.',
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
      buttons: backNext,
    },
  ];
}

function seriesSteps() {
  return [
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
      text: 'Toggle individual forecast methods on/off. Each section has a drag handle on the left \u2014 drag and drop to reorder them. Your preferred layout is saved automatically.',
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
      text: 'The scatter chart plots each method by bias (accuracy) vs RMSE (precision). The green quadrant highlights the best performers. The composite score ranking bar chart shows the weighted score for each method \u2014 lower is better.',
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
  ];
}

function segmentsSteps() {
  return [
    {
      id: 'seg-header',
      attachTo: { element: '#seg-header', on: 'bottom' },
      title: 'Segments Overview',
      text: 'Segments let you group item/site series by filter criteria. Create a new segment with the "+ New Segment" button, or refresh the list.',
      buttons: backNext,
    },
    {
      id: 'seg-table',
      attachTo: { element: '#seg-table', on: 'top' },
      title: 'Segment Table',
      text: 'Each row shows the segment name, description, and member count. Use "Run" to launch a pipeline step scoped to that segment, "Edit" to modify its criteria, "Assign" to refresh membership, or delete it.',
      scrollTo: true,
      buttons: backNext,
    },
    {
      id: 'seg-info',
      attachTo: { element: '#seg-info', on: 'top' },
      title: 'Tips',
      text: 'The Segmentation pipeline step refreshes ABC classes and reassigns all segments. Use Assign on a single segment to re-evaluate just that one.',
      scrollTo: true,
      buttons: backNext,
    },
  ];
}

function pipelineSteps() {
  return [
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
      buttons: backNext,
    },
  ];
}

function logsSteps() {
  return [
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
      buttons: backNext,
    },
  ];
}

function settingsSteps() {
  return [
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
      text: 'Change theme or regional format in the first two tabs. In System Config, edit any pipeline parameter directly \u2014 arrays and objects open in a table editor popup. Changes are saved to config.yaml on the server.',
      scrollTo: true,
      buttons: backNext,
    },
  ];
}

function usersSteps() {
  return [
    {
      id: 'um-header',
      attachTo: { element: '#um-header', on: 'bottom' },
      title: 'User Management',
      text: 'Manage application users. Click "+ New User" to create a local user with a specific role. Only admins can access this page.',
      buttons: backNext,
    },
    {
      id: 'um-list',
      attachTo: { element: '#um-list', on: 'top' },
      title: 'Users List',
      text: 'View all registered users with their auth provider (local, Microsoft, Google), role, and status. Use the action links to promote/demote roles or enable/disable accounts.',
      scrollTo: true,
      buttons: backNext,
    },
  ];
}

/**
 * Map page keys to their step-building functions.
 */
const PAGE_STEP_BUILDERS = {
  dashboard: dashboardSteps,
  series:    seriesSteps,
  segments:  segmentsSteps,
  pipeline:  pipelineSteps,
  logs:      logsSteps,
  settings:  settingsSteps,
  users:     usersSteps,
};

/**
 * Resolve a page key from the current pathname.
 */
function pageKeyFromPath(pathname) {
  if (pathname === '/') return 'dashboard';
  if (pathname.startsWith('/series/')) return 'series';
  if (pathname === '/segments') return 'segments';
  if (pathname === '/pipeline') return 'pipeline';
  if (pathname === '/logs') return 'logs';
  if (pathname === '/settings') return 'settings';
  if (pathname === '/users') return 'users';
  return null;
}

/**
 * Get steps for a single-page tour (original behaviour).
 * Used when the user clicks the Tour button from a specific page.
 */
export function getStepsForRoute(pathname) {
  const welcome = {
    id: 'welcome',
    title: 'Welcome to ForecastAI',
    text: 'This quick tour walks you through the key features of this page. Use the buttons below to navigate.',
    buttons: skipNext,
  };

  const key = pageKeyFromPath(pathname);
  const builder = key && PAGE_STEP_BUILDERS[key];
  if (!builder) return [welcome];

  const steps = builder();
  // Replace the last step's buttons with backDone
  if (steps.length > 0) {
    steps[steps.length - 1] = {
      ...steps[steps.length - 1],
      buttons: backDone,
    };
  }
  return [welcome, ...steps];
}

/**
 * Get steps for the full app tour on a given page.
 * Adds a bridge step at the end that navigates to the next page,
 * or a "Tour Complete" step on the last page.
 *
 * @param {string} pathname         Current route
 * @param {number} pageIndex        Index in the available pages array
 * @param {number} totalPages       Total number of pages in the full tour
 * @param {object|null} nextPage    Next page object {label, navId, path} or null if last
 * @returns {Array} Shepherd step definitions
 */
export function getFullTourSteps(pathname, pageIndex, totalPages, nextPage) {
  const key = pageKeyFromPath(pathname);
  const builder = key && PAGE_STEP_BUILDERS[key];
  const steps = builder ? builder() : [];

  // First page gets the full-tour welcome
  if (pageIndex === 0) {
    steps.unshift({
      id: 'full-tour-welcome',
      title: 'Full App Tour',
      text: `This guided tour walks you through all ${totalPages} pages of ForecastAI. Each page highlights its key features, then navigates you to the next one automatically.`,
      buttons: [
        { text: 'Cancel', action: 'cancel', secondary: true },
        { text: 'Start', action: 'next' },
      ],
    });
  }

  // Add a page counter to the first content step's title (not the welcome)
  const firstContentIdx = pageIndex === 0 ? 1 : 0;
  if (steps[firstContentIdx]) {
    steps[firstContentIdx] = {
      ...steps[firstContentIdx],
      title: `${steps[firstContentIdx].title}  \u2022  Page ${pageIndex + 1}/${totalPages}`,
    };
  }

  if (nextPage) {
    // Bridge step: highlight the sidebar link for the next page
    steps.push({
      id: 'bridge-to-next',
      attachTo: { element: nextPage.navId, on: 'right' },
      title: `Next: ${nextPage.label}`,
      text: `You've explored this page! Click "Continue" to move to the ${nextPage.label} page.`,
      buttons: [
        { text: 'End Tour', action: 'cancel', secondary: true },
        { text: 'Continue \u2192', action: 'navigate-next' },
      ],
      _navigateTo: nextPage.path,
    });
  } else {
    // Last page — tour complete
    steps.push({
      id: 'tour-complete',
      title: 'Tour Complete!',
      text: 'You\'ve explored all the pages of ForecastAI. You can restart the tour anytime from the sidebar.',
      buttons: [
        { text: 'Finish', action: 'complete' },
      ],
    });
  }

  return steps;
}
