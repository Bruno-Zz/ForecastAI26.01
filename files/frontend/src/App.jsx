import { useState, useEffect, lazy, Suspense, Component } from 'react';
import { Routes, Route, Link, Navigate, useLocation } from 'react-router-dom';
import Login from './components/Login';
import ThemeToggle from './components/ThemeToggle';
import { useAuth } from './contexts/AuthContext';
import { useTour } from './tour/useTour';

/** Error boundary — catches render crashes so they show a message instead of a black screen */
class PageErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null }; }
  static getDerivedStateFromError(err) { return { error: err }; }
  componentDidCatch(err, info) { console.error('Page render error:', err, info); }
  render() {
    if (this.state.error) {
      return (
        <div className="flex flex-col items-center justify-center h-64 gap-4 p-8">
          <p className="text-red-600 dark:text-red-400 font-semibold">Something went wrong rendering this page.</p>
          <pre className="text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 rounded p-3 max-w-xl overflow-auto whitespace-pre-wrap">
            {this.state.error?.message}
          </pre>
          <button
            onClick={() => this.setState({ error: null })}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg"
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

/* ── Lazy-loaded route components (code-split into separate chunks) ── */
const Dashboard = lazy(() => import('./components/Dashboard'));
const TimeSeriesViewer = lazy(() => import('./components/TimeSeriesViewer'));
const ProcessRunner = lazy(() => import('./components/ProcessRunner'));
const ProcessLog = lazy(() => import('./components/ProcessLog'));
const Settings = lazy(() => import('./components/Settings'));
const Segments = lazy(() => import('./components/Segments'));
const UserManagement = lazy(() => import('./components/UserManagement'));
const AuditLog = lazy(() => import('./components/AuditLog'));
const ABCClassification = lazy(() => import('./components/ABCClassification'));
const ScenarioManager = lazy(() => import('./components/ScenarioManager'));

/** Shared loading fallback shown while a lazy chunk downloads */
const PageSpinner = () => (
  <div className="flex items-center justify-center h-64">
    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
  </div>
);

function Sidebar({ open, onToggle, onStartTour, onStartFullTour }) {
  const location = useLocation();
  const { user, isAdmin, logout } = useAuth();
  const [lastSeries, setLastSeries] = useState(null);
  const [lastSeriesLabel, setLastSeriesLabel] = useState(null);

  useEffect(() => {
    const ls = localStorage.getItem('last_series');
    const lsl = localStorage.getItem('last_series_label');
    if (ls) setLastSeries(ls);
    setLastSeriesLabel(lsl || null);
  }, [location]);

  const navLink = (to, icon, label, disabled = false, navId = undefined) => {
    const isActive = location.pathname === to || (to !== '/' && location.pathname.startsWith(to));
    return disabled ? (
      <span id={navId} className="flex items-center gap-3 px-3 py-2 rounded-lg text-gray-400 dark:text-gray-600 cursor-not-allowed text-sm">
        <span className="text-base">{icon}</span>
        {open && <span className="truncate">{label}</span>}
      </span>
    ) : (
      <Link
        id={navId}
        to={to}
        className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors
          ${isActive
            ? 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
            : 'text-gray-700 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-300 dark:hover:bg-gray-700 dark:hover:text-white'}`}
      >
        <span className="text-base flex-shrink-0">{icon}</span>
        {open && <span className="truncate">{label}</span>}
      </Link>
    );
  };

  return (
    <aside
      className={`flex flex-col bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 transition-all duration-200 ease-in-out flex-shrink-0
        ${open ? 'w-52' : 'w-14'}`}
    >
      {/* Logo / Title */}
      <div className={`flex items-center h-14 border-b border-gray-100 dark:border-gray-700 px-3 gap-3 ${open ? 'justify-between' : 'justify-center'}`}>
        {open && (
          <Link to="/" className="text-base font-bold text-blue-600 dark:text-blue-400 truncate hover:text-blue-700 dark:hover:text-blue-300">
            ForecastAI
          </Link>
        )}
        <button
          onClick={onToggle}
          className="p-1.5 rounded-md text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-700 dark:hover:text-white flex-shrink-0"
          title={open ? 'Collapse sidebar' : 'Expand sidebar'}
        >
          {open ? (
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7M18 19l-7-7 7-7" />
            </svg>
          ) : (
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 5l7 7-7 7M6 5l7 7-7 7" />
            </svg>
          )}
        </button>
      </div>

      {/* Nav links */}
      <nav id="sidebar-nav" className="flex-1 p-2 space-y-1 overflow-hidden">
        {navLink('/', '🏠', 'Dashboard', false, 'nav-dashboard')}
        {lastSeries
          ? navLink(`/series/${encodeURIComponent(lastSeries)}`, '📈', lastSeriesLabel || lastSeries, false, 'nav-series')
          : navLink('/', '📈', 'Time Series', true, 'nav-series')
        }
        {navLink('/segments', '🗂️', 'Segments', false, 'nav-segments')}
        {navLink('/abc', '🏷️', 'Classifications', false, 'nav-abc')}
        {navLink('/scenarios', '🔀', 'Scenarios', false, 'nav-scenarios')}
        {navLink('/processes', '⚙️', 'Process Runner', false, 'nav-pipeline')}
        {navLink('/logs', '📋', 'Process Log', false, 'nav-logs')}
        {navLink('/settings', '🔧', 'Settings', false, 'nav-settings')}
        {navLink('/audit', '📝', 'Audit Log', false, 'nav-audit')}
        {isAdmin && navLink('/users', '👥', 'Users', false, 'nav-users')}
      </nav>

      {/* Tour triggers */}
      <div className="px-2 pb-1 space-y-0.5">
        <button
          id="tour-trigger"
          onClick={onStartTour}
          className="flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-900 dark:hover:text-white w-full"
          title="Tour this page"
        >
          <span className="text-base flex-shrink-0">🎯</span>
          {open && <span className="truncate">Page Tour</span>}
        </button>
        <button
          id="full-tour-trigger"
          onClick={onStartFullTour}
          className="flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-900 dark:hover:text-white w-full"
          title="Tour all pages"
        >
          <span className="text-base flex-shrink-0">🗺️</span>
          {open && <span className="truncate">Full Tour</span>}
        </button>
      </div>

      {/* User info + logout + theme toggle */}
      <div className="px-2 pb-2 border-t border-gray-100 dark:border-gray-700 pt-2 space-y-1">
        {open && user && (
          <div className="px-3 py-1.5">
            <p className="text-xs font-medium text-gray-700 dark:text-gray-200 truncate">{user.display_name}</p>
            <p className="text-xs text-gray-400 dark:text-gray-500 truncate">{user.email}</p>
            {isAdmin && (
              <span className="inline-block mt-0.5 px-1.5 py-0.5 text-[10px] font-medium bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300 rounded">
                Admin
              </span>
            )}
          </div>
        )}
        <div className={`flex items-center ${open ? 'justify-between px-1' : 'justify-center'}`}>
          {open && (
            <button
              onClick={logout}
              className="text-xs text-gray-500 dark:text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors px-2 py-1 rounded"
              title="Sign out"
            >
              Sign out
            </button>
          )}
          <ThemeToggle />
        </div>
      </div>
    </aside>
  );
}

function App() {
  const { isAuthenticated, loading, isAdmin } = useAuth();
  const { startTour, startFullTour } = useTour(isAdmin);
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    const stored = localStorage.getItem('sidebar_open');
    return stored === null ? true : stored === 'true';
  });
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  // Auto-close mobile drawer on navigation
  useEffect(() => {
    setMobileMenuOpen(false);
  }, [location.pathname]);

  const toggleSidebar = () => {
    setSidebarOpen(prev => {
      const next = !prev;
      localStorage.setItem('sidebar_open', String(next));
      return next;
    });
  };

  // Show loading spinner while validating token
  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-50 dark:bg-gray-900">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  // Unauthenticated: only show login page
  if (!isAuthenticated) {
    return (
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  }

  // Authenticated: full app with sidebar
  return (
    <div className="flex h-screen overflow-hidden" style={{ backgroundColor: 'var(--color-surface-alt)' }}>
      {/* Mobile backdrop */}
      {mobileMenuOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/40 md:hidden"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      {/* Sidebar — inline on desktop, drawer on mobile */}
      <div className="hidden md:flex">
        <Sidebar open={sidebarOpen} onToggle={toggleSidebar} onStartTour={startTour} onStartFullTour={startFullTour} />
      </div>
      {mobileMenuOpen && (
        <div className="fixed inset-y-0 left-0 z-50 flex md:hidden shadow-2xl">
          <Sidebar open={true} onToggle={() => setMobileMenuOpen(false)} onStartTour={startTour} onStartFullTour={startFullTour} />
        </div>
      )}

      {/* Main area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Mobile top bar */}
        <div className="flex items-center justify-between h-12 px-4 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 md:hidden flex-shrink-0">
          <div className="flex items-center">
            <button
              onClick={() => setMobileMenuOpen(true)}
              className="p-1.5 rounded-md text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 mr-3"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>
            <Link to="/" className="text-base font-bold text-blue-600 dark:text-blue-400">ForecastAI</Link>
          </div>
          <ThemeToggle />
        </div>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto">
          <PageErrorBoundary>
          <Suspense fallback={<PageSpinner />}>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/series/:uniqueId" element={<TimeSeriesViewer />} />
              <Route path="/segments" element={<Segments />} />
              <Route path="/abc" element={<ABCClassification />} />
              <Route path="/processes" element={<ProcessRunner />} />
              <Route path="/pipeline" element={<ProcessRunner />} />
              <Route path="/logs" element={<ProcessLog />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="/audit" element={<AuditLog />} />
              <Route path="/users" element={<UserManagement />} />
              <Route path="/scenarios" element={<ScenarioManager />} />
              <Route path="/login" element={<Navigate to="/" replace />} />
            </Routes>
          </Suspense>
          </PageErrorBoundary>
        </main>
      </div>
    </div>
  );
}

export default App;
