import { useState, useEffect } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import Dashboard from './components/Dashboard';
import TimeSeriesViewer from './components/TimeSeriesViewer';
import PipelineRunner from './components/PipelineRunner';

function Sidebar({ open, onToggle }) {
  const location = useLocation();
  const [lastSeries, setLastSeries] = useState(null);

  useEffect(() => {
    const ls = localStorage.getItem('last_series');
    if (ls) setLastSeries(ls);
  }, [location]);

  const navLink = (to, icon, label, disabled = false) => {
    const isActive = location.pathname === to || (to !== '/' && location.pathname.startsWith(to));
    return disabled ? (
      <span className="flex items-center gap-3 px-3 py-2 rounded-lg text-gray-400 cursor-not-allowed text-sm">
        <span className="text-base">{icon}</span>
        {open && <span className="truncate">{label}</span>}
      </span>
    ) : (
      <Link
        to={to}
        className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors
          ${isActive ? 'bg-blue-50 text-blue-700' : 'text-gray-700 hover:bg-gray-100 hover:text-gray-900'}`}
      >
        <span className="text-base flex-shrink-0">{icon}</span>
        {open && <span className="truncate">{label}</span>}
      </Link>
    );
  };

  return (
    <aside
      className={`flex flex-col bg-white border-r border-gray-200 transition-all duration-200 ease-in-out flex-shrink-0
        ${open ? 'w-52' : 'w-14'}`}
    >
      {/* Logo / Title */}
      <div className={`flex items-center h-14 border-b border-gray-100 px-3 gap-3 ${open ? 'justify-between' : 'justify-center'}`}>
        {open && (
          <Link to="/" className="text-base font-bold text-blue-600 truncate hover:text-blue-700">
            ForecastAI
          </Link>
        )}
        <button
          onClick={onToggle}
          className="p-1.5 rounded-md text-gray-500 hover:bg-gray-100 hover:text-gray-700 flex-shrink-0"
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
      <nav className="flex-1 p-2 space-y-1 overflow-hidden">
        {navLink('/', '🏠', 'Dashboard')}
        {lastSeries
          ? navLink(`/series/${encodeURIComponent(lastSeries)}`, '📈', `Series: ${lastSeries}`)
          : navLink('/', '📈', 'Time Series', true)
        }
        {navLink('/pipeline', '⚙️', 'Pipeline')}
      </nav>

      {/* Footer */}
      {open && (
        <div className="p-3 border-t border-gray-100">
          <p className="text-xs text-gray-400 truncate">ForecastAI 2026.01</p>
        </div>
      )}
    </aside>
  );
}

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    const stored = localStorage.getItem('sidebar_open');
    return stored === null ? true : stored === 'true';
  });
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const toggleSidebar = () => {
    setSidebarOpen(prev => {
      const next = !prev;
      localStorage.setItem('sidebar_open', String(next));
      return next;
    });
  };

  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden">
      {/* Mobile backdrop */}
      {mobileMenuOpen && (
        <div
          className="fixed inset-0 z-40 bg-black bg-opacity-40 md:hidden"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      {/* Sidebar — inline on desktop, drawer on mobile */}
      <div className="hidden md:flex">
        <Sidebar open={sidebarOpen} onToggle={toggleSidebar} />
      </div>
      {mobileMenuOpen && (
        <div className="fixed inset-y-0 left-0 z-50 flex md:hidden">
          <Sidebar open={true} onToggle={() => setMobileMenuOpen(false)} />
        </div>
      )}

      {/* Main area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Mobile top bar */}
        <div className="flex items-center h-12 px-4 bg-white border-b border-gray-200 md:hidden flex-shrink-0">
          <button
            onClick={() => setMobileMenuOpen(true)}
            className="p-1.5 rounded-md text-gray-500 hover:bg-gray-100 mr-3"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
            </svg>
          </button>
          <Link to="/" className="text-base font-bold text-blue-600">ForecastAI</Link>
        </div>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/series/:uniqueId" element={<TimeSeriesViewer />} />
            <Route path="/pipeline" element={<PipelineRunner />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}

export default App;
