import { useEffect, useRef, useCallback } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import Shepherd from 'shepherd.js';
import {
  getStepsForRoute,
  getFullTourSteps,
  getAvailableTourPages,
} from './tourSteps';
import 'shepherd.js/dist/css/shepherd.css';
import './tourStyles.css';

// ── Session storage helpers for cross-page state ──
const TOUR_KEY = 'forecastai_full_tour';

function getFullTourState() {
  try {
    const raw = sessionStorage.getItem(TOUR_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function setFullTourState(state) {
  sessionStorage.setItem(TOUR_KEY, JSON.stringify(state));
}

function clearFullTour() {
  sessionStorage.removeItem(TOUR_KEY);
}

// ── Hook ──

export function useTour(isAdmin = false) {
  const location = useLocation();
  const navigate = useNavigate();
  const tourRef = useRef(null);
  // Flag to prevent clearing tour state while navigating between pages
  const isNavigatingRef = useRef(false);

  // Cleanup on route change or unmount — but not during cross-page navigation
  useEffect(() => {
    return () => {
      if (tourRef.current) {
        if (!isNavigatingRef.current) {
          tourRef.current.complete();
        }
        tourRef.current = null;
      }
    };
  }, [location.pathname]);

  /**
   * Build and launch a Shepherd tour from an array of step definitions.
   * Handles the custom 'navigate-next' action for bridge steps.
   */
  const launchTour = useCallback((steps) => {
    if (tourRef.current) {
      tourRef.current.cancel();
      tourRef.current = null;
    }
    if (!steps || steps.length === 0) return;

    const tour = new Shepherd.Tour({
      useModalOverlay: true,
      defaultStepOptions: {
        classes: 'forecastai-tour-step',
        cancelIcon: { enabled: true },
        scrollTo: { behavior: 'smooth', block: 'center' },
      },
    });

    // When the full tour is cancelled or completed, clean up session state
    // — but NOT if we're mid-navigation to the next tour page
    tour.on('cancel', () => { if (!isNavigatingRef.current) clearFullTour(); });
    tour.on('complete', () => { if (!isNavigatingRef.current) clearFullTour(); });

    steps.forEach((step) => {
      const buttons = (step.buttons || []).map((btn) => {
        if (btn.action === 'navigate-next' && step._navigateTo) {
          return {
            text: btn.text,
            classes: btn.secondary ? 'shepherd-button-secondary' : '',
            action: () => {
              // Advance the page index in session state and navigate
              const state = getFullTourState();
              if (state) {
                state.pageIndex = (state.pageIndex || 0) + 1;
                setFullTourState(state);
              }
              // Set flag BEFORE complete/navigate so handlers don't wipe state
              isNavigatingRef.current = true;
              tour.complete();            // clean up current tour visuals
              navigate(step._navigateTo); // go to the next page
            },
          };
        }
        return {
          text: btn.text,
          action: tour[btn.action]?.bind(tour),
          classes: btn.secondary ? 'shepherd-button-secondary' : '',
        };
      });

      tour.addStep({
        id: step.id,
        title: step.title,
        text: step.text,
        attachTo: step.attachTo,
        buttons,
        scrollTo: step.scrollTo !== false,
        when: {
          show() {
            // Skip step if target element does not exist
            if (step.attachTo?.element && !document.querySelector(step.attachTo.element)) {
              tour.next();
            }
          },
        },
      });
    });

    tourRef.current = tour;
    tour.start();
  }, [navigate]);

  /**
   * Start a single-page tour for the current route (original behaviour).
   */
  const startTour = useCallback(() => {
    clearFullTour();
    const steps = getStepsForRoute(location.pathname);
    launchTour(steps);
  }, [location.pathname, launchTour]);

  /**
   * Start the full app tour: navigates through all pages sequentially.
   */
  const startFullTour = useCallback(() => {
    const pages = getAvailableTourPages(isAdmin);
    if (pages.length === 0) return;

    const state = { active: true, pageIndex: 0, pages };
    setFullTourState(state);

    // Navigate to the first page if not already there
    const firstPath = pages[0].path;
    if (location.pathname !== firstPath) {
      navigate(firstPath);
      // Tour will auto-start on the new page via the useEffect below
    } else {
      // Already on the first page — start the tour now
      const nextPage = pages.length > 1 ? pages[1] : null;
      const steps = getFullTourSteps(firstPath, 0, pages.length, nextPage);
      launchTour(steps);
    }
  }, [isAdmin, location.pathname, navigate, launchTour]);

  /**
   * Auto-continue the full tour when navigating to a new page.
   */
  useEffect(() => {
    const state = getFullTourState();
    if (!state?.active) return;

    const { pageIndex, pages } = state;
    if (!pages || pageIndex >= pages.length) {
      clearFullTour();
      return;
    }

    // Give the new page's DOM time to render
    const timer = setTimeout(() => {
      // Reset navigation flag now that we're launching the tour on the new page
      isNavigatingRef.current = false;

      const nextPage = pageIndex + 1 < pages.length ? pages[pageIndex + 1] : null;
      const steps = getFullTourSteps(
        location.pathname,
        pageIndex,
        pages.length,
        nextPage,
      );
      launchTour(steps);
    }, 600);

    return () => clearTimeout(timer);
  }, [location.pathname, launchTour]);

  return { startTour, startFullTour };
}
