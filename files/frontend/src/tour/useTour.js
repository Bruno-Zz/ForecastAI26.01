import { useEffect, useRef, useCallback } from 'react';
import { useLocation } from 'react-router-dom';
import Shepherd from 'shepherd.js';
import { getStepsForRoute } from './tourSteps';
import 'shepherd.js/dist/css/shepherd.css';
import './tourStyles.css';

export function useTour() {
  const location = useLocation();
  const tourRef = useRef(null);

  // Cleanup on route change or unmount
  useEffect(() => {
    return () => {
      if (tourRef.current) {
        tourRef.current.complete();
        tourRef.current = null;
      }
    };
  }, [location.pathname]);

  const startTour = useCallback(() => {
    // Cancel any running tour
    if (tourRef.current) {
      tourRef.current.cancel();
      tourRef.current = null;
    }

    const steps = getStepsForRoute(location.pathname);
    if (steps.length === 0) return;

    const tour = new Shepherd.Tour({
      useModalOverlay: true,
      defaultStepOptions: {
        classes: 'forecastai-tour-step',
        cancelIcon: { enabled: true },
        scrollTo: { behavior: 'smooth', block: 'center' },
      },
    });

    steps.forEach((step) => {
      const buttons = (step.buttons || []).map((btn) => ({
        text: btn.text,
        action: tour[btn.action]?.bind(tour),
        classes: btn.secondary ? 'shepherd-button-secondary' : '',
      }));

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
  }, [location.pathname]);

  return { startTour };
}
