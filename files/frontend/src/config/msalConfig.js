/**
 * Microsoft Authentication Library (MSAL) configuration.
 * Fill in your Azure AD App Registration values via environment variables
 * or replace the placeholder strings directly.
 *
 * Environment variables (set in .env):
 *   VITE_MICROSOFT_CLIENT_ID
 *   VITE_MICROSOFT_TENANT_ID
 */

export const msalConfig = {
  auth: {
    clientId: import.meta.env.VITE_MICROSOFT_CLIENT_ID || 'YOUR-AZURE-APP-CLIENT-ID',
    authority: `https://login.microsoftonline.com/${
      import.meta.env.VITE_MICROSOFT_TENANT_ID || 'common'
    }`,
    redirectUri: window.location.origin,
  },
  cache: {
    cacheLocation: 'localStorage',
    storeAuthStateInCookie: false,
  },
};

export const loginRequest = {
  scopes: ['User.Read'],
};
