/**
 * constants.ts
 * 
 * PURPOSE:
 * Centralized configuration and constants for the application.
 * Single source of truth for API URLs, storage keys, and app-wide constants.
 */

// API Configuration
export const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://video-game-backlog-api.onrender.com';

// Local Storage Keys
export const TOKEN_KEY = 'vg_backlog_auth_token';
export const USER_KEY = 'vg_backlog_user';

// Library Status Options
export const LIBRARY_STATUSES = [
  'want_to_play',
  'playing',
  'completed',
  'dropped',
  'on_hold'
] as const;

export type LibraryStatus = typeof LIBRARY_STATUSES[number];

// Status Display Labels
export const STATUS_LABELS: Record<LibraryStatus, string> = {
  want_to_play: 'Want to Play',
  playing: 'Playing',
  completed: 'Completed',
  dropped: 'Dropped',
  on_hold: 'On Hold'
};

// API Endpoints
export const API_ENDPOINTS = {
  // Auth
  AUTH_SIGNUP: '/api/auth/signup',
  AUTH_SIGNIN: '/api/auth/signin',
  AUTH_SIGNOUT: '/api/auth/signout',
  AUTH_ME: '/api/auth/me',
  
  // Games
  GAMES: '/api/games',
  GAMES_SEARCH: '/api/games/search',
  GAME_BY_ID: (id: number) => `/api/games/${id}`,
  
  // Library (Backlog)
  LIBRARY: '/api/backlog',
  LIBRARY_ITEM: (gameId: number) => `/api/backlog/${gameId}`,
  
  // Reference Data
  GENRES: '/api/reference/genres',
  PLATFORMS: '/api/reference/platforms',
  COMPANIES: '/api/reference/companies'
} as const;

// Pagination
export const DEFAULT_PAGE_SIZE = 20;
export const MAX_PAGE_SIZE = 100;

// Rating Range
export const MIN_RATING = 1;
export const MAX_RATING = 10;

