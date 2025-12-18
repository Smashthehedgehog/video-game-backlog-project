/**
 * library.ts
 * 
 * PURPOSE:
 * TypeScript type definitions for user library (backlog) data structures.
 * Defines the shape of library entries and related operations.
 */

import type { LibraryStatus } from '../utils/constants';
import type { GameListItem } from './game';

/**
 * Library entry (game in user's backlog)
 */
export interface LibraryEntry {
  id: number;
  user_id: string;
  game_id: number;
  status: LibraryStatus;
  rating: number | null;
  notes: string | null;
  created_at: string;
  updated_at: string;
  igdb_games?: GameListItem;
}

/**
 * Request to add a game to library
 */
export interface AddToLibraryRequest {
  gameId: number;
  status: LibraryStatus;
}

/**
 * Request to update a library entry
 */
export interface UpdateLibraryRequest {
  status?: LibraryStatus;
  rating?: number;
  notes?: string;
}

/**
 * Response from GET /api/backlog
 */
export interface LibraryResponse {
  data: LibraryEntry[];
}

/**
 * Response from POST /api/backlog
 */
export interface AddToLibraryResponse {
  data: LibraryEntry;
}

/**
 * Response from PATCH /api/backlog/:gameId
 */
export interface UpdateLibraryResponse {
  data: LibraryEntry;
}

/**
 * Response from GET /api/backlog/:gameId (check if in library)
 */
export interface CheckLibraryResponse {
  inBacklog: boolean;
  status: LibraryStatus | null;
}

/**
 * Library statistics
 */
export interface LibraryStats {
  total: number;
  want_to_play: number;
  playing: number;
  completed: number;
  dropped: number;
  on_hold: number;
}

