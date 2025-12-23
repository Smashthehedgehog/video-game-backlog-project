/**
 * game.ts
 * 
 * PURPOSE:
 * TypeScript type definitions for game-related data structures.
 * Matches the data structure returned from the IGDB backend API.
 */

/**
 * Genre reference data
 */
export interface Genre {
  id: number;
  name: string;
}

/**
 * Platform reference data
 */
export interface Platform {
  id: number;
  name: string;
}

/**
 * Company (developer/publisher) reference data
 */
export interface Company {
  id: number;
  name: string;
}

/**
 * Game cover image
 */
export interface Cover {
  id: number;
  url: string;
  width?: number;
  height?: number;
}

/**
 * Complete game object with all related data
 */
export interface Game {
  id: number;
  name: string;
  summary: string | null;
  first_release_date: string | null;
  rating: number | null;
  total_rating_count: number | null;
  updated_at: string;
  genres?: Genre[];
  platforms?: Platform[];
  companies?: Company[];
}

/**
 * Simplified game object for lists (without full details)
 */
export interface GameListItem {
  id: number;
  name: string;
  first_release_date: string | null;
  rating: number | null;
  summary?: string | null;
  total_rating_count?: number | null;
}

/**
 * Game search result
 */
export interface GameSearchResult {
  id: number;
  name: string;
  first_release_date: string | null;
  rating: number | null;
}

/**
 * Pagination metadata for game lists
 */
export interface GamesPagination {
  page: number;
  limit: number;
  total: number;
}

/**
 * Response from GET /api/games
 */
export interface GamesResponse {
  data: GameListItem[];
  pagination: GamesPagination;
}

/**
 * Response from GET /api/games/:id
 */
export interface GameDetailResponse {
  data: Game;
}

/**
 * Response from GET /api/games/search
 */
export interface GameSearchResponse {
  data: GameSearchResult[];
}

/**
 * Filters for game queries
 */
export interface GameFilters {
  page?: number;
  limit?: number;
  genre?: number;
  platform?: number;
  sortBy?: 'rating' | 'name' | 'first_release_date';
  order?: 'asc' | 'desc';
}

