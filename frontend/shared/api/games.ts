/**
 * games.ts
 * 
 * PURPOSE:
 * API functions for game-related operations.
 * Handles fetching game lists, searching, and retrieving game details.
 */

import { apiClient } from './client';
import { API_ENDPOINTS } from '../utils/constants';
import type {
  Game,
  GameListItem,
  GameSearchResult,
  GamesResponse,
  GameDetailResponse,
  GameSearchResponse,
  GameFilters,
  Genre,
  Platform,
  Company
} from '../types/game';

/**
 * Get paginated list of games with optional filters
 */
export async function getGames(filters?: GameFilters): Promise<GamesResponse> {
  return apiClient.get<GamesResponse>(API_ENDPOINTS.GAMES, {
    params: filters as Record<string, string | number | boolean>
  });
}

/**
 * Get a single game by ID with full details
 */
export async function getGameById(id: number): Promise<Game> {
  const response = await apiClient.get<GameDetailResponse>(
    API_ENDPOINTS.GAME_BY_ID(id)
  );
  return response.data;
}

/**
 * Search for games by name
 */
export async function searchGames(query: string, limit?: number): Promise<GameSearchResult[]> {
  const response = await apiClient.get<GameSearchResponse>(
    API_ENDPOINTS.GAMES_SEARCH,
    {
      params: { q: query, ...(limit && { limit }) }
    }
  );
  return response.data;
}

/**
 * Get all genres
 */
export async function getGenres(): Promise<Genre[]> {
  const response = await apiClient.get<{ data: Genre[] }>(API_ENDPOINTS.GENRES);
  return response.data;
}

/**
 * Get all platforms
 */
export async function getPlatforms(): Promise<Platform[]> {
  const response = await apiClient.get<{ data: Platform[] }>(API_ENDPOINTS.PLATFORMS);
  return response.data;
}

/**
 * Get all companies
 */
export async function getCompanies(limit?: number): Promise<Company[]> {
  const response = await apiClient.get<{ data: Company[] }>(
    API_ENDPOINTS.COMPANIES,
    {
      params: limit ? { limit } : undefined
    }
  );
  return response.data;
}

