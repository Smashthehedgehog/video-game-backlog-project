/**
 * useGames.ts
 * 
 * PURPOSE:
 * React hook for managing game data fetching and state.
 * Provides functions to fetch games, search, and handle pagination.
 */

import { useState, useCallback } from 'react';
import * as gamesApi from '../api/games';
import type {
  GameListItem,
  Game,
  GameSearchResult,
  GameFilters,
  GamesPagination,
  Genre,
  Platform
} from '../types/game';

interface UseGamesReturn {
  games: GameListItem[];
  pagination: GamesPagination | null;
  isLoading: boolean;
  error: string | null;
  fetchGames: (filters?: GameFilters) => Promise<void>;
  searchGames: (query: string) => Promise<GameSearchResult[]>;
  getGameDetails: (id: number) => Promise<Game>;
  clearError: () => void;
}

export function useGames(): UseGamesReturn {
  const [games, setGames] = useState<GameListItem[]>([]);
  const [pagination, setPagination] = useState<GamesPagination | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  /**
   * Fetch paginated list of games
   */
  const fetchGames = useCallback(async (filters?: GameFilters) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await gamesApi.getGames(filters);
      setGames(response.data);
      setPagination(response.pagination);
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to fetch games';
      setError(errorMessage);
      console.error('Fetch games error:', err);
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Search for games by name
   */
  const searchGames = useCallback(async (query: string): Promise<GameSearchResult[]> => {
    setIsLoading(true);
    setError(null);
    
    try {
      const results = await gamesApi.searchGames(query);
      return results;
    } catch (err: any) {
      const errorMessage = err.message || 'Search failed';
      setError(errorMessage);
      console.error('Search games error:', err);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Get detailed information for a single game
   */
  const getGameDetails = useCallback(async (id: number): Promise<Game> => {
    setIsLoading(true);
    setError(null);
    
    try {
      const game = await gamesApi.getGameById(id);
      return game;
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to fetch game details';
      setError(errorMessage);
      console.error('Get game details error:', err);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Clear error message
   */
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  return {
    games,
    pagination,
    isLoading,
    error,
    fetchGames,
    searchGames,
    getGameDetails,
    clearError
  };
}

/**
 * Hook for fetching reference data (genres, platforms)
 */
interface UseReferenceDataReturn {
  genres: Genre[];
  platforms: Platform[];
  isLoading: boolean;
  error: string | null;
  fetchGenres: () => Promise<void>;
  fetchPlatforms: () => Promise<void>;
}

export function useReferenceData(): UseReferenceDataReturn {
  const [genres, setGenres] = useState<Genre[]>([]);
  const [platforms, setPlatforms] = useState<Platform[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchGenres = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      const data = await gamesApi.getGenres();
      setGenres(data);
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to fetch genres';
      setError(errorMessage);
      console.error('Fetch genres error:', err);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const fetchPlatforms = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      const data = await gamesApi.getPlatforms();
      setPlatforms(data);
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to fetch platforms';
      setError(errorMessage);
      console.error('Fetch platforms error:', err);
    } finally {
      setIsLoading(false);
    }
  }, []);

  return {
    genres,
    platforms,
    isLoading,
    error,
    fetchGenres,
    fetchPlatforms
  };
}

