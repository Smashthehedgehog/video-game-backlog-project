/**
 * useLibrary.ts
 * 
 * PURPOSE:
 * React hook for managing user's game library (backlog).
 * Provides functions to add, update, remove games and manage library state.
 */

import { useState, useCallback, useEffect } from 'react';
import * as libraryApi from '../api/library';
import type { LibraryStatus } from '../utils/constants';
import type { LibraryEntry, UpdateLibraryRequest } from '../types/library';

interface UseLibraryReturn {
  library: LibraryEntry[];
  isLoading: boolean;
  error: string | null;
  fetchLibrary: (status?: LibraryStatus) => Promise<void>;
  addToLibrary: (gameId: number, status?: LibraryStatus) => Promise<void>;
  updateEntry: (gameId: number, updates: UpdateLibraryRequest) => Promise<void>;
  removeFromLibrary: (gameId: number) => Promise<void>;
  isInLibrary: (gameId: number) => boolean;
  getEntryByGameId: (gameId: number) => LibraryEntry | undefined;
  clearError: () => void;
}

export function useLibrary(autoFetch: boolean = false): UseLibraryReturn {
  const [library, setLibrary] = useState<LibraryEntry[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  /**
   * Fetch user's library
   */
  const fetchLibrary = useCallback(async (status?: LibraryStatus) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const data = await libraryApi.getLibrary(status);
      setLibrary(data);
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to fetch library';
      setError(errorMessage);
      console.error('Fetch library error:', err);
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Add a game to library
   */
  const addToLibrary = useCallback(async (gameId: number, status: LibraryStatus = 'want_to_play') => {
    setIsLoading(true);
    setError(null);
    
    try {
      const newEntry = await libraryApi.addToLibrary(gameId, status);
      
      // Optimistically update local state
      setLibrary(prev => [...prev, newEntry]);
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to add game to library';
      setError(errorMessage);
      console.error('Add to library error:', err);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Update a library entry
   */
  const updateEntry = useCallback(async (gameId: number, updates: UpdateLibraryRequest) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const updatedEntry = await libraryApi.updateLibraryEntry(gameId, updates);
      
      // Optimistically update local state
      setLibrary(prev =>
        prev.map(entry =>
          entry.game_id === gameId ? updatedEntry : entry
        )
      );
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to update library entry';
      setError(errorMessage);
      console.error('Update library entry error:', err);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Remove a game from library
   */
  const removeFromLibrary = useCallback(async (gameId: number) => {
    setIsLoading(true);
    setError(null);
    
    try {
      await libraryApi.removeFromLibrary(gameId);
      
      // Optimistically update local state
      setLibrary(prev => prev.filter(entry => entry.game_id !== gameId));
    } catch (err: any) {
      const errorMessage = err.message || 'Failed to remove game from library';
      setError(errorMessage);
      console.error('Remove from library error:', err);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Check if a game is in the library
   */
  const isInLibrary = useCallback((gameId: number): boolean => {
    return library.some(entry => entry.game_id === gameId);
  }, [library]);

  /**
   * Get library entry by game ID
   */
  const getEntryByGameId = useCallback((gameId: number): LibraryEntry | undefined => {
    return library.find(entry => entry.game_id === gameId);
  }, [library]);

  /**
   * Clear error message
   */
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  /**
   * Auto-fetch library on mount if enabled
   */
  useEffect(() => {
    if (autoFetch) {
      fetchLibrary();
    }
  }, [autoFetch, fetchLibrary]);

  return {
    library,
    isLoading,
    error,
    fetchLibrary,
    addToLibrary,
    updateEntry,
    removeFromLibrary,
    isInLibrary,
    getEntryByGameId,
    clearError
  };
}

