/**
 * library.ts
 * 
 * PURPOSE:
 * API functions for user library (backlog) operations.
 * Handles adding, updating, removing, and fetching games in user's library.
 */

import { apiClient } from './client';
import { API_ENDPOINTS } from '../utils/constants';
import type { LibraryStatus } from '../utils/constants';
import type {
  LibraryEntry,
  AddToLibraryRequest,
  UpdateLibraryRequest,
  LibraryResponse,
  AddToLibraryResponse,
  UpdateLibraryResponse,
  CheckLibraryResponse
} from '../types/library';

/**
 * Get user's complete library (backlog)
 */
export async function getLibrary(status?: LibraryStatus): Promise<LibraryEntry[]> {
  const response = await apiClient.get<LibraryResponse>(
    API_ENDPOINTS.LIBRARY,
    {
      params: status ? { status } : undefined
    }
  );
  return response.data;
}

/**
 * Add a game to user's library
 */
export async function addToLibrary(gameId: number, status: LibraryStatus = 'want_to_play'): Promise<LibraryEntry> {
  const data: AddToLibraryRequest = { gameId, status };
  const response = await apiClient.post<AddToLibraryResponse>(
    API_ENDPOINTS.LIBRARY,
    data
  );
  return response.data;
}

/**
 * Update a library entry (status, rating, notes)
 */
export async function updateLibraryEntry(
  gameId: number,
  updates: UpdateLibraryRequest
): Promise<LibraryEntry> {
  const response = await apiClient.patch<UpdateLibraryResponse>(
    API_ENDPOINTS.LIBRARY_ITEM(gameId),
    updates
  );
  return response.data;
}

/**
 * Remove a game from user's library
 */
export async function removeFromLibrary(gameId: number): Promise<void> {
  await apiClient.delete(API_ENDPOINTS.LIBRARY_ITEM(gameId));
}

/**
 * Check if a game is in user's library
 */
export async function checkInLibrary(gameId: number): Promise<CheckLibraryResponse> {
  return apiClient.get<CheckLibraryResponse>(
    API_ENDPOINTS.LIBRARY_ITEM(gameId)
  );
}

/**
 * Update only the status of a library entry
 */
export async function updateStatus(gameId: number, status: LibraryStatus): Promise<LibraryEntry> {
  return updateLibraryEntry(gameId, { status });
}

/**
 * Update only the rating of a library entry
 */
export async function updateRating(gameId: number, rating: number): Promise<LibraryEntry> {
  return updateLibraryEntry(gameId, { rating });
}

/**
 * Update only the notes of a library entry
 */
export async function updateNotes(gameId: number, notes: string): Promise<LibraryEntry> {
  return updateLibraryEntry(gameId, { notes });
}

