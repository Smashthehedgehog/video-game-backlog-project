/**
 * storage.ts
 * 
 * PURPOSE:
 * Abstraction layer for browser storage (localStorage).
 * Provides type-safe methods for storing and retrieving auth tokens and user data.
 * Makes it easy to switch storage mechanisms (e.g., sessionStorage, cookies) later.
 */

import { TOKEN_KEY, USER_KEY } from './constants';
import type { User } from '../types/user';

/**
 * Save authentication token to localStorage
 */
export function saveToken(token: string): void {
  try {
    localStorage.setItem(TOKEN_KEY, token);
  } catch (error) {
    console.error('Failed to save token:', error);
  }
}

/**
 * Retrieve authentication token from localStorage
 */
export function getToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch (error) {
    console.error('Failed to get token:', error);
    return null;
  }
}

/**
 * Remove authentication token from localStorage
 */
export function removeToken(): void {
  try {
    localStorage.removeItem(TOKEN_KEY);
  } catch (error) {
    console.error('Failed to remove token:', error);
  }
}

/**
 * Check if user is authenticated (has valid token)
 */
export function isAuthenticated(): boolean {
  return !!getToken();
}

/**
 * Save user data to localStorage
 */
export function saveUser(user: User): void {
  try {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  } catch (error) {
    console.error('Failed to save user:', error);
  }
}

/**
 * Retrieve user data from localStorage
 */
export function getUser(): User | null {
  try {
    const userJson = localStorage.getItem(USER_KEY);
    return userJson ? JSON.parse(userJson) : null;
  } catch (error) {
    console.error('Failed to get user:', error);
    return null;
  }
}

/**
 * Remove user data from localStorage
 */
export function removeUser(): void {
  try {
    localStorage.removeItem(USER_KEY);
  } catch (error) {
    console.error('Failed to remove user:', error);
  }
}

/**
 * Clear all auth-related data from storage
 */
export function clearAuth(): void {
  removeToken();
  removeUser();
}

