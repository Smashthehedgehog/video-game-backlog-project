/**
 * useAuth.ts
 * 
 * PURPOSE:
 * React hook for managing authentication state and operations.
 * Provides user state, login/logout functions, and loading/error states.
 * Automatically persists auth state and validates tokens on mount.
 */

import { useState, useEffect, useCallback } from 'react';
import * as authApi from '../api/auth';
import { getUser, isAuthenticated, clearAuth } from '../utils/storage';
import type { User, SignUpRequest, SignInRequest } from '../types/user';

interface UseAuthReturn {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  error: string | null;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string, displayName: string) => Promise<void>;
  logout: () => Promise<void>;
  clearError: () => void;
}

export function useAuth(): UseAuthReturn {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  /**
   * Initialize auth state on mount
   */
  useEffect(() => {
    const initAuth = async () => {
      try {
        // Check if user is authenticated
        if (isAuthenticated()) {
          // Try to get stored user
          const storedUser = getUser();
          if (storedUser) {
            setUser(storedUser);
            
            // Validate token in background
            try {
              const currentUser = await authApi.getCurrentUser();
              setUser(currentUser);
            } catch {
              // Token is invalid, clear auth
              clearAuth();
              setUser(null);
            }
          }
        }
      } catch (err) {
        console.error('Auth initialization error:', err);
        clearAuth();
      } finally {
        setIsLoading(false);
      }
    };

    initAuth();
  }, []);

  /**
   * Register a new user
   */
  const register = useCallback(async (email: string, password: string, displayName: string) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const data: SignUpRequest = { email, password, displayName };
      const response = await authApi.signUp(data);
      setUser(response.user);
    } catch (err: any) {
      const errorMessage = err.message || 'Registration failed';
      setError(errorMessage);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Sign in an existing user
   */
  const login = useCallback(async (email: string, password: string) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const data: SignInRequest = { email, password };
      const response = await authApi.signIn(data);
      setUser(response.user);
    } catch (err: any) {
      const errorMessage = err.message || 'Login failed';
      setError(errorMessage);
      throw err;
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Sign out the current user
   */
  const logout = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      await authApi.signOut();
    } catch (err: any) {
      console.error('Logout error:', err);
      // Continue with logout even if API call fails
    } finally {
      setUser(null);
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
    user,
    isLoading,
    isAuthenticated: !!user,
    error,
    login,
    register,
    logout,
    clearError
  };
}

