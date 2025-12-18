/**
 * auth.ts
 * 
 * PURPOSE:
 * API functions for user authentication operations.
 * Handles signup, signin, signout, and user profile retrieval.
 */

import { apiClient } from './client';
import { API_ENDPOINTS } from '../utils/constants';
import { saveToken, saveUser, clearAuth } from '../utils/storage';
import type {
  AuthResponse,
  SignUpRequest,
  SignInRequest,
  User
} from '../types/user';

/**
 * Register a new user
 */
export async function signUp(data: SignUpRequest): Promise<AuthResponse> {
  const response = await apiClient.post<AuthResponse>(
    API_ENDPOINTS.AUTH_SIGNUP,
    data
  );
  
  // Save token and user to storage
  if (response.session?.access_token) {
    saveToken(response.session.access_token);
  }
  if (response.user) {
    saveUser(response.user);
  }
  
  return response;
}

/**
 * Sign in an existing user
 */
export async function signIn(data: SignInRequest): Promise<AuthResponse> {
  const response = await apiClient.post<AuthResponse>(
    API_ENDPOINTS.AUTH_SIGNIN,
    data
  );
  
  // Save token and user to storage
  if (response.session?.access_token) {
    saveToken(response.session.access_token);
  }
  if (response.user) {
    saveUser(response.user);
  }
  
  return response;
}

/**
 * Sign out the current user
 */
export async function signOut(): Promise<void> {
  try {
    await apiClient.post(API_ENDPOINTS.AUTH_SIGNOUT);
  } finally {
    // Always clear local storage, even if API call fails
    clearAuth();
  }
}

/**
 * Get current authenticated user
 */
export async function getCurrentUser(): Promise<User> {
  const response = await apiClient.get<{ user: User }>(API_ENDPOINTS.AUTH_ME);
  
  // Update stored user data
  if (response.user) {
    saveUser(response.user);
  }
  
  return response.user;
}

/**
 * Check if user is authenticated and token is valid
 */
export async function validateToken(): Promise<boolean> {
  try {
    await getCurrentUser();
    return true;
  } catch {
    clearAuth();
    return false;
  }
}

