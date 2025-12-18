/**
 * user.ts
 * 
 * PURPOSE:
 * TypeScript type definitions for user-related data structures.
 * Ensures type safety for user authentication and profile data.
 */

/**
 * User object returned from the backend
 */
export interface User {
  id: string;
  email: string;
  created_at: string;
  // Add other user fields as needed
}

/**
 * User profile with display name
 */
export interface UserProfile {
  id: number;
  user_id: string;
  display_name: string;
  email: string;
  created_at: string;
  updated_at: string;
}

/**
 * Session data returned after authentication
 */
export interface Session {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  expires_at?: number;
  token_type: string;
  user: User;
}

/**
 * Complete authentication response from signup/signin
 */
export interface AuthResponse {
  user: User;
  session: Session;
  message?: string;
}

/**
 * Sign up request payload
 */
export interface SignUpRequest {
  email: string;
  password: string;
  displayName: string;
}

/**
 * Sign in request payload
 */
export interface SignInRequest {
  email: string;
  password: string;
}

/**
 * Auth error response
 */
export interface AuthError {
  error: string;
  message?: string;
}

