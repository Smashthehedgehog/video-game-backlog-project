/**
 * auth.js
 * 
 * PURPOSE:
 * Handles user authentication using Supabase Auth.
 * Provides functions for sign up, sign in, sign out, and session management.
 * Supabase Auth handles password hashing, JWT tokens, and session storage.
 * 
 * AUTHENTICATION FLOW:
 * 1. User signs up with email/password → Supabase creates user in auth.users
 * 2. User signs in → Supabase returns access token and refresh token
 * 3. Access token is sent with each API request in Authorization header
 * 4. Backend validates token to identify the user
 * 
 * USAGE:
 * const { signUp, signIn, signOut, getUser } = require('./auth');
 */

const { supabase } = require('./supabaseClient');

/**
 * Registers a new user with email and password.
 * 
 * @param {string} email - User's email address
 * @param {string} password - User's password (min 6 characters)
 * @param {Object} metadata - Optional user metadata (display name, etc.)
 * @returns {Promise<{user: Object|null, session: Object|null, error: Error|null}>}
 */
async function signUp(email, password, metadata = {}) {
    console.log('[AUTH SERVICE] signUp called for email:', email);
    console.log('[AUTH SERVICE] Metadata:', JSON.stringify(metadata));
    
    const { data, error } = await supabase.auth.signUp({
        email,
        password,
        options: {
            data: metadata // Stored in user's raw_user_meta_data
        }
    });

    if (error) {
        console.error('[AUTH SERVICE] Supabase signUp error:', error.message);
    } else {
        console.log('[AUTH SERVICE] Supabase signUp success, user ID:', data?.user?.id);
    }

    return {
        user: data?.user || null,
        session: data?.session || null,
        error
    };
}

/**
 * Signs in a user with email and password.
 * 
 * @param {string} email - User's email address
 * @param {string} password - User's password
 * @returns {Promise<{user: Object|null, session: Object|null, error: Error|null}>}
 */
async function signIn(email, password) {
    console.log('[AUTH SERVICE] signIn called for email:', email);
    
    const { data, error } = await supabase.auth.signInWithPassword({
        email,
        password
    });

    if (error) {
        console.error('[AUTH SERVICE] Supabase signIn error:', error.message);
    } else {
        console.log('[AUTH SERVICE] Supabase signIn success, user ID:', data?.user?.id);
    }

    return {
        user: data?.user || null,
        session: data?.session || null,
        error
    };
}

/**
 * Signs out the current user.
 * 
 * @returns {Promise<{error: Error|null}>}
 */
async function signOut() {
    console.log('[AUTH SERVICE] signOut called');
    const { error } = await supabase.auth.signOut();
    
    if (error) {
        console.error('[AUTH SERVICE] Supabase signOut error:', error.message);
    } else {
        console.log('[AUTH SERVICE] Supabase signOut success');
    }
    
    return { error };
}

/**
 * Gets the currently authenticated user from the session.
 * 
 * @returns {Promise<{user: Object|null, error: Error|null}>}
 */
async function getUser() {
    const { data: { user }, error } = await supabase.auth.getUser();
    return { user, error };
}

/**
 * Gets the current session (includes access token).
 * 
 * @returns {Promise<{session: Object|null, error: Error|null}>}
 */
async function getSession() {
    const { data: { session }, error } = await supabase.auth.getSession();
    return { session, error };
}

/**
 * Sends a password reset email to the user.
 * 
 * @param {string} email - User's email address
 * @returns {Promise<{error: Error|null}>}
 */
async function resetPassword(email) {
    const { error } = await supabase.auth.resetPasswordForEmail(email);
    return { error };
}

/**
 * Updates the user's password (requires user to be signed in).
 * 
 * @param {string} newPassword - The new password
 * @returns {Promise<{user: Object|null, error: Error|null}>}
 */
async function updatePassword(newPassword) {
    const { data, error } = await supabase.auth.updateUser({
        password: newPassword
    });

    return { user: data?.user || null, error };
}

/**
 * Middleware helper: Extracts user from JWT token in Authorization header.
 * Use this in Express routes to authenticate requests.
 * 
 * @param {string} token - JWT access token from Authorization header
 * @returns {Promise<{user: Object|null, error: Error|null}>}
 */
async function getUserFromToken(token) {
    console.log('[AUTH SERVICE] getUserFromToken called');
    console.log('[AUTH SERVICE] Token preview:', token?.substring(0, 20) + '...');
    
    const { data: { user }, error } = await supabase.auth.getUser(token);
    
    if (error) {
        console.error('[AUTH SERVICE] Token validation error:', error.message);
    } else if (user) {
        console.log('[AUTH SERVICE] Token valid, user ID:', user.id);
    } else {
        console.log('[AUTH SERVICE] Token validation returned no user');
    }
    
    return { user, error };
}

module.exports = {
    signUp,
    signIn,
    signOut,
    getUser,
    getSession,
    resetPassword,
    updatePassword,
    getUserFromToken
};

