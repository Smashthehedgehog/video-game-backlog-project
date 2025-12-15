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
    const { data, error } = await supabase.auth.signUp({
        email,
        password,
        options: {
            data: metadata // Stored in user's raw_user_meta_data
        }
    });

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
    const { data, error } = await supabase.auth.signInWithPassword({
        email,
        password
    });

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
    const { error } = await supabase.auth.signOut();
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
    const { data: { user }, error } = await supabase.auth.getUser(token);
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

