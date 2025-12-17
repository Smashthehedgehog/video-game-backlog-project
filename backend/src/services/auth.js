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
 * Registers a new user with email and password, and creates a user profile with display name.
 * 
 * @param {string} email - User's email address
 * @param {string} password - User's password (min 6 characters)
 * @param {string} displayName - User's display name (required, must be unique)
 * @returns {Promise<{user: Object|null, session: Object|null, error: Error|null}>}
 */
async function signUp(email, password, displayName) {
    console.log('[AUTH SERVICE] signUp called for email:', email, 'with display name:', displayName);
    
    // First, check if display name is already taken
    const { data: existingProfile, error: checkError } = await supabase
        .from('user_profiles')
        .select('display_name')
        .eq('display_name', displayName)
        .maybeSingle();

    if (checkError && checkError.code !== 'PGRST116') {
        console.error('[AUTH SERVICE] Error checking display name uniqueness:', checkError.message);
        return { user: null, session: null, error: checkError };
    }

    if (existingProfile) {
        console.log('[AUTH SERVICE] Display name already taken:', displayName);
        return { 
            user: null, 
            session: null, 
            error: { message: 'Display name is already taken', code: '23505' } 
        };
    }

    // Create the auth user
    const { data: authData, error: authError } = await supabase.auth.signUp({
        email,
        password
    });

    if (authError) {
        console.error('[AUTH SERVICE] Supabase signUp error:', authError.message);
        return { user: null, session: null, error: authError };
    }

    console.log('[AUTH SERVICE] Supabase signUp success, user ID:', authData?.user?.id);

    // Create user profile with display name
    if (authData.user) {
        console.log('[AUTH SERVICE] Creating user profile for:', authData.user.id);
        const { error: profileError } = await supabase
            .from('user_profiles')
            .insert({
                user_id: authData.user.id,
                display_name: displayName,
                email: email
            });

        if (profileError) {
            console.error('[AUTH SERVICE] Error creating user profile:', profileError.message);
            // If profile creation fails, we should delete the auth user (cleanup)
            // But Supabase doesn't allow this easily, so we return the error
            return { user: null, session: null, error: profileError };
        }

        console.log('[AUTH SERVICE] User profile created successfully');
    }

    return {
        user: authData?.user || null,
        session: authData?.session || null,
        error: null
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

