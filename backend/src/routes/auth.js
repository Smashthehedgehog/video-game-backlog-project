/**
 * routes/auth.js
 * 
 * PURPOSE:
 * Express router for authentication API endpoints.
 * Handles user registration, login, logout, and password management.
 * Uses Supabase Auth under the hood for secure authentication.
 * 
 * ENDPOINTS:
 * POST /api/auth/signup          - Register new user
 * POST /api/auth/signin          - Login user
 * POST /api/auth/signout         - Logout user
 * GET  /api/auth/me              - Get current user
 * POST /api/auth/reset-password  - Request password reset email
 * POST /api/auth/update-password - Update password (authenticated)
 * 
 * USAGE:
 * const authRouter = require('./routes/auth');
 * app.use('/api/auth', authRouter);
 */

const express = require('express');
const router = express.Router();
const {
    signUp,
    signIn,
    signOut,
    getUser,
    resetPassword,
    updatePassword,
    getUserFromToken
} = require('../services/auth');

/**
 * POST /api/auth/signup
 * Registers a new user.
 * 
 * Request Body:
 * - email (string): User's email (required)
 * - password (string): User's password, min 6 chars (required)
 * - displayName (string): User's display name (optional)
 */
router.post('/signup', async (req, res) => {
    try {
        console.log('[AUTH ROUTE] POST /api/auth/signup - Request received');
        console.log('[AUTH ROUTE] Request body:', JSON.stringify(req.body));
        
        const { email, password, displayName } = req.body;

        if (!email || !password) {
            console.log('[AUTH ROUTE] Validation failed: Missing email or password');
            console.log('[AUTH ROUTE] Email:', email, 'Password:', password ? '[PROVIDED]' : '[MISSING]');
            return res.status(400).json({ error: 'Email and password are required' });
        }

        if (password.length < 6) {
            console.log('[AUTH ROUTE] Validation failed: Password too short');
            return res.status(400).json({ error: 'Password must be at least 6 characters' });
        }

        console.log('[AUTH ROUTE] Calling signUp service for:', email);
        const { user, session, error } = await signUp(email, password, { displayName });

        if (error) {
            console.log('[AUTH ROUTE] Sign up error:', error.message);
            return res.status(400).json({ error: error.message });
        }

        console.log('[AUTH ROUTE] Sign up successful for user:', user?.id);
        res.status(201).json({
            message: 'User created successfully. Check email for verification.',
            user,
            session
        });
    } catch (err) {
        console.error('[AUTH ROUTE] Unexpected error in signup:', err);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/auth/signin
 * Signs in an existing user.
 * 
 * Request Body:
 * - email (string): User's email (required)
 * - password (string): User's password (required)
 */
router.post('/signin', async (req, res) => {
    try {
        console.log('[AUTH ROUTE] POST /api/auth/signin - Request received');
        console.log('[AUTH ROUTE] Request body:', JSON.stringify(req.body));
        
        const { email, password } = req.body;

        if (!email || !password) {
            console.log('[AUTH ROUTE] Validation failed: Missing email or password');
            return res.status(400).json({ error: 'Email and password are required' });
        }

        console.log('[AUTH ROUTE] Calling signIn service for:', email);
        const { user, session, error } = await signIn(email, password);

        if (error) {
            console.log('[AUTH ROUTE] Sign in error:', error.message);
            return res.status(401).json({ error: 'Invalid email or password' });
        }

        console.log('[AUTH ROUTE] Sign in successful for user:', user?.id);
        res.json({ user, session });
    } catch (err) {
        console.error('[AUTH ROUTE] Unexpected error in signin:', err);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/auth/signout
 * Signs out the current user.
 */
router.post('/signout', async (req, res) => {
    try {
        console.log('[AUTH ROUTE] POST /api/auth/signout - Request received');
        
        const { error } = await signOut();

        if (error) {
            console.log('[AUTH ROUTE] Sign out error:', error.message);
            return res.status(500).json({ error: error.message });
        }

        console.log('[AUTH ROUTE] Sign out successful');
        res.json({ message: 'Signed out successfully' });
    } catch (err) {
        console.error('[AUTH ROUTE] Unexpected error in signout:', err);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/auth/me
 * Gets the currently authenticated user.
 * Requires Authorization header with Bearer token.
 */
router.get('/me', async (req, res) => {
    try {
        console.log('[AUTH ROUTE] GET /api/auth/me - Request received');
        const authHeader = req.headers.authorization;

        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            console.log('[AUTH ROUTE] No token provided in Authorization header');
            return res.status(401).json({ error: 'No token provided' });
        }

        const token = authHeader.split(' ')[1];
        console.log('[AUTH ROUTE] Token received, validating...');
        const { user, error } = await getUserFromToken(token);

        if (error || !user) {
            console.log('[AUTH ROUTE] Token validation failed:', error?.message);
            return res.status(401).json({ error: 'Invalid or expired token' });
        }

        console.log('[AUTH ROUTE] User retrieved successfully:', user?.id);
        res.json({ user });
    } catch (err) {
        console.error('[AUTH ROUTE] Unexpected error in /me:', err);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/auth/reset-password
 * Sends a password reset email.
 * 
 * Request Body:
 * - email (string): User's email (required)
 */
router.post('/reset-password', async (req, res) => {
    try {
        const { email } = req.body;

        if (!email) {
            return res.status(400).json({ error: 'Email is required' });
        }

        const { error } = await resetPassword(email);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        // Always return success to prevent email enumeration
        res.json({ message: 'If an account exists, a reset email has been sent' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/auth/update-password
 * Updates the user's password.
 * Requires Authorization header with Bearer token.
 * 
 * Request Body:
 * - password (string): New password, min 6 chars (required)
 */
router.post('/update-password', async (req, res) => {
    try {
        const authHeader = req.headers.authorization;

        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'No token provided' });
        }

        const { password } = req.body;

        if (!password || password.length < 6) {
            return res.status(400).json({ error: 'Password must be at least 6 characters' });
        }

        const { user, error } = await updatePassword(password);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ message: 'Password updated successfully', user });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;

