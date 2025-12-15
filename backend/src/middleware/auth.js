/**
 * middleware/auth.js
 * 
 * PURPOSE:
 * Express middleware for authenticating requests using Supabase JWT tokens.
 * Validates the Authorization header and attaches the user to req.user.
 * Use this middleware on routes that require authentication.
 * 
 * HOW IT WORKS:
 * 1. Client includes JWT token in Authorization header: "Bearer <token>"
 * 2. This middleware extracts and validates the token with Supabase
 * 3. If valid, attaches user object to req.user and calls next()
 * 4. If invalid, returns 401 Unauthorized
 * 
 * USAGE:
 * const { authMiddleware } = require('./middleware/auth');
 * 
 * // Protect a single route
 * app.get('/api/protected', authMiddleware, (req, res) => { ... });
 * 
 * // Protect all routes in a router
 * app.use('/api/backlog', authMiddleware, backlogRouter);
 */

const { getUserFromToken } = require('../services/auth');

/**
 * Middleware that requires authentication.
 * Returns 401 if no token or invalid token.
 */
async function authMiddleware(req, res, next) {
    try {
        const authHeader = req.headers.authorization;

        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'Authentication required' });
        }

        const token = authHeader.split(' ')[1];
        const { user, error } = await getUserFromToken(token);

        if (error || !user) {
            return res.status(401).json({ error: 'Invalid or expired token' });
        }

        // Attach user to request object for use in route handlers
        req.user = user;
        next();
    } catch (err) {
        return res.status(401).json({ error: 'Authentication failed' });
    }
}

/**
 * Optional auth middleware.
 * Attaches user if token is valid, but allows request to continue even without auth.
 * Useful for routes that have different behavior for logged in vs anonymous users.
 */
async function optionalAuthMiddleware(req, res, next) {
    try {
        const authHeader = req.headers.authorization;

        if (authHeader && authHeader.startsWith('Bearer ')) {
            const token = authHeader.split(' ')[1];
            const { user } = await getUserFromToken(token);
            req.user = user || null;
        } else {
            req.user = null;
        }

        next();
    } catch (err) {
        req.user = null;
        next();
    }
}

module.exports = {
    authMiddleware,
    optionalAuthMiddleware
};

