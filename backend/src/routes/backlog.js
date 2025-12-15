/**
 * routes/backlog.js
 * 
 * PURPOSE:
 * Express router for user backlog API endpoints.
 * All routes require authentication - user must be signed in.
 * Manages the user's personal game backlog (add, update, remove games).
 * 
 * ENDPOINTS:
 * GET    /api/backlog           - Get user's backlog
 * POST   /api/backlog           - Add game to backlog
 * PATCH  /api/backlog/:gameId   - Update backlog entry
 * DELETE /api/backlog/:gameId   - Remove from backlog
 * GET    /api/backlog/:gameId   - Check if game is in backlog
 * 
 * USAGE:
 * const backlogRouter = require('./routes/backlog');
 * app.use('/api/backlog', authMiddleware, backlogRouter);
 */

const express = require('express');
const router = express.Router();
const {
    getUserBacklog,
    addToBacklog,
    updateBacklogEntry,
    removeFromBacklog,
    isInBacklog,
    VALID_STATUSES
} = require('../services/backlog');

/**
 * GET /api/backlog
 * Gets all games in the user's backlog.
 * 
 * Query Parameters:
 * - status (string): Filter by status (want_to_play, playing, completed, etc.)
 */
router.get('/', async (req, res) => {
    try {
        const userId = req.user.id; // Set by auth middleware
        const { status } = req.query;

        const { data, error } = await getUserBacklog(userId, status);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /api/backlog
 * Adds a game to the user's backlog.
 * 
 * Request Body:
 * - gameId (number): Game ID to add (required)
 * - status (string): Initial status (default: 'want_to_play')
 */
router.post('/', async (req, res) => {
    try {
        const userId = req.user.id;
        const { gameId, status } = req.body;

        if (!gameId) {
            return res.status(400).json({ error: 'gameId is required' });
        }

        const { data, error } = await addToBacklog(userId, gameId, status);

        if (error) {
            // Handle duplicate entry
            if (error.code === '23505') {
                return res.status(409).json({ error: 'Game already in backlog' });
            }
            return res.status(500).json({ error: error.message });
        }

        res.status(201).json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * PATCH /api/backlog/:gameId
 * Updates a backlog entry (status, rating, notes).
 * 
 * URL Parameters:
 * - gameId (number): Game ID to update
 * 
 * Request Body:
 * - status (string): New status
 * - rating (number): User's rating (1-10)
 * - notes (string): User's notes
 */
router.patch('/:gameId', async (req, res) => {
    try {
        const userId = req.user.id;
        const { gameId } = req.params;
        const updates = req.body;

        const { data, error } = await updateBacklogEntry(userId, parseInt(gameId), updates);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            return res.status(404).json({ error: 'Backlog entry not found' });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * DELETE /api/backlog/:gameId
 * Removes a game from the user's backlog.
 * 
 * URL Parameters:
 * - gameId (number): Game ID to remove
 */
router.delete('/:gameId', async (req, res) => {
    try {
        const userId = req.user.id;
        const { gameId } = req.params;

        const { error } = await removeFromBacklog(userId, parseInt(gameId));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.status(204).send();
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/backlog/:gameId
 * Checks if a game is in the user's backlog.
 * 
 * URL Parameters:
 * - gameId (number): Game ID to check
 */
router.get('/:gameId', async (req, res) => {
    try {
        const userId = req.user.id;
        const { gameId } = req.params;

        const { inBacklog, status, error } = await isInBacklog(userId, parseInt(gameId));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ inBacklog, status });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/backlog/statuses
 * Returns valid status values for backlog entries.
 */
router.get('/meta/statuses', (req, res) => {
    res.json({ statuses: VALID_STATUSES });
});

module.exports = router;

