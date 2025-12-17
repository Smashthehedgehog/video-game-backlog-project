/**
 * routes/games.js
 * 
 * PURPOSE:
 * Express router for game-related API endpoints.
 * Exposes the game service functions as RESTful API routes.
 * 
 * ENDPOINTS:
 * GET  /api/games          - Get paginated list of games with filters
 * GET  /api/games/search   - Search games by name
 * GET  /api/games/:id      - Get single game with full details
 * 
 * USAGE:
 * const gamesRouter = require('./routes/games');
 * app.use('/api/games', gamesRouter);
 */

const express = require('express');
const router = express.Router();
const { getGames, getGameById, searchGames } = require('../services/games');

/**
 * GET /api/games
 * Fetches a paginated list of games.
 * 
 * Query Parameters:
 * - page (number): Page number, default 1
 * - limit (number): Items per page, default 20
 * - genre (number): Filter by genre ID
 * - platform (number): Filter by platform ID
 * - sortBy (string): Sort field (rating, name, first_release_date)
 * - order (string): Sort order (asc, desc)
 */
router.get('/', async (req, res) => {
    try {
        const { page, limit, genre, platform, sortBy, order } = req.query;

        const { data, count, error } = await getGames({
            page: parseInt(page) || 1,
            limit: parseInt(limit) || 20,
            genreId: genre ? parseInt(genre) : undefined,
            platformId: platform ? parseInt(platform) : undefined,
            sortBy: sortBy || 'rating',
            order: order || 'desc'
        });

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({
            data,
            pagination: {
                page: parseInt(page) || 1,
                limit: parseInt(limit) || 20,
                total: count
            }
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/games/search
 * Searches for games by name.
 * 
 * Query Parameters:
 * - q (string): Search query (required)
 * - limit (number): Max results, default 20
 */
router.get('/search', async (req, res) => {
    try {
        const { q, limit } = req.query;

        if (!q || q.trim() === '') {
            return res.status(400).json({ error: 'Search query is required' });
        }

        const { data, error } = await searchGames(q, parseInt(limit) || 20);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/games/:id
 * Fetches a single game by ID with all related data.
 * 
 * URL Parameters:
 * - id (number): Game ID
 */
router.get('/:id', async (req, res) => {
    try {
        const { id } = req.params;

        const { data, error } = await getGameById(parseInt(id));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            return res.status(404).json({ error: 'Game not found' });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;

