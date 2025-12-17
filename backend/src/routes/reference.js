/**
 * routes/reference.js
 * 
 * PURPOSE:
 * Express router for reference data API endpoints.
 * Provides access to genres, platforms, and companies.
 * This data is relatively static and good for caching.
 * 
 * ENDPOINTS:
 * GET /api/reference/genres         - Get all genres
 * GET /api/reference/genres/:id     - Get genre by ID
 * GET /api/reference/platforms      - Get all platforms
 * GET /api/reference/platforms/:id  - Get platform by ID
 * GET /api/reference/companies      - Get all companies
 * GET /api/reference/companies/:id  - Get company by ID
 * 
 * USAGE:
 * const referenceRouter = require('./routes/reference');
 * app.use('/api/reference', referenceRouter);
 */

const express = require('express');
const router = express.Router();
const {
    getGenres,
    getPlatforms,
    getCompanies,
    getGenreById,
    getPlatformById,
    getCompanyById
} = require('../services/reference');

/**
 * GET /api/reference/genres
 * Fetches all video game genres.
 */
router.get('/genres', async (req, res) => {
    try {
        const { data, error } = await getGenres();

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/reference/genres/:id
 * Fetches a single genre by ID.
 */
router.get('/genres/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { data, error } = await getGenreById(parseInt(id));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            return res.status(404).json({ error: 'Genre not found' });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/reference/platforms
 * Fetches all gaming platforms.
 */
router.get('/platforms', async (req, res) => {
    try {
        const { data, error } = await getPlatforms();

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/reference/platforms/:id
 * Fetches a single platform by ID.
 */
router.get('/platforms/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { data, error } = await getPlatformById(parseInt(id));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            return res.status(404).json({ error: 'Platform not found' });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/reference/companies
 * Fetches game companies (developers/publishers).
 * 
 * Query Parameters:
 * - limit (number): Max companies to return (default: 100)
 */
router.get('/companies', async (req, res) => {
    try {
        const { limit } = req.query;
        const { data, error } = await getCompanies(parseInt(limit) || 100);

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /api/reference/companies/:id
 * Fetches a single company by ID.
 */
router.get('/companies/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { data, error } = await getCompanyById(parseInt(id));

        if (error) {
            return res.status(500).json({ error: error.message });
        }

        if (!data) {
            return res.status(404).json({ error: 'Company not found' });
        }

        res.json({ data });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;

