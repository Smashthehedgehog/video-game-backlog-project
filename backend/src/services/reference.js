/**
 * reference.js
 * 
 * PURPOSE:
 * Provides access to reference/lookup data: genres, platforms, and companies.
 * This data is relatively static and can be cached on the frontend.
 * Used for populating filter dropdowns, displaying game metadata, etc.
 * 
 * TABLES USED:
 * - igdb_genres: Video game genres (Action, RPG, Puzzle, etc.)
 * - igdb_platforms: Gaming platforms (PlayStation, Xbox, PC, etc.)
 * - igdb_companies: Game developers and publishers
 * 
 * USAGE:
 * const { getGenres, getPlatforms, getCompanies } = require('./reference');
 */

const { supabase } = require('./supabaseClient');

/**
 * Fetches all video game genres.
 * 
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function getGenres() {
    const { data, error } = await supabase
        .from('igdb_genres')
        .select('id, name')
        .order('name', { ascending: true });

    return { data, error };
}

/**
 * Fetches all gaming platforms.
 * 
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function getPlatforms() {
    const { data, error } = await supabase
        .from('igdb_platforms')
        .select('id, name')
        .order('name', { ascending: true });

    return { data, error };
}

/**
 * Fetches all game companies (developers/publishers).
 * 
 * @param {number} limit - Maximum companies to return (default: 100)
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function getCompanies(limit = 100) {
    const { data, error } = await supabase
        .from('igdb_companies')
        .select('id, name')
        .order('name', { ascending: true })
        .limit(limit);

    return { data, error };
}

/**
 * Fetches a single genre by ID.
 * 
 * @param {number} id - Genre ID
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function getGenreById(id) {
    const { data, error } = await supabase
        .from('igdb_genres')
        .select('id, name')
        .eq('id', id)
        .single();

    return { data, error };
}

/**
 * Fetches a single platform by ID.
 * 
 * @param {number} id - Platform ID
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function getPlatformById(id) {
    const { data, error } = await supabase
        .from('igdb_platforms')
        .select('id, name')
        .eq('id', id)
        .single();

    return { data, error };
}

/**
 * Fetches a single company by ID.
 * 
 * @param {number} id - Company ID
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function getCompanyById(id) {
    const { data, error } = await supabase
        .from('igdb_companies')
        .select('id, name')
        .eq('id', id)
        .single();

    return { data, error };
}

module.exports = {
    getGenres,
    getPlatforms,
    getCompanies,
    getGenreById,
    getPlatformById,
    getCompanyById
};

