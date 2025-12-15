/**
 * games.js
 * 
 * PURPOSE:
 * Provides functions to query video game data from Supabase.
 * Handles fetching games with filters, pagination, search, and retrieving
 * individual game details with all related data (genres, platforms, covers, etc.).
 * 
 * TABLES USED:
 * - igdb_games: Main games table
 * - igdb_covers: Game cover images
 * - igdb_screenshots: Game screenshots
 * - game_genres, game_platforms, game_companies: Junction tables for relationships
 * - igdb_genres, igdb_platforms, igdb_companies: Reference tables
 * 
 * USAGE:
 * const { getGames, getGameById, searchGames } = require('./games');
 */

const { supabase } = require('./supabaseClient');

/**
 * Fetches a paginated list of games with optional filters.
 * 
 * @param {Object} options - Query options
 * @param {number} options.page - Page number (default: 1)
 * @param {number} options.limit - Items per page (default: 20)
 * @param {number} options.genreId - Filter by genre ID
 * @param {number} options.platformId - Filter by platform ID
 * @param {string} options.sortBy - Sort field (default: 'rating')
 * @param {string} options.order - Sort order 'asc' or 'desc' (default: 'desc')
 * @returns {Promise<{data: Array, count: number, error: Error|null}>}
 */
async function getGames({ page = 1, limit = 20, genreId, platformId, sortBy = 'rating', order = 'desc' } = {}) {
    const offset = (page - 1) * limit;

    let query = supabase
        .from('igdb_games')
        .select(`
            id,
            name,
            summary,
            first_release_date,
            rating,
            total_rating_count,
            igdb_covers (id, url)
        `, { count: 'exact' })
        .order(sortBy, { ascending: order === 'asc' })
        .range(offset, offset + limit - 1);

    // Apply genre filter if provided
    if (genreId) {
        const { data: gameIds } = await supabase
            .from('game_genres')
            .select('game_id')
            .eq('genre_id', genreId);
        
        if (gameIds && gameIds.length > 0) {
            query = query.in('id', gameIds.map(g => g.game_id));
        }
    }

    // Apply platform filter if provided
    if (platformId) {
        const { data: gameIds } = await supabase
            .from('game_platforms')
            .select('game_id')
            .eq('platform_id', platformId);
        
        if (gameIds && gameIds.length > 0) {
            query = query.in('id', gameIds.map(g => g.game_id));
        }
    }

    const { data, count, error } = await query;

    return { data, count, error };
}

/**
 * Fetches a single game by ID with all related data.
 * 
 * @param {number} id - The game ID
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function getGameById(id) {
    // Get the base game data with cover
    const { data: game, error: gameError } = await supabase
        .from('igdb_games')
        .select(`
            id,
            name,
            summary,
            first_release_date,
            rating,
            total_rating_count,
            updated_at,
            igdb_covers (id, url, width, height)
        `)
        .eq('id', id)
        .single();

    if (gameError || !game) {
        return { data: null, error: gameError };
    }

    // Get genres for this game
    const { data: genres } = await supabase
        .from('game_genres')
        .select('igdb_genres (id, name)')
        .eq('game_id', id);

    // Get platforms for this game
    const { data: platforms } = await supabase
        .from('game_platforms')
        .select('igdb_platforms (id, name)')
        .eq('game_id', id);

    // Get companies for this game
    const { data: companies } = await supabase
        .from('game_companies')
        .select('igdb_companies (id, name)')
        .eq('game_id', id);

    // Get screenshots for this game
    const { data: screenshots } = await supabase
        .from('game_screenshots')
        .select('igdb_screenshots (id, url, width, height)')
        .eq('game_id', id);

    // Flatten the nested structure
    game.genres = genres?.map(g => g.igdb_genres).filter(Boolean) || [];
    game.platforms = platforms?.map(p => p.igdb_platforms).filter(Boolean) || [];
    game.companies = companies?.map(c => c.igdb_companies).filter(Boolean) || [];
    game.screenshots = screenshots?.map(s => s.igdb_screenshots).filter(Boolean) || [];

    return { data: game, error: null };
}

/**
 * Searches for games by name.
 * 
 * @param {string} query - The search query
 * @param {number} limit - Maximum results to return (default: 20)
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function searchGames(query, limit = 20) {
    const { data, error } = await supabase
        .from('igdb_games')
        .select(`
            id,
            name,
            first_release_date,
            rating,
            igdb_covers (id, url)
        `)
        .ilike('name', `%${query}%`)
        .order('rating', { ascending: false })
        .limit(limit);

    return { data, error };
}

module.exports = {
    getGames,
    getGameById,
    searchGames
};

