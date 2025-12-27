/**
 * games.js
 * 
 * PURPOSE:
 * Provides functions to query video game data from Supabase.
 * Handles fetching games with filters, pagination, search, and retrieving
 * individual game details with all related data (genres, platforms, companies).
 * 
 * TABLES USED:
 * - igdb_games: Main games table
 * - igdb_covers: Game cover images
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
    console.log('[getGames] Starting game fetch with parameters:', { page, limit, genreId, platformId, sortBy, order });
    
    const offset = (page - 1) * limit;
    console.log('[getGames] Calculated pagination offset:', offset, '(range:', offset, 'to', offset + limit - 1, ')');

    console.log('[getGames] Building base query for igdb_games table');
    let query = supabase
        .from('igdb_games')
        .select(`
            id,
            name,
            summary,
            first_release_date,
            rating,
            total_rating_count,
            cover_id,
            igdb_covers(id, url, width, height)
        `, { count: 'exact' })
        .order(sortBy, { ascending: order === 'asc' })
        .range(offset, offset + limit - 1);

    // Apply genre filter if provided
    if (genreId) {
        console.log('[getGames] Genre filter detected, fetching games with genre ID:', genreId);
        const { data: gameIds } = await supabase
            .from('game_genres')
            .select('game_id')
            .eq('genre_id', genreId);
        
        console.log('[getGames] Found', gameIds?.length || 0, 'games matching genre filter');
        if (gameIds && gameIds.length > 0) {
            const gameIdArray = gameIds.map(g => g.game_id);
            console.log('[getGames] Applying genre filter with game IDs:', gameIdArray.slice(0, 5), gameIdArray.length > 5 ? '...' : '');
            query = query.in('id', gameIdArray);
        } else {
            console.log('[getGames] No games found for genre filter, query will return empty results');
        }
    }

    // Apply platform filter if provided
    if (platformId) {
        console.log('[getGames] Platform filter detected, fetching games with platform ID:', platformId);
        const { data: gameIds } = await supabase
            .from('game_platforms')
            .select('game_id')
            .eq('platform_id', platformId);
        
        console.log('[getGames] Found', gameIds?.length || 0, 'games matching platform filter');
        if (gameIds && gameIds.length > 0) {
            const gameIdArray = gameIds.map(g => g.game_id);
            console.log('[getGames] Applying platform filter with game IDs:', gameIdArray.slice(0, 5), gameIdArray.length > 5 ? '...' : '');
            query = query.in('id', gameIdArray);
        } else {
            console.log('[getGames] No games found for platform filter, query will return empty results');
        }
    }

    console.log('[getGames] Executing final query to fetch games');
    const { data, count, error } = await query;

    if (error) {
        console.error('[getGames] Error fetching games:', error);
        console.error('[getGames] Error details:', JSON.stringify(error, null, 2));
        return { data, count, error };
    }

    console.log('[getGames] Query response - data:', data?.length || 0, 'items, count:', count);
    console.log('[getGames] Raw data sample:', data?.slice(0, 2));
    
    if (count === 0) {
        console.warn('[getGames] WARNING: No games found in igdb_games table!');
        console.warn('[getGames] This could mean:');
        console.warn('[getGames]   1. The table is empty');
        console.warn('[getGames]   2. RLS (Row Level Security) is blocking access');
        console.warn('[getGames]   3. The table name is incorrect');
    }
    
    console.log('[getGames] Successfully fetched', data?.length || 0, 'games out of', count, 'total matching games');
    return { data, count, error };
}

/**
 * Fetches a single game by ID with all related data.
 * 
 * @param {number} id - The game ID
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function getGameById(id) {
    console.log('[getGameById] Fetching game details for ID:', id);
    
    // Get the base game data
    console.log('[getGameById] Querying igdb_games table for base game data');
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
            cover_id
        `)
        .eq('id', id)
        .single();

    if (gameError || !game) {
        console.error('[getGameById] Failed to fetch game:', gameError?.message || 'Game not found');
        return { data: null, error: gameError };
    }

    console.log('[getGameById] Base game data retrieved:', game.name);

    // Get cover for this game
    if (game.cover_id) {
        console.log('[getGameById] Fetching cover for cover ID:', game.cover_id);
        const { data: cover } = await supabase
            .from('igdb_covers')
            .select('id, url, width, height')
            .eq('id', game.cover_id)
            .single();
        game.cover = cover || null;
        console.log('[getGameById] Cover fetched:', cover ? 'Yes' : 'No');
    } else {
        console.log('[getGameById] No cover_id for this game');
        game.cover = null;
    }

    // Get genres for this game
    console.log('[getGameById] Fetching genres for game ID:', id);
    const { data: genres } = await supabase
        .from('game_genres')
        .select('igdb_genres (id, name)')
        .eq('game_id', id);
    console.log('[getGameById] Found', genres?.length || 0, 'genres');

    // Get platforms for this game
    console.log('[getGameById] Fetching platforms for game ID:', id);
    const { data: platforms } = await supabase
        .from('game_platforms')
        .select('igdb_platforms (id, name)')
        .eq('game_id', id);
    console.log('[getGameById] Found', platforms?.length || 0, 'platforms');

    // Get companies for this game
    console.log('[getGameById] Fetching companies for game ID:', id);
    const { data: companies } = await supabase
        .from('game_companies')
        .select('igdb_companies (id, name)')
        .eq('game_id', id);
    console.log('[getGameById] Found', companies?.length || 0, 'companies');

    // Flatten the nested structure
    console.log('[getGameById] Flattening nested relationship data');
    game.genres = genres?.map(g => g.igdb_genres).filter(Boolean) || [];
    game.platforms = platforms?.map(p => p.igdb_platforms).filter(Boolean) || [];
    game.companies = companies?.map(c => c.igdb_companies).filter(Boolean) || [];

    console.log('[getGameById] Successfully compiled complete game data for:', game.name);
    console.log('[getGameById] Final data includes:', game.genres.length, 'genres,', game.platforms.length, 'platforms,', game.companies.length, 'companies, cover:', game.cover ? 'Yes' : 'No');
    
    return { data: game, error: null };
}

/**
 * Searches for games by name.
 * Returns all matching results (no limit).
 * 
 * @param {string} query - The search query
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function searchGames(query) {
    console.log('[searchGames] Starting game search with query:', query);
    
    console.log('[searchGames] Executing case-insensitive LIKE search on game names (no limit)');
    const { data, error } = await supabase
        .from('igdb_games')
        .select(`
            id,
            name,
            first_release_date,
            rating
        `)
        .ilike('name', `%${query}%`)
        .order('rating', { ascending: false });

    if (error) {
        console.error('[searchGames] Search failed with error:', error.message);
        return { data, error };
    }

    console.log('[searchGames] Search completed successfully, found', data?.length || 0, 'matching games');
    if (data && data.length > 0) {
        console.log('[searchGames] Top results:', data.slice(0, 3).map(g => g.name).join(', '));
    }
    
    return { data, error };
}

module.exports = {
    getGames,
    getGameById,
    searchGames
};

