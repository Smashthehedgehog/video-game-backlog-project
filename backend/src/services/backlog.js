/**
 * backlog.js
 * 
 * PURPOSE:
 * Manages user video game backlogs. Allows users to add games to their personal
 * backlog, update play status (want to play, playing, completed, dropped, etc.),
 * and remove games from their backlog.
 * 
 * TABLES USED:
 * - user_backlog: Stores user-game relationships with status
 *   (You'll need to create this table in Supabase)
 * 
 * TABLE SCHEMA (suggested):
 * CREATE TABLE user_backlog (
 *   id SERIAL PRIMARY KEY,
 *   user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
 *   game_id INTEGER REFERENCES igdb_games(id) ON DELETE CASCADE,
 *   status TEXT NOT NULL DEFAULT 'want_to_play',
 *   rating INTEGER,
 *   notes TEXT,
 *   created_at TIMESTAMPTZ DEFAULT NOW(),
 *   updated_at TIMESTAMPTZ DEFAULT NOW(),
 *   UNIQUE(user_id, game_id)
 * );
 * 
 * STATUS VALUES:
 * - 'want_to_play': User wants to play this game
 * - 'playing': Currently playing
 * - 'completed': Finished the game
 * - 'dropped': Stopped playing, won't finish
 * - 'on_hold': Paused, may return to it
 * 
 * USAGE:
 * const { getUserBacklog, addToBacklog, updateBacklogEntry, removeFromBacklog } = require('./backlog');
 */

const { supabase } = require('./supabaseClient');

// Valid status values for backlog entries
const VALID_STATUSES = ['want_to_play', 'playing', 'completed', 'dropped', 'on_hold'];

/**
 * Gets all games in a user's backlog with game details.
 * 
 * @param {string} userId - The user's UUID
 * @param {string} status - Optional filter by status
 * @returns {Promise<{data: Array, error: Error|null}>}
 */
async function getUserBacklog(userId, status = null) {
    let query = supabase
        .from('user_backlog')
        .select(`
            id,
            game_id,
            status,
            rating,
            notes,
            created_at,
            updated_at,
            igdb_games (
                id,
                name,
                first_release_date,
                rating,
                cover_id
            )
        `)
        .eq('user_id', userId)
        .order('updated_at', { ascending: false });

    if (status && VALID_STATUSES.includes(status)) {
        query = query.eq('status', status);
    }

    const { data, error } = await query;

    if (error) {
        return { data, error };
    }

    // Fetch covers for games that have cover_id (similar to getGames in games.js)
    if (data && data.length > 0) {
        const coverIds = data
            .filter(entry => entry.igdb_games?.cover_id)
            .map(entry => entry.igdb_games.cover_id);
        
        if (coverIds.length > 0) {
            const { data: covers, error: coversError } = await supabase
                .from('igdb_covers')
                .select('id, url, width, height')
                .in('id', coverIds);
            
            if (!coversError && covers) {
                // Create a map for quick lookup
                const coversMap = new Map(covers.map(cover => [cover.id, cover]));
                
                // Attach covers to games
                data.forEach(entry => {
                    if (entry.igdb_games?.cover_id && coversMap.has(entry.igdb_games.cover_id)) {
                        entry.igdb_games.igdb_covers = coversMap.get(entry.igdb_games.cover_id);
                    }
                });
            }
        }
    }

    return { data, error };
}

/**
 * Adds a game to the user's backlog.
 * 
 * @param {string} userId - The user's UUID
 * @param {number} gameId - The game ID to add
 * @param {string} status - Initial status (default: 'want_to_play')
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function addToBacklog(userId, gameId, status = 'want_to_play') {
    if (!VALID_STATUSES.includes(status)) {
        return { data: null, error: new Error(`Invalid status. Must be one of: ${VALID_STATUSES.join(', ')}`) };
    }

    const { data, error } = await supabase
        .from('user_backlog')
        .insert({
            user_id: userId,
            game_id: gameId,
            status: status
        })
        .select()
        .single();

    return { data, error };
}

/**
 * Updates a backlog entry (status, rating, notes).
 * 
 * @param {string} userId - The user's UUID
 * @param {number} gameId - The game ID to update
 * @param {Object} updates - Fields to update
 * @param {string} updates.status - New status
 * @param {number} updates.rating - User's rating (1-10)
 * @param {string} updates.notes - User's notes
 * @returns {Promise<{data: Object|null, error: Error|null}>}
 */
async function updateBacklogEntry(userId, gameId, updates) {
    const allowedFields = ['status', 'rating', 'notes'];
    const updateData = { updated_at: new Date().toISOString() };

    for (const field of allowedFields) {
        if (updates[field] !== undefined) {
            if (field === 'status' && !VALID_STATUSES.includes(updates[field])) {
                return { data: null, error: new Error(`Invalid status. Must be one of: ${VALID_STATUSES.join(', ')}`) };
            }
            updateData[field] = updates[field];
        }
    }

    const { data, error } = await supabase
        .from('user_backlog')
        .update(updateData)
        .eq('user_id', userId)
        .eq('game_id', gameId)
        .select(`
            id,
            game_id,
            status,
            rating,
            notes,
            created_at,
            updated_at,
            igdb_games (
                id,
                name,
                first_release_date,
                rating,
                cover_id
            )
        `)
        .single();

    if (error || !data) {
        return { data, error };
    }

    // Fetch cover if game has cover_id
    if (data.igdb_games?.cover_id) {
        const { data: cover } = await supabase
            .from('igdb_covers')
            .select('id, url, width, height')
            .eq('id', data.igdb_games.cover_id)
            .single();
        
        if (cover) {
            data.igdb_games.igdb_covers = cover;
        }
    }

    return { data, error };
}

/**
 * Removes a game from the user's backlog.
 * 
 * @param {string} userId - The user's UUID
 * @param {number} gameId - The game ID to remove
 * @returns {Promise<{error: Error|null}>}
 */
async function removeFromBacklog(userId, gameId) {
    const { error } = await supabase
        .from('user_backlog')
        .delete()
        .eq('user_id', userId)
        .eq('game_id', gameId);

    return { error };
}

/**
 * Checks if a game is in the user's backlog.
 * 
 * @param {string} userId - The user's UUID
 * @param {number} gameId - The game ID to check
 * @returns {Promise<{inBacklog: boolean, status: string|null, error: Error|null}>}
 */
async function isInBacklog(userId, gameId) {
    const { data, error } = await supabase
        .from('user_backlog')
        .select('status')
        .eq('user_id', userId)
        .eq('game_id', gameId)
        .single();

    return {
        inBacklog: !!data,
        status: data?.status || null,
        error: error?.code === 'PGRST116' ? null : error // Ignore "not found" errors
    };
}

module.exports = {
    getUserBacklog,
    addToBacklog,
    updateBacklogEntry,
    removeFromBacklog,
    isInBacklog,
    VALID_STATUSES
};

