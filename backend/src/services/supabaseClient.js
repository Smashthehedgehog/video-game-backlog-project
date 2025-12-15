/**
 * supabaseClient.js
 * 
 * PURPOSE:
 * Initializes and exports a configured Supabase client instance.
 * This is the single source of truth for database connections throughout the app.
 * All other service files import this client to interact with Supabase.
 * 
 * USAGE:
 * const { supabase } = require('./supabaseClient');
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables');
}

const supabase = createClient(supabaseUrl, supabaseAnonKey);

module.exports = { supabase };

