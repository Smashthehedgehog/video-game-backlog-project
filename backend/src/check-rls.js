/**
 * check-rls.js
 * Check if RLS (Row Level Security) is blocking access to tables
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE;

console.log('=== RLS (Row Level Security) Check ===\n');

async function checkWithAnonKey() {
    console.log('Test 1: Using ANON KEY (what your API uses)...');
    const supabase = createClient(supabaseUrl, supabaseAnonKey);
    
    const { count, error } = await supabase
        .from('igdb_games')
        .select('*', { count: 'exact', head: true });
    
    if (error) {
        console.log('❌ Error with ANON key:', error.message);
        console.log('   Code:', error.code);
        console.log('   Details:', error.details);
        console.log('   Hint:', error.hint);
    } else {
        console.log('✓ ANON key can access igdb_games');
        console.log('  Count:', count);
    }
    console.log('');
}

async function checkWithServiceKey() {
    console.log('Test 2: Using SERVICE ROLE KEY (bypasses RLS)...');
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    
    const { count, error } = await supabase
        .from('igdb_games')
        .select('*', { count: 'exact', head: true });
    
    if (error) {
        console.log('❌ Error with SERVICE key:', error.message);
    } else {
        console.log('✓ SERVICE key can access igdb_games');
        console.log('  Count:', count);
    }
    console.log('');
}

async function checkOtherTables() {
    console.log('Test 3: Checking other tables with ANON key...');
    const supabase = createClient(supabaseUrl, supabaseAnonKey);
    
    const tables = [
        'igdb_genres',
        'igdb_platforms', 
        'igdb_companies',
        'game_genres',
        'game_platforms',
        'game_companies'
    ];
    
    for (const table of tables) {
        const { count, error } = await supabase
            .from(table)
            .select('*', { count: 'exact', head: true });
        
        if (error) {
            console.log(`  ❌ ${table}: ${error.message}`);
        } else {
            console.log(`  ✓ ${table}: ${count} rows`);
        }
    }
    console.log('');
}

async function main() {
    await checkWithAnonKey();
    await checkWithServiceKey();
    await checkOtherTables();
    
    console.log('=== Diagnosis ===\n');
    console.log('If ANON key shows 0 rows but SERVICE key shows data:');
    console.log('  → RLS is ENABLED and blocking access');
    console.log('  → Solution: Disable RLS or add policies for public read access');
    console.log('');
    console.log('If both show 0 rows:');
    console.log('  → Tables are empty, need to upload data');
    console.log('');
    console.log('To disable RLS in Supabase Dashboard:');
    console.log('  1. Go to Authentication > Policies');
    console.log('  2. Select each table (igdb_games, igdb_genres, etc.)');
    console.log('  3. Click "Disable RLS" or add a policy for SELECT');
    console.log('');
    console.log('SQL to disable RLS for all game tables:');
    console.log('  ALTER TABLE igdb_games DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE igdb_genres DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE igdb_platforms DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE igdb_companies DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE igdb_covers DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE game_genres DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE game_platforms DISABLE ROW LEVEL SECURITY;');
    console.log('  ALTER TABLE game_companies DISABLE ROW LEVEL SECURITY;');
}

main().catch(console.error);

