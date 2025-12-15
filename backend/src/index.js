/**
 * index.js
 * 
 * PURPOSE:
 * Main entry point for the Express backend server.
 * Sets up middleware, mounts API routes, and starts the HTTP server.
 * 
 * API ROUTES:
 * /api/auth       - Authentication (signup, signin, signout)
 * /api/games      - Game queries (list, search, details)
 * /api/backlog    - User backlog management (requires auth)
 * /api/reference  - Reference data (genres, platforms, companies)
 * 
 * ENVIRONMENT VARIABLES:
 * - PORT: Server port (default: 3000)
 * - SUPABASE_URL: Your Supabase project URL
 * - SUPABASE_ANON_KEY: Your Supabase anonymous/public key
 * 
 * USAGE:
 * npm start       - Start the server
 * npm run dev     - Start with nodemon for development
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');

// Import routes
const authRouter = require('./routes/auth');
const gamesRouter = require('./routes/games');
const backlogRouter = require('./routes/backlog');
const referenceRouter = require('./routes/reference');

// Import middleware
const { authMiddleware } = require('./middleware/auth');

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// ---------- MIDDLEWARE ----------

// Enable CORS for frontend requests
app.use(cors({
    origin: process.env.FRONTEND_URL || '*',
    credentials: true
}));

// Parse JSON request bodies
app.use(express.json());

// Request logging (simple logger)
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
    next();
});

// ---------- ROUTES ----------

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Public routes (no auth required)
app.use('/api/auth', authRouter);
app.use('/api/games', gamesRouter);
app.use('/api/reference', referenceRouter);

// Protected routes (auth required)
app.use('/api/backlog', authMiddleware, backlogRouter);

// ---------- ERROR HANDLING ----------

// 404 handler
app.use((req, res) => {
    res.status(404).json({ error: 'Not found' });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({ error: 'Internal server error' });
});

// ---------- START SERVER ----------

app.listen(PORT, () => {
    console.log(`
╔════════════════════════════════════════════════╗
║   Video Game Backlog API                       ║
║   Server running on http://localhost:${PORT}      ║
╚════════════════════════════════════════════════╝

Available endpoints:
  GET  /health                    - Health check
  
  POST /api/auth/signup           - Register
  POST /api/auth/signin           - Login
  POST /api/auth/signout          - Logout
  GET  /api/auth/me               - Get current user
  
  GET  /api/games                 - List games
  GET  /api/games/search?q=       - Search games
  GET  /api/games/:id             - Game details
  
  GET  /api/backlog               - User's backlog (auth)
  POST /api/backlog               - Add to backlog (auth)
  PATCH /api/backlog/:gameId      - Update entry (auth)
  DELETE /api/backlog/:gameId     - Remove from backlog (auth)
  
  GET  /api/reference/genres      - All genres
  GET  /api/reference/platforms   - All platforms
  GET  /api/reference/companies   - All companies
    `);
});

module.exports = app;

