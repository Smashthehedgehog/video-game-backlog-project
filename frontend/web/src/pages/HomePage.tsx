import { Link } from 'react-router-dom';
import { useAuth } from '../../../shared/hooks/useAuth';

export function HomePage() {
  const { isAuthenticated } = useAuth();

  return (
    <div className="min-h-screen text-white">
      <div className="container mx-auto px-4 py-16">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-6xl font-bold mb-6 bg-gradient-to-r from-green-400 to-emerald-300 bg-clip-text text-transparent">
            Welcome to Game & Logz
          </h1>
          <p className="text-2xl text-green-100 mb-8">
            Track, organize, and manage your gaming life with ease!
          </p>
          
          {!isAuthenticated && (
            <div className="flex justify-center space-x-4">
              <Link
                to="/register"
                className="bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 px-8 py-3 rounded-lg text-lg font-semibold transition shadow-lg"
              >
                Get Started
              </Link>
              <Link
                to="/login"
                className="bg-dark-green-800 hover:bg-dark-green-700 px-8 py-3 rounded-lg text-lg font-semibold transition border border-green-600"
              >
                Login
              </Link>
            </div>
          )}
        </div>

        {/* Features Section */}
        <div className="grid md:grid-cols-3 gap-8 mb-16">
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-8 rounded-lg border border-green-700/30 hover:border-green-500/50 transition">
            <div className="text-4xl mb-4">🎮</div>
            <h3 className="text-2xl font-bold mb-3 text-green-300">Search Games</h3>
            <p className="text-green-100">
              Browse thousands of games from the IGDB database
            </p>
          </div>

          <div className="bg-dark-green-900/50 backdrop-blur-sm p-8 rounded-lg border border-green-700/30 hover:border-green-500/50 transition">
            <div className="text-4xl mb-4">📚</div>
            <h3 className="text-2xl font-bold mb-3 text-green-300">Track Your Backlog</h3>
            <p className="text-green-100">
              Organize games by status: Want to Play, Playing, Completed
            </p>
          </div>

          <div className="bg-dark-green-900/50 backdrop-blur-sm p-8 rounded-lg border border-green-700/30 hover:border-green-500/50 transition">
            <div className="text-4xl mb-4">⭐</div>
            <h3 className="text-2xl font-bold mb-3 text-green-300">Rate & Review (Coming Soon)</h3>
            <p className="text-green-100">
              Keep notes and ratings for games you've played
            </p>
          </div>
        </div>

        {/* CTA Section */}
        <div className="text-center bg-dark-green-900/50 backdrop-blur-sm p-12 rounded-lg border border-green-700/30">
          <h2 className="text-3xl font-bold mb-4 text-green-300">
            Ready to organize your gaming life?
          </h2>
          <p className="text-xl text-green-100 mb-6">
            Start tracking your backlog today
          </p>
          <Link
            to="/search"
            className="inline-block bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 px-8 py-3 rounded-lg text-lg font-semibold transition shadow-lg"
          >
            Browse Games
          </Link>
        </div>
      </div>
    </div>
  );
}

