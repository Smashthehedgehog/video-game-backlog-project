import { Link } from 'react-router-dom';
import { useAuth } from '../../../shared/hooks/useAuth';

export function HomePage() {
  const { isAuthenticated } = useAuth();

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-900 to-gray-800 text-white">
      <div className="container mx-auto px-4 py-16">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-6xl font-bold mb-6">
            Video Game Backlog
          </h1>
          <p className="text-2xl text-gray-300 mb-8">
            Track, organize, and conquer your gaming backlog
          </p>
          
          {!isAuthenticated && (
            <div className="flex justify-center space-x-4">
              <Link
                to="/register"
                className="bg-blue-600 hover:bg-blue-700 px-8 py-3 rounded-lg text-lg font-semibold transition"
              >
                Get Started
              </Link>
              <Link
                to="/login"
                className="bg-gray-700 hover:bg-gray-600 px-8 py-3 rounded-lg text-lg font-semibold transition"
              >
                Login
              </Link>
            </div>
          )}
        </div>

        {/* Features Section */}
        <div className="grid md:grid-cols-3 gap-8 mb-16">
          <div className="bg-gray-800 p-8 rounded-lg">
            <div className="text-4xl mb-4">🎮</div>
            <h3 className="text-2xl font-bold mb-3">Search Games</h3>
            <p className="text-gray-300">
              Browse thousands of games from the IGDB database
            </p>
          </div>

          <div className="bg-gray-800 p-8 rounded-lg">
            <div className="text-4xl mb-4">📚</div>
            <h3 className="text-2xl font-bold mb-3">Track Your Backlog</h3>
            <p className="text-gray-300">
              Organize games by status: Want to Play, Playing, Completed
            </p>
          </div>

          <div className="bg-gray-800 p-8 rounded-lg">
            <div className="text-4xl mb-4">⭐</div>
            <h3 className="text-2xl font-bold mb-3">Rate & Review</h3>
            <p className="text-gray-300">
              Keep notes and ratings for games you've played
            </p>
          </div>
        </div>

        {/* CTA Section */}
        <div className="text-center bg-gray-800 p-12 rounded-lg">
          <h2 className="text-3xl font-bold mb-4">
            Ready to organize your gaming life?
          </h2>
          <p className="text-xl text-gray-300 mb-6">
            Start tracking your backlog today
          </p>
          <Link
            to="/search"
            className="inline-block bg-blue-600 hover:bg-blue-700 px-8 py-3 rounded-lg text-lg font-semibold transition"
          >
            Browse Games
          </Link>
        </div>
      </div>
    </div>
  );
}

