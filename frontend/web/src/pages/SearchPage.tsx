import { useState, useEffect } from 'react';
import { useGames } from '../../../shared/hooks/useGames';
import { useLibrary } from '../../../shared/hooks/useLibrary';
import { useAuth } from '../../../shared/hooks/useAuth';
import type { GameListItem } from '../../../shared/types/game';

export function SearchPage() {
  const { games, fetchGames, searchGames, isLoading, error } = useGames();
  const { addToLibrary, isInLibrary } = useLibrary();
  const { isAuthenticated } = useAuth();
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);

  useEffect(() => {
    // Load initial games on mount
    fetchGames({ page: 1, limit: 20, sortBy: 'rating', order: 'desc' });
  }, []);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      const results = await searchGames(searchQuery);
      setSearchResults(results);
    }
  };

  const handleAddToLibrary = async (gameId: number) => {
    if (!isAuthenticated) {
      alert('Please login to add games to your backlog');
      return;
    }

    try {
      await addToLibrary(gameId, 'want_to_play');
      alert('Game added to backlog!');
    } catch (error: any) {
      alert(error.message || 'Failed to add game');
    }
  };

  const displayGames = searchResults.length > 0 ? searchResults : games;

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="container mx-auto px-4 py-8">
        {/* Search Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-6">Search Games</h1>
          
          {/* Search Form */}
          <form onSubmit={handleSearch} className="flex gap-4">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search for games..."
              className="flex-1 px-4 py-3 rounded-lg bg-gray-800 text-white border border-gray-700 focus:outline-none focus:border-blue-500"
            />
            <button
              type="submit"
              className="bg-blue-600 hover:bg-blue-700 px-8 py-3 rounded-lg font-semibold transition"
            >
              Search
            </button>
          </form>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="text-center py-12">
            <div className="text-xl">Loading games...</div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-900 border border-red-700 text-white px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        {/* Games Grid */}
        {!isLoading && displayGames.length > 0 && (
          <div className="grid md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {displayGames.map((game) => (
              <div
                key={game.id}
                className="bg-gray-800 rounded-lg overflow-hidden hover:ring-2 hover:ring-blue-500 transition"
              >
                <div className="p-6">
                  <h3 className="text-xl font-bold mb-2 line-clamp-2">
                    {game.name}
                  </h3>
                  
                  {game.rating && (
                    <div className="flex items-center mb-3">
                      <span className="text-yellow-400 mr-2">⭐</span>
                      <span className="text-gray-300">
                        {Math.round(game.rating)}/100
                      </span>
                    </div>
                  )}

                  {game.first_release_date && (
                    <p className="text-gray-400 text-sm mb-4">
                      {new Date(game.first_release_date).getFullYear()}
                    </p>
                  )}

                  {isAuthenticated && (
                    <button
                      onClick={() => handleAddToLibrary(game.id)}
                      disabled={isInLibrary(game.id)}
                      className={`w-full py-2 rounded-lg font-semibold transition ${
                        isInLibrary(game.id)
                          ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                          : 'bg-blue-600 hover:bg-blue-700'
                      }`}
                    >
                      {isInLibrary(game.id) ? 'In Backlog' : 'Add to Backlog'}
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* No Results */}
        {!isLoading && displayGames.length === 0 && (
          <div className="text-center py-12">
            <p className="text-xl text-gray-400">No games found</p>
          </div>
        )}
      </div>
    </div>
  );
}

