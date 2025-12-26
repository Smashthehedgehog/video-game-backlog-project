import { useState, useEffect } from 'react';
import { useGames } from '../../../shared/hooks/useGames';
import { useLibrary } from '../../../shared/hooks/useLibrary';
import { useAuth } from '../../../shared/hooks/useAuth';
import type { GameListItem } from '../../../shared/types/game';

export function SearchPage() {
  const { games, pagination, fetchGames, searchGames, isLoading, error } = useGames();
  const { addToLibrary, isInLibrary } = useLibrary();
  const { isAuthenticated } = useAuth();
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [isSearchMode, setIsSearchMode] = useState(false);
  const ITEMS_PER_PAGE = 50;

  useEffect(() => {
    // Load initial games on mount
    fetchGames({ page: 1, limit: ITEMS_PER_PAGE, sortBy: 'rating', order: 'desc' });
  }, []);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      setIsSearchMode(true);
      setCurrentPage(1);
      const results = await searchGames(searchQuery);
      setSearchResults(results);
    } else {
      // If search is cleared, go back to browse mode
      setIsSearchMode(false);
      setCurrentPage(1);
      fetchGames({ page: 1, limit: ITEMS_PER_PAGE, sortBy: 'rating', order: 'desc' });
    }
  };

  const handlePageChange = (newPage: number) => {
    setCurrentPage(newPage);
    window.scrollTo({ top: 0, behavior: 'smooth' });
    
    if (!isSearchMode) {
      fetchGames({ page: newPage, limit: ITEMS_PER_PAGE, sortBy: 'rating', order: 'desc' });
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

  // For search mode, paginate the search results manually
  const displayGames = isSearchMode ? searchResults : games;
  const paginatedSearchResults = isSearchMode 
    ? searchResults.slice((currentPage - 1) * ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE)
    : displayGames;
  
  const totalPages = isSearchMode 
    ? Math.ceil(searchResults.length / ITEMS_PER_PAGE)
    : pagination ? Math.ceil(pagination.total / ITEMS_PER_PAGE) : 1;

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
            {isSearchMode && (
              <button
                type="button"
                onClick={() => {
                  setSearchQuery('');
                  setIsSearchMode(false);
                  setCurrentPage(1);
                  fetchGames({ page: 1, limit: ITEMS_PER_PAGE, sortBy: 'rating', order: 'desc' });
                }}
                className="bg-gray-700 hover:bg-gray-600 px-6 py-3 rounded-lg font-semibold transition"
              >
                Clear
              </button>
            )}
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

        {/* Results Info */}
        {!isLoading && paginatedSearchResults.length > 0 && (
          <div className="mb-4 text-gray-400">
            Showing {((currentPage - 1) * ITEMS_PER_PAGE) + 1} - {Math.min(currentPage * ITEMS_PER_PAGE, isSearchMode ? searchResults.length : (pagination?.total || 0))} of {isSearchMode ? searchResults.length : (pagination?.total || 0)} games
          </div>
        )}

        {/* Games Grid */}
        {!isLoading && paginatedSearchResults.length > 0 && (
          <div className="grid md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {paginatedSearchResults.map((game) => (
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

        {/* Pagination Controls */}
        {!isLoading && paginatedSearchResults.length > 0 && totalPages > 1 && (
          <div className="mt-8 flex justify-center items-center gap-2">
            <button
              onClick={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1}
              className={`px-4 py-2 rounded-lg font-semibold transition ${
                currentPage === 1
                  ? 'bg-gray-800 text-gray-600 cursor-not-allowed'
                  : 'bg-gray-700 hover:bg-gray-600 text-white'
              }`}
            >
              Previous
            </button>

            <div className="flex gap-2">
              {/* First page */}
              {currentPage > 3 && (
                <>
                  <button
                    onClick={() => handlePageChange(1)}
                    className="px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 text-white transition"
                  >
                    1
                  </button>
                  {currentPage > 4 && <span className="px-2 py-2 text-gray-500">...</span>}
                </>
              )}

              {/* Page numbers around current page */}
              {Array.from({ length: totalPages }, (_, i) => i + 1)
                .filter(page => page >= currentPage - 2 && page <= currentPage + 2)
                .map(page => (
                  <button
                    key={page}
                    onClick={() => handlePageChange(page)}
                    className={`px-4 py-2 rounded-lg font-semibold transition ${
                      page === currentPage
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-700 hover:bg-gray-600 text-white'
                    }`}
                  >
                    {page}
                  </button>
                ))}

              {/* Last page */}
              {currentPage < totalPages - 2 && (
                <>
                  {currentPage < totalPages - 3 && <span className="px-2 py-2 text-gray-500">...</span>}
                  <button
                    onClick={() => handlePageChange(totalPages)}
                    className="px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 text-white transition"
                  >
                    {totalPages}
                  </button>
                </>
              )}
            </div>

            <button
              onClick={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages}
              className={`px-4 py-2 rounded-lg font-semibold transition ${
                currentPage === totalPages
                  ? 'bg-gray-800 text-gray-600 cursor-not-allowed'
                  : 'bg-gray-700 hover:bg-gray-600 text-white'
              }`}
            >
              Next
            </button>
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

