import { useState, useEffect } from 'react';
import { useGames } from '../../../shared/hooks/useGames';
import { useLibrary } from '../../../shared/hooks/useLibrary';
import { useAuth } from '../../../shared/hooks/useAuth';
import { GameCard } from '../components/GameCard';

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
    <div className="min-h-screen text-white">
      <div className="container mx-auto px-4 py-8">
        {/* Search Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-6 text-green-300">Search Games</h1>
          
          {/* Search Form */}
          <form onSubmit={handleSearch} className="flex gap-4">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search for games..."
              className="flex-1 px-4 py-3 rounded-lg bg-dark-green-900/50 text-white border border-green-700/50 focus:outline-none focus:border-green-500 placeholder-green-300/50"
            />
            <button
              type="submit"
              className="bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 px-8 py-3 rounded-lg font-semibold transition shadow-lg"
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
                className="bg-dark-green-800 hover:bg-dark-green-700 px-6 py-3 rounded-lg font-semibold transition border border-green-600"
              >
                Clear
              </button>
            )}
          </form>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="text-center py-12">
            <div className="text-xl text-green-300">Loading games...</div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-900/50 border border-red-700 text-white px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        {/* Results Info */}
        {!isLoading && paginatedSearchResults.length > 0 && (
          <div className="mb-4 text-green-300">
            Showing {((currentPage - 1) * ITEMS_PER_PAGE) + 1} - {Math.min(currentPage * ITEMS_PER_PAGE, isSearchMode ? searchResults.length : (pagination?.total || 0))} of {isSearchMode ? searchResults.length : (pagination?.total || 0)} games
          </div>
        )}

        {/* Games Grid */}
        {!isLoading && paginatedSearchResults.length > 0 && (
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 2xl:grid-cols-8 gap-4">
            {paginatedSearchResults.map((game) => (
              <GameCard
                key={game.id}
                game={game}
                onAddToLibrary={handleAddToLibrary}
                isInLibrary={isInLibrary(game.id)}
                isAuthenticated={isAuthenticated}
                showAddButton={isAuthenticated}
              />
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

