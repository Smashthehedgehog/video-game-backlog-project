import { useEffect, useState } from 'react';
import { useLibrary } from '../../../shared/hooks/useLibrary';
import { LIBRARY_STATUSES, STATUS_LABELS, type LibraryStatus } from '../../../shared/utils/constants';

export function BacklogPage() {
  const { library, fetchLibrary, updateEntry, removeFromLibrary, isLoading, error } = useLibrary();
  const [filterStatus, setFilterStatus] = useState<LibraryStatus | 'all'>('all');

  useEffect(() => {
    fetchLibrary();
  }, []);

  const handleStatusChange = async (gameId: number, newStatus: LibraryStatus) => {
    try {
      await updateEntry(gameId, { status: newStatus });
    } catch (error: any) {
      alert(error.message || 'Failed to update status');
    }
  };

  const handleRemove = async (gameId: number) => {
    if (confirm('Remove this game from your backlog?')) {
      try {
        await removeFromLibrary(gameId);
      } catch (error: any) {
        alert(error.message || 'Failed to remove game');
      }
    }
  };

  const filteredLibrary = filterStatus === 'all' 
    ? library 
    : library.filter(entry => entry.status === filterStatus);

  // Calculate stats
  const stats = {
    total: library.length,
    want_to_play: library.filter(e => e.status === 'want_to_play').length,
    playing: library.filter(e => e.status === 'playing').length,
    completed: library.filter(e => e.status === 'completed').length,
    dropped: library.filter(e => e.status === 'dropped').length,
    on_hold: library.filter(e => e.status === 'on_hold').length,
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <h1 className="text-4xl font-bold mb-8">My Backlog</h1>

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4 mb-8">
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-blue-400">{stats.total}</div>
            <div className="text-sm text-gray-400">Total</div>
          </div>
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-yellow-400">{stats.want_to_play}</div>
            <div className="text-sm text-gray-400">Want to Play</div>
          </div>
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-green-400">{stats.playing}</div>
            <div className="text-sm text-gray-400">Playing</div>
          </div>
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-purple-400">{stats.completed}</div>
            <div className="text-sm text-gray-400">Completed</div>
          </div>
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-red-400">{stats.dropped}</div>
            <div className="text-sm text-gray-400">Dropped</div>
          </div>
          <div className="bg-gray-800 p-4 rounded-lg text-center">
            <div className="text-3xl font-bold text-orange-400">{stats.on_hold}</div>
            <div className="text-sm text-gray-400">On Hold</div>
          </div>
        </div>

        {/* Filter */}
        <div className="mb-6">
          <label className="block text-sm font-medium mb-2">Filter by Status:</label>
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value as LibraryStatus | 'all')}
            className="px-4 py-2 rounded-lg bg-gray-800 border border-gray-700 focus:outline-none focus:border-blue-500"
          >
            <option value="all">All Games</option>
            {LIBRARY_STATUSES.map(status => (
              <option key={status} value={status}>
                {STATUS_LABELS[status]}
              </option>
            ))}
          </select>
        </div>

        {/* Loading State */}
        {isLoading && (
          <div className="text-center py-12">
            <div className="text-xl">Loading backlog...</div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-900 border border-red-700 text-white px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        {/* Backlog List */}
        {!isLoading && filteredLibrary.length > 0 && (
          <div className="space-y-4">
            {filteredLibrary.map((entry) => (
              <div
                key={entry.id}
                className="bg-gray-800 p-6 rounded-lg hover:ring-2 hover:ring-blue-500 transition"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <h3 className="text-2xl font-bold mb-2">
                      {entry.igdb_games?.name || `Game ID: ${entry.game_id}`}
                    </h3>
                    
                    {entry.igdb_games?.rating && (
                      <div className="flex items-center mb-3">
                        <span className="text-yellow-400 mr-2">⭐</span>
                        <span className="text-gray-300">
                          {Math.round(entry.igdb_games.rating)}/100
                        </span>
                      </div>
                    )}

                    <div className="flex items-center space-x-4 mb-4">
                      <select
                        value={entry.status}
                        onChange={(e) => handleStatusChange(entry.game_id, e.target.value as LibraryStatus)}
                        className="px-3 py-1 rounded bg-gray-700 border border-gray-600 focus:outline-none focus:border-blue-500"
                      >
                        {LIBRARY_STATUSES.map(status => (
                          <option key={status} value={status}>
                            {STATUS_LABELS[status]}
                          </option>
                        ))}
                      </select>

                      {entry.rating && (
                        <span className="text-gray-400">
                          Your Rating: {entry.rating}/10
                        </span>
                      )}
                    </div>

                    {entry.notes && (
                      <p className="text-gray-400 italic">"{entry.notes}"</p>
                    )}
                  </div>

                  <button
                    onClick={() => handleRemove(entry.game_id)}
                    className="ml-4 text-red-400 hover:text-red-300 transition"
                  >
                    Remove
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Empty State */}
        {!isLoading && filteredLibrary.length === 0 && (
          <div className="text-center py-12">
            <p className="text-xl text-gray-400 mb-4">
              {filterStatus === 'all' 
                ? 'Your backlog is empty' 
                : `No games with status: ${STATUS_LABELS[filterStatus as LibraryStatus]}`
              }
            </p>
            <a href="/search" className="text-blue-400 hover:text-blue-300">
              Browse games to add to your backlog
            </a>
          </div>
        )}
      </div>
    </div>
  );
}

