import { useEffect, useState } from 'react';
import { useLibrary } from '../../../shared/hooks/useLibrary';
import { LIBRARY_STATUSES, STATUS_LABELS, type LibraryStatus } from '../../../shared/utils/constants';
import { getCoverImageUrl } from '../../../shared/utils/imageUtils';

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
    <div className="min-h-screen text-white">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <h1 className="text-4xl font-bold mb-8 text-green-300">My Backlog</h1>

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4 mb-8">
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-green-400">{stats.total}</div>
            <div className="text-sm text-green-200">Total</div>
          </div>
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-yellow-400">{stats.want_to_play}</div>
            <div className="text-sm text-green-200">Want to Play</div>
          </div>
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-emerald-400">{stats.playing}</div>
            <div className="text-sm text-green-200">Playing</div>
          </div>
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-green-300">{stats.completed}</div>
            <div className="text-sm text-green-200">Completed</div>
          </div>
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-red-400">{stats.dropped}</div>
            <div className="text-sm text-green-200">Dropped</div>
          </div>
          <div className="bg-dark-green-900/50 backdrop-blur-sm p-4 rounded-lg text-center border border-green-700/30">
            <div className="text-3xl font-bold text-orange-400">{stats.on_hold}</div>
            <div className="text-sm text-green-200">On Hold</div>
          </div>
        </div>

        {/* Filter */}
        <div className="mb-6">
          <label className="block text-sm font-medium mb-2 text-green-300">Filter by Status:</label>
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value as LibraryStatus | 'all')}
            className="px-4 py-2 rounded-lg bg-dark-green-900/50 border border-green-700/50 focus:outline-none focus:border-green-500 text-white"
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
            <div className="text-xl text-green-300">Loading backlog...</div>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-900/50 border border-red-700 text-white px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        {/* Backlog List */}
        {!isLoading && filteredLibrary.length > 0 && (
          <div className="space-y-4">
            {filteredLibrary.map((entry) => {
              const coverUrl = getCoverImageUrl(entry.igdb_games?.igdb_covers, 'cover_small');
              
              return (
                <div
                  key={entry.id}
                  className="bg-dark-green-900/50 backdrop-blur-sm rounded-lg hover:ring-2 hover:ring-green-500 transition overflow-hidden border border-green-700/30"
                >
                  <div className="flex">
                    {/* Cover Image */}
                    <div className="flex-shrink-0 w-24 h-32 bg-dark-green-800">
                      <img
                        src={coverUrl}
                        alt={entry.igdb_games?.name || 'Game cover'}
                        className="w-full h-full object-cover"
                        loading="lazy"
                        onError={(e) => {
                          const target = e.target as HTMLImageElement;
                          target.src = getCoverImageUrl(null);
                        }}
                      />
                    </div>

                    {/* Game Info */}
                    <div className="flex-1 p-6 flex items-start justify-between">
                      <div className="flex-1">
                        <h3 className="text-2xl font-bold mb-2 text-green-300">
                          {entry.igdb_games?.name || `Game ID: ${entry.game_id}`}
                        </h3>

                        <div className="flex items-center space-x-4 mb-4">
                          <select
                            value={entry.status}
                            onChange={(e) => handleStatusChange(entry.game_id, e.target.value as LibraryStatus)}
                            className="px-3 py-1 rounded bg-dark-green-800 border border-green-600 focus:outline-none focus:border-green-500 text-white"
                          >
                            {LIBRARY_STATUSES.map(status => (
                              <option key={status} value={status}>
                                {STATUS_LABELS[status]}
                              </option>
                            ))}
                          </select>

                          {entry.rating && (
                            <span className="text-green-200">
                              Your Rating: {entry.rating}/10
                            </span>
                          )}
                        </div>

                        {entry.notes && (
                          <p className="text-green-200 italic">"{entry.notes}"</p>
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
                </div>
              );
            })}
          </div>
        )}

        {/* Empty State */}
        {!isLoading && filteredLibrary.length === 0 && (
          <div className="text-center py-12">
            <p className="text-xl text-green-300 mb-4">
              {filterStatus === 'all' 
                ? 'Your backlog is empty' 
                : `No games with status: ${STATUS_LABELS[filterStatus as LibraryStatus]}`
              }
            </p>
            <a href="/search" className="text-green-400 hover:text-green-300">
              Browse games to add to your backlog
            </a>
          </div>
        )}
      </div>
    </div>
  );
}

