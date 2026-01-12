/**
 * GameCard.tsx
 * 
 * PURPOSE:
 * Reusable component for displaying game information with cover image.
 * Used in search results and game lists.
 */

import type { GameListItem, GameSearchResult } from '../../../shared/types/game';
import { getCoverImageUrl } from '../../../shared/utils/imageUtils';

interface GameCardProps {
  game: GameListItem | GameSearchResult;
  onAddToLibrary?: (gameId: number) => void;
  isInLibrary?: boolean;
  isAuthenticated?: boolean;
  showAddButton?: boolean;
}

export function GameCard({ 
  game, 
  onAddToLibrary, 
  isInLibrary = false, 
  isAuthenticated = false,
  showAddButton = true 
}: GameCardProps) {
  const coverUrl = getCoverImageUrl(game.igdb_covers, 'cover_big');

  return (
    <div className="bg-dark-green-900/50 backdrop-blur-sm rounded-lg overflow-hidden hover:ring-2 hover:ring-green-500 transition border border-green-700/30">
      {/* Cover Image */}
      <div className="relative aspect-[3/4] bg-dark-green-800">
        <img
          src={coverUrl}
          alt={game.name}
          className="w-full h-full object-cover"
          loading="lazy"
          onError={(e) => {
            // Fallback if image fails to load
            const target = e.target as HTMLImageElement;
            target.src = getCoverImageUrl(null);
          }}
        />
      </div>

      {/* Game Info */}
      <div className="p-4">
        <h3 className="text-lg font-bold mb-2 line-clamp-2 min-h-[3.5rem] text-green-300">
          {game.name}
        </h3>

        {game.first_release_date && (
          <p className="text-green-200 text-sm mb-3">
            {new Date(game.first_release_date).getFullYear()}
          </p>
        )}

        {showAddButton && onAddToLibrary && (
          <button
            onClick={() => onAddToLibrary(game.id)}
            disabled={isInLibrary}
            className={`w-full py-2 rounded-lg font-semibold transition ${
              isInLibrary
                ? 'bg-dark-green-800 text-green-400 cursor-not-allowed border border-green-700'
                : 'bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 shadow-lg'
            }`}
          >
            {isInLibrary ? 'In Backlog' : 'Add to Backlog'}
          </button>
        )}
      </div>
    </div>
  );
}

