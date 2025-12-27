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
    <div className="bg-gray-800 rounded-lg overflow-hidden hover:ring-2 hover:ring-blue-500 transition">
      {/* Cover Image */}
      <div className="relative aspect-[3/4] bg-gray-700">
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
        
        {/* Rating Badge */}
        {game.rating && (
          <div className="absolute top-2 right-2 bg-black bg-opacity-75 px-2 py-1 rounded-lg flex items-center">
            <span className="text-yellow-400 text-sm mr-1">⭐</span>
            <span className="text-white text-sm font-semibold">
              {Math.round(game.rating)}
            </span>
          </div>
        )}
      </div>

      {/* Game Info */}
      <div className="p-4">
        <h3 className="text-lg font-bold mb-2 line-clamp-2 min-h-[3.5rem]">
          {game.name}
        </h3>

        {game.first_release_date && (
          <p className="text-gray-400 text-sm mb-3">
            {new Date(game.first_release_date).getFullYear()}
          </p>
        )}

        {showAddButton && isAuthenticated && onAddToLibrary && (
          <button
            onClick={() => onAddToLibrary(game.id)}
            disabled={isInLibrary}
            className={`w-full py-2 rounded-lg font-semibold transition ${
              isInLibrary
                ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                : 'bg-blue-600 hover:bg-blue-700'
            }`}
          >
            {isInLibrary ? 'In Backlog' : 'Add to Backlog'}
          </button>
        )}
      </div>
    </div>
  );
}

