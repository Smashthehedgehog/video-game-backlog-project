/**
 * imageUtils.ts
 * 
 * PURPOSE:
 * Utility functions for handling IGDB image URLs and transformations.
 */

import type { Cover } from '../types/game';

/**
 * IGDB image size options
 */
export type IGDBImageSize = 
  | 'cover_small'      // 90x128
  | 'screenshot_med'   // 569x320
  | 'cover_big'        // 264x352
  | 'logo_med'         // 284x160
  | 'screenshot_big'   // 889x500
  | 'screenshot_huge'  // 1280x720
  | '720p'             // 1280x720
  | '1080p';           // 1920x1080

/**
 * Get the full IGDB image URL with specified size
 * 
 * @param cover - The cover object from the API
 * @param size - The desired image size (default: 'cover_big')
 * @returns Full HTTPS URL to the image, or null if no cover
 */
export function getIGDBImageUrl(
  cover: Cover | null | undefined,
  size: IGDBImageSize = 'cover_big'
): string | null {
  if (!cover?.url) {
    return null;
  }

  // IGDB URLs come as //images.igdb.com/igdb/image/upload/t_thumb/abc123.jpg
  // We need to:
  // 1. Add https: prefix
  // 2. Replace t_thumb with the desired size (t_cover_big, t_720p, etc.)
  
  const sizePrefix = `t_${size}`;
  const fullUrl = cover.url.replace(/t_\w+/, sizePrefix);
  
  return `https:${fullUrl}`;
}

/**
 * Get a placeholder image URL when no cover is available
 */
export function getPlaceholderImage(): string {
  // You can replace this with your own placeholder image
  return 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="264" height="352" viewBox="0 0 264 352"%3E%3Crect width="264" height="352" fill="%23374151"/%3E%3Ctext x="50%25" y="50%25" dominant-baseline="middle" text-anchor="middle" font-family="sans-serif" font-size="24" fill="%239CA3AF"%3ENo Image%3C/text%3E%3C/svg%3E';
}

/**
 * Get image URL with fallback to placeholder
 * 
 * @param cover - The cover object from the API
 * @param size - The desired image size
 * @returns Image URL (either IGDB or placeholder)
 */
export function getCoverImageUrl(
  cover: Cover | null | undefined,
  size: IGDBImageSize = 'cover_big'
): string {
  return getIGDBImageUrl(cover, size) || getPlaceholderImage();
}

