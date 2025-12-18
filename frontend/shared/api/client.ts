/**
 * client.ts
 * 
 * PURPOSE:
 * HTTP client wrapper using native fetch API.
 * Provides configured request methods with automatic token injection,
 * error handling, and response formatting.
 */

import { API_BASE_URL } from '../utils/constants';
import { getToken } from '../utils/storage';

/**
 * API Error class for structured error handling
 */
export class ApiError extends Error {
  constructor(
    public status: number,
    public message: string,
    public data?: any
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/**
 * Request configuration options
 */
interface RequestConfig {
  headers?: Record<string, string>;
  params?: Record<string, string | number | boolean>;
}

/**
 * Build URL with query parameters
 */
function buildUrl(endpoint: string, params?: Record<string, string | number | boolean>): string {
  const url = new URL(`${API_BASE_URL}${endpoint}`);
  
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      url.searchParams.append(key, String(value));
    });
  }
  
  return url.toString();
}

/**
 * Get default headers including auth token if available
 */
function getHeaders(customHeaders?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...customHeaders
  };
  
  const token = getToken();
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  return headers;
}

/**
 * Handle fetch response and errors
 */
async function handleResponse<T>(response: Response): Promise<T> {
  // Check if response is ok (status 200-299)
  if (!response.ok) {
    let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
    let errorData;
    
    try {
      errorData = await response.json();
      errorMessage = errorData.error || errorData.message || errorMessage;
    } catch {
      // Response body is not JSON, use status text
    }
    
    throw new ApiError(response.status, errorMessage, errorData);
  }
  
  // Handle 204 No Content
  if (response.status === 204) {
    return {} as T;
  }
  
  // Parse JSON response
  try {
    return await response.json();
  } catch {
    throw new ApiError(response.status, 'Failed to parse response JSON');
  }
}

/**
 * HTTP Client
 */
export const apiClient = {
  /**
   * GET request
   */
  async get<T>(endpoint: string, config?: RequestConfig): Promise<T> {
    const url = buildUrl(endpoint, config?.params);
    const headers = getHeaders(config?.headers);
    
    const response = await fetch(url, {
      method: 'GET',
      headers
    });
    
    return handleResponse<T>(response);
  },
  
  /**
   * POST request
   */
  async post<T>(endpoint: string, data?: any, config?: RequestConfig): Promise<T> {
    const url = buildUrl(endpoint, config?.params);
    const headers = getHeaders(config?.headers);
    
    const response = await fetch(url, {
      method: 'POST',
      headers,
      body: data ? JSON.stringify(data) : undefined
    });
    
    return handleResponse<T>(response);
  },
  
  /**
   * PATCH request
   */
  async patch<T>(endpoint: string, data?: any, config?: RequestConfig): Promise<T> {
    const url = buildUrl(endpoint, config?.params);
    const headers = getHeaders(config?.headers);
    
    const response = await fetch(url, {
      method: 'PATCH',
      headers,
      body: data ? JSON.stringify(data) : undefined
    });
    
    return handleResponse<T>(response);
  },
  
  /**
   * DELETE request
   */
  async delete<T>(endpoint: string, config?: RequestConfig): Promise<T> {
    const url = buildUrl(endpoint, config?.params);
    const headers = getHeaders(config?.headers);
    
    const response = await fetch(url, {
      method: 'DELETE',
      headers
    });
    
    return handleResponse<T>(response);
  }
};

