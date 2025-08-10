import apiClient from './apiClient';

// Use the same env var here, no duplicates or confusion
export const API_BASE_URL = process.env.REACT_APP_API_URL;
export const API_BASE_URL_V2 = process.env.REACT_APP_API_URL;

export const setAuthToken = (token) => {
  if (token) {
    localStorage.setItem('jwt_token', token);
  } else {
    localStorage.removeItem('jwt_token');
  }
};

export const getAuthToken = () => {
  return localStorage.getItem('jwt_token');
};

export const apiRequest = async (url, method = 'GET', data = null, contentType = 'application/json') => {
  const token = getAuthToken();
  const headers = {};

  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  if (contentType === 'application/json') {
    headers['Content-Type'] = 'application/json';
  }

  let body = data;
  if (contentType === 'application/json' && data !== null) {
    body = JSON.stringify(data);
  }

  try {
    const response = await fetch(`${API_BASE_URL}${url}`, {
      method,
      headers,
      body: (method === 'GET' || method === 'HEAD') ? null : body,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      if (response.status === 401) {
        setAuthToken(null);
        window.location.href = '/login';
        const authError = new Error('Authentication required or session expired. Please log in again.');
        authError.statusCode = 401;
        throw authError;
      }
      const errorMessage = errorData.detail || errorData.message || response.statusText;
      const error = new Error(errorMessage);
      error.statusCode = response.status;
      throw error;
    }

    const contentTypeHeader = response.headers.get('content-type');
    if (response.status === 204 || !contentTypeHeader) {
      return null;
    }

    return await response.json();
  } catch (error) {
    console.error('API Request Failed:', error);
    throw error;
  }
};

// Your other service functions here (unchanged) ...
