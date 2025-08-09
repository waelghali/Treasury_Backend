// src/services/apiService.js
// Centralized service for making API calls to the backend.

import apiClient from './apiClient'; // CORRECTED: Use a default import

export const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000/api/v1'; // Your FastAPI backend URL for v1 endpoints
export const API_BASE_URL_V2 = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000/api/v2'; // Your FastAPI backend URL for v2 endpoints (NEW)

// Function to store the access token
export const setAuthToken = (token) => {
  if (token) {
    localStorage.setItem('jwt_token', token);
  } else {
    localStorage.removeItem('jwt_token');
  }
};

// Function to get the current access token
export const getAuthToken = () => {
  return localStorage.getItem('jwt_token');
};

/**
 * A centralized function to make authenticated API requests.
 * @param {string} url The URL path for the API endpoint.
 * @param {string} method The HTTP method (GET, POST, PUT, DELETE).
 * @param {object} data The request body data (optional).
 * @param {string} contentType The Content-Type header (e.g., 'application/json', 'multipart/form-data').
 * @returns {Promise<any>} A promise that resolves to the response data.
 * @throws {Error} Throws an error for non-2xx responses or network issues.
 */
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

    // Check for a 204 No Content response
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

// --- System Owner Endpoints for Notifications ---

/**
 * @deprecated Use the more generic fetchSystemNotificationsForUser in notificationService.js
 */
export const getSystemNotifications = async () => {
  return apiRequest('/system-owner/system-notifications/');
};

export const getSystemNotificationById = async (id) => {
  return apiRequest(`/system-owner/system-notifications/${id}`, 'GET');
};

export const createSystemNotification = async (notificationData) => {
  return apiRequest('/system-owner/system-notifications/', 'POST', notificationData);
};

export const updateSystemNotification = async (id, notificationData) => {
  return apiRequest(`/system-owner/system-notifications/${id}`, 'PUT', notificationData);
};

export const deleteSystemNotification = async (id) => {
  return apiRequest(`/system-owner/system-notifications/${id}`, 'DELETE');
};

export const restoreSystemNotification = async (id) => {
  return apiRequest(`/system-owner/system-notifications/${id}/restore`, 'POST');
};

// --- NEW: General Notifications Endpoints for All Users ---

/**
 * Fetches all active system notifications relevant to the current user.
 * This includes global notifications and those targeted at the user's customer.
 * @returns {Array} An array of active system notification objects.
 */
export const fetchActiveSystemNotifications = async () => {
  return apiClient.get('/end-user/system-notifications/');
};

//**
// * Logs a user's dismissal of a specific notification.
// * @param {number} id The ID of the notification to dismiss.
// * @returns {Promise<object>} The updated notification object.
 
//export const dismissSystemNotification = async (id) => {
//  return apiClient.post(`/end-user/system-notifications/${id}/dismiss`);
//}; 


// --- NEW PHASE 2 HELPER FUNCTIONS ---
/**
 * Fetches all users for the System Owner to target notifications.
 */
export const getAllUsersForSystemOwner = async () => {
  return apiRequest('/system-owner/users/');
};

/**
 * Fetches all available user roles for the System Owner to target notifications.
 */
export const getAllRolesForSystemOwner = async () => {
  // This endpoint might not exist, so we mock a list of roles.
  // In a real app, this would be a backend call to /roles or similar.
  return ['System Owner', 'Corporate Admin', 'End User'];
};

/**
 * Sends a cancellation request for an LG instruction.
 * @param {number} instructionId The ID of the instruction to cancel.
 * @param {string} reason The reason for the cancellation.
 * @param {boolean} declarationConfirmed Whether the user has confirmed the declaration.
 * @returns {Promise<object>} The response data, including the updated instruction and LG record.
 */
export const cancelLGInstruction = async (instructionId, reason, declarationConfirmed) => {
  const payload = { reason, declaration_confirmed: declarationConfirmed };
  return apiRequest(`/end-user/lg-records/instructions/${instructionId}/cancel`, 'POST', payload);
};