// frontend/src/services/notificationService.js
import apiClient from './apiClient'; // CORRECTED: Use a default import

/**
 * Fetches all active system notifications relevant to the current user.
 * This includes global notifications and those targeted at the user's customer.
 * @returns {Array} An array of active system notification objects.
 */
export const fetchActiveSystemNotifications = async () => {
    try {
        const response = await apiClient.get('/end-user/system-notifications/');
        return response.data;
    } catch (error) {
        console.error('Error fetching system notifications:', error);
        // It's safe to return an empty array on error, as notifications are not critical to page functionality.
        return [];
    }
};
