// frontend/src/utils/auth.js

// Function to determine the default redirect path based on user role
export const getRedirectPathFromToken = (role) => {
    switch (role) {
        case 'system_owner':
            return '/system-owner/dashboard';
        case 'corporate_admin':
            return '/corporate-admin/dashboard';
        case 'end_user':
        case 'checker':
            return '/end-user/dashboard';
        default:
            return '/login'; // Fallback
    }
};