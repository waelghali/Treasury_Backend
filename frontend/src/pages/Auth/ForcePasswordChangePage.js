// frontend/src/pages/Auth/ForcePasswordChangePage.js
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest, getAuthToken, setAuthToken } from 'services/apiService.js';
import { jwtDecode } from 'jwt-decode';
import { AlertCircle, Lock } from 'lucide-react';

function ForcePasswordChangePage({ onPasswordChangeSuccess }) {
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmNewPassword, setConfirmNewPassword] = useState(''); // Corrected state variable
  
  const [error, setError] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState(null);
  const navigate = useNavigate();

  const getRedirectPathFromToken = () => {
    const token = getAuthToken();
    if (token) {
      try {
        const decoded = jwtDecode(token);
        if (decoded.role === 'system_owner') {
          return "/system-owner/dashboard";
        } else if (decoded.role === 'corporate_admin') {
          return "/corporate-admin/dashboard";
        } else if (decoded.role === 'end_user' || decoded.role === 'checker') {
          return "/end-user/dashboard";
        }
      } catch (error) {
        console.error("ForcePasswordChangePage: Failed to decode token for redirect path:", error);
      }
    }
    return "/login";
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setSuccessMessage(null);

    if (newPassword !== confirmNewPassword) {
      setError('New password and confirmation do not match.');
      setIsLoading(false);
      return;
    }

    if (newPassword.length < 8) {
      setError('New password must be at least 8 characters long.');
      setIsLoading(false);
      return;
    }
    // Add other frontend validations here to match backend policy if desired
    // E.g., if (!/[A-Z]/.test(newPassword)) { setError('New password must contain an uppercase letter.'); return; }
    // etc.

    setIsLoading(true);
    console.log("ForcePasswordChangePage: Submitting password change...");
    try {
      const response = await apiRequest(
        `/change-password`,
        'POST',
        {
          current_password: currentPassword,
          new_password: newPassword,
          confirm_new_password: confirmNewPassword // ADDED: Send confirm_new_password to backend
        }
      );

      if (response && response.access_token) {
        setAuthToken(response.access_token);
        console.log("ForcePasswordChangePage: New token received and set in localStorage.");
      } else {
        throw new Error("No new token received after password change. Please log in again.");
      }

      setSuccessMessage('Password changed successfully! Redirecting to your dashboard...');

      if (onPasswordChangeSuccess) {
        console.log("ForcePasswordChangePage: Calling onPasswordChangeSuccess prop.");
        onPasswordChangeSuccess();
      }

    } catch (err) {
      console.error('ForcePasswordChangePage: Password change error:', err);
      setError(err.message || 'An unexpected error occurred during password change.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100 p-4">
      <div className="bg-white p-8 rounded-lg shadow-md w-full max-w-md">
        <h2 className="text-2xl font-bold text-center text-gray-800 mb-6">Change Your Password</h2>
        <p className="text-sm text-gray-600 text-center mb-6">
          You are required to change your password before proceeding.
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="currentPassword" className="block text-gray-700 text-sm font-medium mb-2">
              Current Password (The one you just used to log in)
            </label>
            <input
              type="password"
              id="currentPassword"
              name="currentPassword"
              className="shadow-sm appearance-none border rounded-md w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition duration-200"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              required
            />
          </div>
          <div>
            <label htmlFor="newPassword" className="block text-gray-700 text-sm font-medium mb-2">
              New Password
            </label>
            <input
              type="password"
              id="newPassword"
              name="newPassword"
              className="shadow-sm appearance-none border rounded-md w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition duration-200"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              required
              minLength="8"
            />
          </div>
          <div>
            <label htmlFor="confirmNewPassword" className="block text-gray-700 text-sm font-medium mb-2">
              Confirm New Password
            </label>
            <input
              type="password"
              id="confirmNewPassword"
              name="confirmNewPassword"
              className="shadow-sm appearance-none border rounded-md w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition duration-200"
              value={confirmNewPassword}
              onChange={(e) => setConfirmNewPassword(e.target.value)}
              required
              minLength="8"
            />
          </div>

          {error && (
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
              <AlertCircle className="h-5 w-5 mr-2" />
              <span className="block sm:inline">{error}</span>
            </div>
          )}

          {successMessage && (
            <div className="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded-md relative mb-4" role="alert">
              <span className="block sm:inline">{successMessage}</span>
            </div>
          )}

          <button
            type="submit"
            className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
            disabled={isLoading}
          >
            {isLoading ? (
              <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
            ) : (
              <>
                <Lock className="h-5 w-5 mr-2" />
                Change Password
              </>
            )}
          </button>
        </form>
      </div>
    </div>
  );
}

export default ForcePasswordChangePage;