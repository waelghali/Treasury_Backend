import React, { useEffect } from 'react';
import { Navigate, Outlet } from 'react-router-dom';

/**
 * AuthWrapper component checks if the user is authenticated based on props.
 * It no longer performs its own token decoding or state management.
 * If authenticated, it renders its <Outlet /> (which will be ProtectedLayout).
 * If not authenticated, it redirects to the login page.
 */
function AuthWrapper({ isAuthenticated, userRole, onLogout }) { // Receive state as props
  useEffect(() => {
    // This useEffect is now primarily for logging or side effects,
    // as auth logic is handled by props from App.js.
    console.log(`AuthWrapper: Rendered. isAuthenticated: ${isAuthenticated}, userRole: ${userRole}`);
  }, [isAuthenticated, userRole]); // Re-run if auth state changes

  // If not authenticated, redirect to login page
  if (!isAuthenticated) {
    console.log(`AuthWrapper: User is NOT authenticated. Redirecting to /login from path: ${window.location.pathname}`);
    return <Navigate to="/login" replace />;
  }

  // If authenticated, render the Outlet. The parent Route in App.js will define what gets rendered here.
  return <Outlet />;
}

export default AuthWrapper;