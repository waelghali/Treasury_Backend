// frontend/src/App.js
import React, { useState, useEffect, useRef } from 'react';
import { BrowserRouter, Routes, Route, Navigate, useNavigate } from 'react-router-dom';

import LoginPage from './pages/Auth/LoginPage';
import ForcePasswordChangePage from './pages/Auth/ForcePasswordChangePage';
import ForgotPasswordPage from './pages/Auth/ForgotPasswordPage';
import ResetPasswordPage from './pages/Auth/ResetPasswordPage';

import RenewalPage from './pages/RenewalPage'; 

import AuthWrapper from './components/AuthWrapper';
import ProtectedLayout from './components/ProtectedLayout';

import SystemOwnerRoutes from './routes/SystemOwnerRoutes.js';
import CorporateAdminRoutes from './routes/CorporateAdminRoutes.js';
import EndUserRoutes from './routes/EndUserRoutes.js';

import { getAuthToken, setAuthToken } from './services/apiService';
import { jwtDecode } from 'jwt-decode';

import { ToastContainer, Flip, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import './ToastAnimations.css';

function AppContent() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [userRole, setUserRole] = useState(null);
  const [mustChangePassword, setMustChangePassword] = useState(false);
  const [userId, setUserId] = useState(null);
  const [customerName, setCustomerName] = useState(null);
  const [initialLoadComplete, setInitialLoadComplete] = useState(false);

  // NEW: State for subscription status and end date
  const [subscriptionStatus, setSubscriptionStatus] = useState(null);
  const [subscriptionEndDate, setSubscriptionEndDate] = useState(null);

  const navigate = useNavigate();

  const initialDecodeRef = useRef(false);

  const decodeTokenAndSetState = () => {
    const token = getAuthToken();
    let authStatus = false;
    let role = null;
    let mustChange = false;
    let id = null;
    let name = null;

    // NEW: Subscription vars
    let status = null;
    let endDate = null;

    if (token) {
      try {
        const decoded = jwtDecode(token);
        authStatus = true;
        role = decoded.role;
        mustChange = decoded.must_change_password || false;
        id = decoded.user_id;
        name = decoded.customer_name;

        // NEW: Read subscription details from token
        status = decoded.subscription_status || 'active'; // Default to active
        endDate = decoded.subscription_end_date || null;

      } catch (error) {
        console.error("App.js: Failed to decode token:", error);
        setAuthToken(null);
      }
    }

    setIsAuthenticated(authStatus);
    setUserRole(role);
    setMustChangePassword(mustChange);
    setUserId(id);
    setCustomerName(name);

    // NEW: Set subscription state
    setSubscriptionStatus(status);
    setSubscriptionEndDate(endDate);

    return { mustChangePassword: mustChange, userRole: role, isAuthenticated: authStatus, subscriptionStatus: status };
  };

  useEffect(() => {
    if (!initialDecodeRef.current) {
      decodeTokenAndSetState();
      setInitialLoadComplete(true);
      initialDecodeRef.current = true;
    }

    const handleStorageChange = (event) => {
      if (event.key === 'jwt_token') {
        decodeTokenAndSetState();
      }
    };
    window.addEventListener('storage', handleStorageChange);

    return () => {
      window.removeEventListener('storage', handleStorageChange);
    };
  }, []);

  const handleLoginSuccess = () => {
    const { mustChangePassword: updatedMustChangePassword, userRole: updatedUserRole, isAuthenticated: updatedIsAuthenticated, subscriptionStatus: updatedSubscriptionStatus } = decodeTokenAndSetState();

    if (!updatedIsAuthenticated) {
        navigate("/login", { replace: true });
    } else if (updatedMustChangePassword) {
        navigate("/force-password-change", { replace: true });
    } else if (updatedSubscriptionStatus === 'expired') { // NEW: Redirect to renewal page if expired
        navigate("/renewal", { replace: true });
    } else {
        const redirectPath = getDefaultRedirectPath(updatedUserRole);
        navigate(redirectPath, { replace: true });
    }
  };

  const handleLogout = async () => {
    setIsAuthenticated(false);
    setUserRole(null);
    setMustChangePassword(false);
    setUserId(null);
    setCustomerName(null);
    setAuthToken(null);
    navigate("/login", { replace: true });
  };

  const getDefaultRedirectPath = (role) => {
    if (role === 'system_owner') {
      return "/system-owner/dashboard";
    } else if (role === 'corporate_admin') {
      return "/corporate-admin/dashboard";
    } else if (role === 'end_user' || role === 'checker') {
      return "/end-user/dashboard";
    }
    return "/login";
  };

  const renderAppRoutes = () => {
    if (!initialLoadComplete) {
      return (
        <div className="flex justify-center items-center min-h-screen bg-gray-100">
          <p>Loading application and determining access...</p>
        </div>
      );
    }

    // NEW: Redirect to renewal page if subscription is expired
    if (isAuthenticated && subscriptionStatus === 'expired') {
      return <Navigate to="/renewal" replace />;
    }

    return (
      <Routes>
        <Route path="/login" element={<LoginPage onLoginSuccess={handleLoginSuccess} />} />
        <Route path="/forgot-password" element={<ForgotPasswordPage />} />
        <Route path="/reset-password" element={<ResetPasswordPage />} />
        <Route path="/force-password-change" element={<ForcePasswordChangePage onPasswordChangeSuccess={handleLoginSuccess} />} />

        {/* NEW: Renewal page outside of the protected routes */}
        <Route path="/renewal" element={<RenewalPage />} />

        <Route element={<AuthWrapper isAuthenticated={isAuthenticated} userRole={userRole} onLogout={handleLogout} />}>
          {mustChangePassword ? (
            <Route path="*" element={<Navigate to="/force-password-change" replace />} />
          ) : (
            <Route
              path="/*"
              element={<ProtectedLayout onLogout={handleLogout} key={userId} customerName={customerName} subscriptionStatus={subscriptionStatus} subscriptionEndDate={subscriptionEndDate} />}
            >
              <Route path="system-owner/*" element={<SystemOwnerRoutes onLogout={handleLogout} />} />
              <Route path="corporate-admin/*" element={<CorporateAdminRoutes onLogout={handleLogout} />} />
              <Route path="end-user/*" element={<EndUserRoutes onLogout={handleLogout} />} />
              <Route path="*" element={<Navigate to={getDefaultRedirectPath(userRole)} replace />} />
            </Route>
          )}
        </Route>

        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  };

  return renderAppRoutes();
}

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100 font-sans text-gray-800 antialiased">
        <AppContent />
        <ToastContainer
          position="top-right"
          autoClose={2500}
          hideProgressBar={false}
          newestOnTop={false}
          closeOnClick
          rtl={false}
          pauseOnFocusLoss
          draggable
          pauseOnHover
          transition={Flip}
        />
      </div>
    </BrowserRouter>
  );
}

export default App;