import React, { useState, useEffect } from 'react';
import { useLocation, Outlet } from 'react-router-dom';
import SidebarLayout from './Layout/SidebarLayout';
import CorporateAdminLayout from './Layout/CorporateAdminLayout';
import EndUserLayout from './Layout/EndUserLayout';
import { jwtDecode } from 'jwt-decode';
import { fetchActiveSystemNotifications } from '../services/notificationService';

function ProtectedLayout({ onLogout, subscriptionStatus, subscriptionEndDate }) {
  const location = useLocation();
  const [userRole, setUserRole] = useState(null);
  const [customerId, setCustomerId] = useState(null);
  const [customerName, setCustomerName] = useState(null);
  const [activeMenuItem, setActiveMenuItem] = useState(null);
  const [headerTitle, setHeaderTitle] = useState('');
  const [systemNotifications, setSystemNotifications] = useState([]);

  const getActiveState = (currentPath, role) => {
    let activeItem = null;
    let title = '';

    if (!currentPath || !role) {
      return { activeItem, title };
    }
    
    // Logic for System Owner menu items and headers
    if (role === 'system_owner') {
      if (currentPath.startsWith('/system-owner/dashboard')) { activeItem = 'dashboard'; title = 'Treasury Dashboard'; }
      else if (currentPath.startsWith('/system-owner/customers')) { activeItem = 'customer-management'; title = 'Customer Management'; }
      else if (currentPath.startsWith('/system-owner/subscription-plans')) { activeItem = 'subscription-plans'; title = 'Subscription Plans'; }
      else if (currentPath.startsWith('/system-owner/global-configurations/common-list/')) {
        activeItem = 'global-configurations-sub';
        const listType = currentPath.split('/').pop();
        title = `Global Configurations: ${listType.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`;
      }
      else if (currentPath.startsWith('/system-owner/global-configurations/templates')) {
        activeItem = 'global-configurations-sub';
        title = 'Templates Management';
      }
      else if (currentPath.startsWith('/system-owner/global-configurations')) { activeItem = 'global-configurations'; title = 'Global Configurations Overview'; }
      else if (currentPath.startsWith('/system-owner/audit-logs')) { activeItem = 'audit-logs'; title = 'Audit Logs'; }
      else if (currentPath.startsWith('/system-owner/reports')) { 
        activeItem = 'reports'; 
        const reportPath = currentPath.split('/').pop();
        title = `Report: ${reportPath.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`;
      }
      else { activeItem = 'dashboard'; title = 'Treasury Dashboard'; }
    }
    // Logic for Corporate Admin menu items and headers
    else if (role === 'corporate_admin') {
      if (currentPath.startsWith('/corporate-admin/dashboard')) { activeItem = 'corporate-admin-dashboard'; title = 'Corporate Admin Dashboard'; }
      else if (currentPath.startsWith('/corporate-admin/action-center')) { activeItem = 'corporate-admin-action-center'; title = 'Action Center'; }
      else if (currentPath.startsWith('/corporate-admin/lg-categories/new')) { activeItem = 'corporate-admin-lg-categories'; title = 'Create LG Category'; }
      else if (currentPath.startsWith('/corporate-admin/lg-categories/edit/')) { activeItem = 'corporate-admin-lg-categories'; title = 'Edit LG Category'; }
      else if (currentPath.startsWith('/corporate-admin/lg-categories')) { activeItem = 'corporate-admin-lg-categories'; title = 'LG Categories'; }
      else if (currentPath.startsWith('/corporate-admin/users/new')) { activeItem = 'corporate-admin-users'; title = 'Create User'; }
      else if (currentPath.startsWith('/corporate-admin/users/edit/')) { activeItem = 'corporate-admin-users'; title = 'Edit User'; }
      else if (currentPath.startsWith('/corporate-admin/users')) { activeItem = 'corporate-admin-users'; title = 'Manage Users'; }
      else if (currentPath.startsWith('/corporate-admin/module-configs')) { activeItem = 'corporate-admin-module-configs'; title = 'Module Settings'; }
      else if (currentPath.startsWith('/corporate-admin/reports')) { 
        activeItem = 'corporate-admin-reports'; 
        const reportPath = currentPath.split('/').pop();
        title = `Report: ${reportPath.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`;
      }
      else if (currentPath.startsWith('/corporate-admin/audit-logs')) { activeItem = 'corporate-admin-audit-logs'; title = 'Audit Log'; }
      else if (currentPath.startsWith('/corporate-admin/approval-requests')) { activeItem = 'corporate-admin-approval-requests'; title = 'Pending Approvals'; }
      else if (currentPath.startsWith('/corporate-admin/lg-records/')) { activeItem = 'corporate-admin-lg-records'; title = 'LG Record Details (Read-Only)'; }
      else if (currentPath.startsWith('/corporate-admin/lg-records')) { activeItem = 'corporate-admin-lg-records'; title = 'Manage LG Records (View)'; }
      else { activeItem = 'corporate-admin-dashboard'; title = 'Corporate Admin Dashboard'; }
    }
    // Logic for End User / Checker menu items and headers
    else if (role === 'end_user' || role === 'checker') {
      if (currentPath.startsWith('/end-user/dashboard')) { activeItem = 'end-user-dashboard'; title = 'End User Dashboard'; }
      else if (currentPath.startsWith('/end-user/action-center')) { activeItem = 'end-user-action-center'; title = 'Action Center'; }
      else if (currentPath.startsWith('/end-user/lg-records/new')) { activeItem = 'end-user-record-new-lg'; title = 'Record New LG'; }
      else if (currentPath.startsWith('/end-user/lg-records/')) { activeItem = 'end-user-manage-lg-records'; title = 'LG Record Details'; }
      else if (currentPath.startsWith('/end-user/lg-records')) { activeItem = 'end-user-manage-lg-records'; title = 'Manage LG Records'; }
      else if (currentPath.startsWith('/end-user/internal-owners')) { activeItem = 'end-user-internal-owners'; title = 'Manage Internal Owners'; }
      else if (currentPath.startsWith('/end-user/pending-approvals')) { activeItem = 'end-user-pending-approvals'; title = 'Withdraw a Pending Approval'; }
      else if (currentPath.startsWith('/end-user/reports')) {
        activeItem = 'end-user-reports';
        const reportPath = currentPath.split('/').pop();
        title = `Report: ${reportPath.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`;
      }
      else { activeItem = 'end-user-dashboard'; title = 'End User Dashboard'; }
    }
    
    return { activeItem, title };
  };

  useEffect(() => {
    const token = localStorage.getItem('jwt_token');
    if (token) {
      try {
        const decoded = jwtDecode(token);
        setUserRole(decoded.role);
        setCustomerId(decoded.customer_id);
        setCustomerName(decoded.customer_name);
      } catch (error) {
        console.error("ProtectedLayout: Failed to decode token:", error);
        onLogout(); 
      }
    } else {
      onLogout();
    }
  }, [onLogout]);

  useEffect(() => {
    const { activeItem, title } = getActiveState(location.pathname, userRole);
    setActiveMenuItem(activeItem);
    setHeaderTitle(title);
  }, [location.pathname, userRole]);

  useEffect(() => {
    const getNotifications = async () => {
      if (userRole && userRole !== 'system_owner') {
        const notifications = await fetchActiveSystemNotifications();
        setSystemNotifications(notifications);
      } else {
        setSystemNotifications([]);
      }
    };

    getNotifications();
  }, [userRole, customerId]);

  if (userRole === 'system_owner') {
    return (
      <SidebarLayout onLogout={onLogout} activeMenuItem={activeMenuItem} headerTitle={headerTitle}>
        <Outlet />
      </SidebarLayout>
    );
  } else if (userRole === 'corporate_admin') {
    return (
      <CorporateAdminLayout
        onLogout={onLogout}
        activeMenuItem={activeMenuItem}
        customerName={customerName}
        headerTitle={headerTitle}
        systemNotifications={systemNotifications}
        subscriptionStatus={subscriptionStatus}
        subscriptionEndDate={subscriptionEndDate}
      >
        <Outlet />
      </CorporateAdminLayout>
    );
  } else if (userRole === 'end_user' || userRole === 'checker') {
    return (
      <EndUserLayout
        onLogout={onLogout}
        activeMenuItem={activeMenuItem}
        customerName={customerName}
        headerTitle={headerTitle}
        systemNotifications={systemNotifications}
        subscriptionStatus={subscriptionStatus}
        subscriptionEndDate={subscriptionEndDate}
      >
        <Outlet />
      </EndUserLayout>
    );
  } else {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Determining user role and loading layout...</p>
      </div>
    );
  }
}

export default ProtectedLayout;