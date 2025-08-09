// src/CorporateAdminRoutes.js
import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';

// Import all Corporate Admin Pages
import CorporateAdminDashboard from 'pages/CorporateAdmin/CorporateAdminDashboard';
import LGCategoryList from 'pages/CorporateAdmin/LGCategoryList';
import LGCategoryForm from 'pages/CorporateAdmin/LGCategoryForm';
import UserManagementPage from 'pages/CorporateAdmin/UserManagementPage';
import UserForm from 'pages/CorporateAdmin/UserForm';
import CustomerConfigurationManagementPage from 'pages/CorporateAdmin/CustomerConfigurationManagementPage';
import PendingApprovalsPage from 'pages/CorporateAdmin/PendingApprovalsPage';
import LGDetailsReadOnlyPage from 'pages/CorporateAdmin/LGDetailsReadOnlyPage';
import LGRecordListReadOnlyPage from 'pages/CorporateAdmin/LGRecordListReadOnlyPage';
import AuditLogsCorporate from 'pages/CorporateAdmin/AuditLogsCorporate';
import ActionCenter from 'pages/CorporateAdmin/ActionCenter';

// NEW: Import the generic ReportsPage and the single report component for this role
import ReportsPage from 'pages/Reports/ReportsPage';
import CustomerLGPerformanceReport from 'pages/Reports/CustomerLGPerformanceReport';

// Define the single report for the Corporate Admin role
const corporateAdminReports = [
  { 
    name: "Customer LG Performance", 
    description: "View LG activity and user performance within your organization.", 
    path: "customer-lg-performance", 
    iconName: "BarChart" 
  },
];


function CorporateAdminRoutes({ onLogout, subscriptionStatus }) {
  const isGracePeriod = subscriptionStatus === 'grace';

  return (
    <Routes>
      {/* Dashboard */}
      <Route path="dashboard" element={<CorporateAdminDashboard onLogout={onLogout} />} />

      {/* Action Center - NEW ROUTE */}
      <Route path="action-center" element={<ActionCenter onLogout={onLogout} />} />

      {/* LG Records - Read-Only View for Corporate Admin */}
      <Route path="lg-records/:id" element={<LGDetailsReadOnlyPage onLogout={onLogout} />} />
      <Route path="lg-records" element={<LGRecordListReadOnlyPage onLogout={onLogout} />} />

      {/* LG Category Management - PASS PROP */}
      <Route path="lg-categories" element={<LGCategoryList onLogout={onLogout} isGracePeriod={isGracePeriod} />} />
      <Route path="lg-categories/new" element={<LGCategoryForm onLogout={onLogout} isGracePeriod={isGracePeriod} />} />
      <Route path="lg-categories/edit/:id" element={<LGCategoryForm onLogout={onLogout} isGracePeriod={isGracePeriod} />} />

      {/* User Management Routes - PASS PROP */}
      <Route path="users" element={<UserManagementPage onLogout={onLogout} isGracePeriod={isGracePeriod} />} />
      <Route path="users/new" element={<UserForm onLogout={onLogout} isGracePeriod={isGracePeriod} />} />
      <Route path="users/edit/:id" element={<UserForm onLogout={onLogout} isGracePeriod={isGracePeriod} />} />

      {/* Corporate Admin Module Configurations - PASS PROP */}
      <Route path="module-configs" element={<CustomerConfigurationManagementPage onLogout={onLogout} isGracePeriod={isGracePeriod} />} />

      {/* Pending Approval Requests Page */}
      <Route path="approval-requests" element={<PendingApprovalsPage />} />

      {/* Reports (Updated for Corporate Admin) */}
      <Route path="reports" element={<ReportsPage reports={corporateAdminReports} />}>
        {/* The index route navigates directly to the single report page */}
        <Route index element={<Navigate to="customer-lg-performance" replace />} />
        <Route path="customer-lg-performance" element={<CustomerLGPerformanceReport />} />
      </Route>

      {/* Audit Logs for CA */}
      <Route path="audit-logs" element={<AuditLogsCorporate onLogout={onLogout} />} />

      {/* Fallback for any unmatched path within /corporate-admin/* */}
      <Route path="*" element={<Navigate to="dashboard" replace />} />
    </Routes>
  );
}

export default CorporateAdminRoutes;