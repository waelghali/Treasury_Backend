// src/components/Layout/CorporateAdminLayout.js
import React from 'react';
import { Link, Outlet } from 'react-router-dom';
import { Home, Users, FolderKanban, LogOut, Settings, FileText, BarChart, Hourglass, ListTodo, ClipboardList } from 'lucide-react';
import NotificationBanner from '../NotificationBanner';
import SubscriptionBanner from '../SubscriptionBanner'; // NEW: Import the subscription banner

function CorporateAdminLayout({ activeMenuItem, onLogout, customerName, headerTitle, systemNotifications, subscriptionStatus, subscriptionEndDate }) { // NEW: Receive subscription props
  const isDashboard = activeMenuItem === 'corporate-admin-dashboard';
  const isGracePeriod = subscriptionStatus === 'grace'; // NEW: Check for grace status

  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      <aside className="w-72 bg-white shadow-lg border-r border-gray-200 flex flex-col flex-shrink-0">
        {/* FIX: Reduced padding to shrink top space */}
        <div className="py-4 px-6 border-b border-gray-200">
          <h1 className="text-xl font-bold text-gray-800">Treasury Platform</h1>
          <p className="text-sm text-gray-500">Corporate Edition</p>
        </div>

        {/* FIX: Removed overflow-y-auto to prevent the scrollbar, and adjusted padding */}
        <nav className="flex-grow p-4 space-y-2">
          <Link
            to="/corporate-admin/dashboard"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-dashboard' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Home className="h-5 w-5 mr-3" />
            Dashboard
          </Link>
          
          <Link
            to="/corporate-admin/action-center"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-action-center' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <ClipboardList className="h-5 w-5 mr-3" />
            Action Center (View)
          </Link>

          <Link
            to="/corporate-admin/lg-records"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-lg-records' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FolderKanban className="h-5 w-5 mr-3" />
            Manage LG Records (View)
          </Link>

          <Link
            to="/corporate-admin/users"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-users' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Users className="h-5 w-5 mr-3" />
            Manage Users
          </Link>

          <Link
            to="/corporate-admin/module-configs"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-module-configs' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Settings className="h-5 w-5 mr-3" />
            Module Settings
          </Link>

          <Link
            to="/corporate-admin/lg-categories"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-lg-categories' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FolderKanban className="h-5 w-5 mr-3" />
            LG Categories
          </Link>

          <Link
            to="/corporate-admin/approval-requests"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-approval-requests' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Hourglass className="h-5 w-5 mr-3" />
            Pending Approvals
          </Link>

          <Link
            to="/corporate-admin/reports"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-reports' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <BarChart className="h-5 w-5 mr-3" />
            Reports
          </Link>

          <Link
            to="/corporate-admin/audit-logs"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'corporate-admin-audit-logs' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FileText className="h-5 w-5 mr-3" />
            Audit Log
          </Link>
        </nav>

        <div className="p-4 border-t border-gray-200 flex-shrink-0">
          <div className="flex items-center mb-3">
            <span className="inline-flex items-center justify-center h-8 w-8 rounded-full bg-blue-500 text-white text-sm font-semibold">
              CA
            </span>
            <div className="ml-3">
              <p className="text-sm font-semibold text-gray-800">Corporate Admin</p>
              {/* FIX: Conditionally render customerName to avoid "Org: " when no name is available */}
              <p className="text-xs text-gray-500">{customerName ? `Org: ${customerName}` : 'Org: N/A'}</p>
            </div>
          </div>
          <button
            onClick={onLogout}
            className="w-full flex items-center justify-center p-3 text-red-600 hover:bg-red-50 rounded-lg transition-colors duration-200"
          >
            <LogOut className="h-5 w-5 mr-2" />
            Sign Out
          </button>
        </div>
      </aside>

      <main className="flex-1 p-8 overflow-y-auto">
        <header className="flex justify-between items-center pb-6 border-b border-gray-200 mb-6">
          <h1 className="text-2xl font-semibold text-gray-800">
            {headerTitle}
          </h1>
        </header>
        {isDashboard && <NotificationBanner notifications={systemNotifications} />}
        {isGracePeriod && <SubscriptionBanner subscriptionEndDate={subscriptionEndDate} />}
        <Outlet />
      </main>
    </div>
  );
}

export default CorporateAdminLayout;