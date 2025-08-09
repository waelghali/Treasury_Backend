import React from 'react';
import { Link, Outlet } from 'react-router-dom';
import { Home, FileText, PlusCircle, BarChart, LogOut, FolderKanban, Users, ListTodo } from 'lucide-react';
import NotificationBanner from '../NotificationBanner';
import SubscriptionBanner from '../SubscriptionBanner'; // NEW: Import the subscription banner

function EndUserLayout({ onLogout, activeMenuItem, customerName, headerTitle, systemNotifications, subscriptionStatus, subscriptionEndDate }) { // NEW: Receive subscription props
  const isDashboard = activeMenuItem === 'end-user-dashboard';
  const isGracePeriod = subscriptionStatus === 'grace'; // NEW: Check for grace status

  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      <aside className="w-72 bg-white shadow-lg border-r border-gray-200 flex flex-col flex-shrink-0">
        <div className="p-6 border-b border-gray-200">
          <h1 className="text-xl font-bold text-gray-800">Treasury Platform</h1>
          <p className="text-sm text-gray-500">End User Edition</p>
        </div>

        <nav className="flex-grow p-4 space-y-2 overflow-y-auto">
          <Link
            to="/end-user/dashboard"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-dashboard' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Home className="h-5 w-5 mr-3" />
            Dashboard
          </Link>

          <Link
            to="/end-user/action-center"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-action-center' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <ListTodo className="h-5 w-5 mr-3" />
            Action Center
          </Link>

          <Link
            to="/end-user/lg-records/new"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-record-new-lg' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <PlusCircle className="h-5 w-5 mr-3" />
            Record New LG
          </Link>

          <Link
            to="/end-user/lg-records"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-manage-lg-records' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FolderKanban className="h-5 w-5 mr-3" />
            Manage LG Records
          </Link>

          <Link
            to="/end-user/pending-approvals"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-pending-approvals' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FileText className="h-5 w-5 mr-3" />
            Withdraw Request for Approval
          </Link>

          <Link
            to="/end-user/internal-owners"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-internal-owners' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Users className="h-5 w-5 mr-3" />
            Manage Internal Owners
          </Link>

          <Link
            to="/end-user/reports"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              activeMenuItem === 'end-user-reports' ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <BarChart className="h-5 w-5 mr-3" />
            Reports
          </Link>
        </nav>

        <div className="p-4 border-t border-gray-200 flex-shrink-0">
          <div className="flex items-center mb-3">
            <span className="inline-flex items-center justify-center h-8 w-8 rounded-full bg-blue-500 text-white text-sm font-semibold">
              EU
            </span>
            <div className="ml-3">
              <p className="text-sm font-semibold text-gray-800">End User</p>
              <p className="text-xs text-gray-500">Org: {customerName}</p>
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

export default EndUserLayout;