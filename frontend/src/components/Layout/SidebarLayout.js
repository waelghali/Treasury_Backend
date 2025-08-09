// src/components/SidebarLayout.js
import React, { useState, useEffect } from 'react';
import { Link, Outlet, useLocation } from 'react-router-dom';
import { Home, Users, Briefcase, Settings, FileText, BarChart, LogOut, DollarSign, List, Gavel, File, HardDrive, LayoutDashboard, Clock } from 'lucide-react';

function SidebarLayout({ onLogout, headerTitle }) {
  const [showGlobalConfigSubMenu, setShowGlobalConfigSubMenu] = useState(false);
  const location = useLocation();

  useEffect(() => {
    if (location.pathname.startsWith('/system-owner/global-configurations')) {
      setShowGlobalConfigSubMenu(true);
    }
  }, [location.pathname]);
  
  // A helper to determine if a link is active for highlighting
  const isLinkActive = (path) => location.pathname.startsWith(path);

  return (
    <div className="flex h-screen bg-gray-100 overflow-hidden">
      <aside className="w-72 bg-white shadow-lg border-r border-gray-200 flex flex-col flex-shrink-0">
        <div className="p-6 border-b border-gray-200">
          <h1 className="text-xl font-bold text-gray-800">Treasury Platform</h1>
          <p className="text-sm text-gray-500">Enterprise Edition</p>
        </div>

        <nav className="flex-grow p-4 space-y-2 overflow-y-auto">
          <Link
            to="/system-owner/dashboard"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/dashboard') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Home className="h-5 w-5 mr-3" />
            Dashboard
          </Link>

          <Link
            to="/system-owner/subscription-plans"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/subscription-plans') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Briefcase className="h-5 w-5 mr-3" />
            Subscription Plans
          </Link>

          <Link
            to="/system-owner/customers"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/customers') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Users className="h-5 w-5 mr-3" />
            Customer Management
          </Link>
          <Link
            to="/system-owner/system-notifications"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/system-notifications') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Settings className="h-5 w-5 mr-3" />
            System Notifications
          </Link>		
          <Link
            to="/system-owner/scheduler"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/scheduler') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <Clock className="h-5 w-5 mr-3" />
            Scheduler
          </Link>

          <div>
            <button
              onClick={() => setShowGlobalConfigSubMenu(!showGlobalConfigSubMenu)}
              className={`flex items-center justify-between w-full p-3 rounded-lg transition-colors duration-200 ${
                isLinkActive('/system-owner/global-configurations') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
              }`}
            >
              <span className="flex items-center">
                <Settings className="h-5 w-5 mr-3" />
                Global Configurations
              </span>
              <svg
                className={`w-4 h-4 transform ${showGlobalConfigSubMenu ? 'rotate-90' : 'rotate-0'} transition-transform`}
                fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M9 5l7 7-7 7"></path>
              </svg>
            </button>
            {showGlobalConfigSubMenu && (
              <div className="ml-6 mt-1 space-y-1">
                <Link
                  to="/system-owner/global-configurations"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations') && !isLinkActive('/system-owner/global-configurations/common-list') && !isLinkActive('/system-owner/global-configurations/templates')
                    ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <LayoutDashboard className="h-4 w-4 mr-2" />
                  Ranges Settings
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/banks"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/banks') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <HardDrive className="h-4 w-4 mr-2" />
                  Banks
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/currencies"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/currencies') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <DollarSign className="h-4 w-4 mr-2" />
                  Currencies
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/lg-types"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/lg-types') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <List className="h-4 w-4 mr-2" />
                  LG Types
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/rules"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/rules') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <Gavel className="h-4 w-4 mr-2" />
                  Rules
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/issuing-methods"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/issuing-methods') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <File className="h-4 w-4 mr-2" />
                  Issuing Methods
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/lg-statuses"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/lg-statuses') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <List className="h-4 w-4 mr-2" />
                  LG Statuses
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/lg-operational-statuses"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/lg-operational-statuses') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <LayoutDashboard className="h-4 w-4 mr-2" />
                  LG Operational Statuses
                </Link>
                <Link
                  to="/system-owner/global-configurations/common-list/universal-categories"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/common-list/universal-categories') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <List className="h-4 w-4 mr-2" />
                  Universal Categories
                </Link>

                <Link
                  to="/system-owner/global-configurations/templates"
                  className={`flex items-center p-2 rounded-lg transition-colors duration-200 ${
                    isLinkActive('/system-owner/global-configurations/templates') ? 'bg-blue-50 text-blue-600 font-medium' : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <FileText className="h-4 w-4 mr-2" />
                  Templates
                </Link>
              </div>
            )}
          </div>

          <Link
            to="/system-owner/audit-logs"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/audit-logs')
                ? 'bg-blue-100 text-blue-700 font-semibold'
                : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <FileText className="h-5 w-5 mr-3" />
            Audit Logs
          </Link>

          <Link
            to="/system-owner/reports"
            className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${
              isLinkActive('/system-owner/reports') ? 'bg-blue-100 text-blue-700 font-semibold' : 'text-gray-700 hover:bg-gray-100'
            }`}
          >
            <BarChart className="h-5 w-5 mr-3" />
            Reports
          </Link>
        </nav>

        <div className="p-4 border-t border-gray-200 flex-shrink-0">
          <div className="flex items-center mb-3">
            <span className="inline-flex items-center justify-center h-8 w-8 rounded-full bg-blue-500 text-white text-sm font-semibold">
              SO
            </span>
            <div className="ml-3">
              <p className="text-sm font-semibold text-gray-800">System Owner</p>
              <p className="text-xs text-gray-500">Admin</p>
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

        <Outlet />
      </main>
    </div>
  );
}

export default SidebarLayout;