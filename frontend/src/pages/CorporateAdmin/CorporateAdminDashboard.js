import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { FolderKanban, Users, Settings } from 'lucide-react';
import { toast } from 'react-toastify';

// NEW: A reusable component to provide a tooltip for disabled elements during the grace period.
const GracePeriodTooltip = ({ children, isGracePeriod }) => {
    if (isGracePeriod) {
        return (
            <div className="relative group inline-block">
                {children}
                <div className="opacity-0 w-max bg-gray-800 text-white text-xs rounded-lg py-2 px-3 absolute z-10 bottom-full left-1/2 -translate-x-1/2 pointer-events-none group-hover:opacity-100 transition-opacity duration-200">
                    This action is disabled during your subscription's grace period.
                    <svg className="absolute text-gray-800 h-2 w-full left-0 top-full" x="0px" y="0px" viewBox="0 0 255 255">
                        <polygon className="fill-current" points="0,0 127.5,127.5 255,0"/>
                    </svg>
                </div>
            </div>
        );
    }
    return children;
};

function CorporateAdminDashboard({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const [dashboardMetrics, setDashboardMetrics] = useState({
    total_lg_categories: 'N/A',
    total_active_users: 'N/A',
    pending_approvals: 'N/A',
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  // NOTE: I am not modifying the commented-out fetch logic as per instructions.

  // Quick actions specific to Corporate Admin
  const quickActions = [
    { 
      id: 1, 
      title: 'Manage LG Categories', 
      description: 'Create and organize customer-specific LG categories', 
      icon: FolderKanban, 
      link: '/corporate-admin/lg-categories',
      isWriteAction: true, // NEW: Flag this as a write action
    },
    { 
      id: 2, 
      title: 'Manage Users', 
      description: 'Add, edit, and deactivate users for your organization', 
      icon: Users, 
      link: '/corporate-admin/users',
      isWriteAction: true, // NEW: Flag this as a write action
    },
    { 
      id: 3, 
      title: 'Module Configurations', 
      description: 'Adjust settings for subscribed modules (e.g., LG Custody)', 
      icon: Settings, 
      link: '/corporate-admin/module-configs',
      isWriteAction: true, // NEW: Flag this as a write action
    },
  ];

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading Corporate Admin Dashboard...</p>
      </div>
    );
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">Corporate Admin Dashboard</h2>
      
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
        <div className="card flex items-center p-5 space-x-4">
          <div className="p-3 rounded-full bg-blue-100 text-blue-600">
            <FolderKanban className="h-6 w-6" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">Total LG Categories</p>
            <p className="text-2xl font-semibold text-gray-900">{dashboardMetrics.total_lg_categories}</p>
          </div>
        </div>
        <div className="card flex items-center p-5 space-x-4">
          <div className="p-3 rounded-full bg-purple-100 text-purple-600">
            <Users className="h-6 w-6" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">Active Users (Org)</p>
            <p className="text-2xl font-semibold text-gray-900">{dashboardMetrics.total_active_users}</p>
          </div>
        </div>
        <div className="card flex items-center p-5 space-x-4">
          <div className="p-3 rounded-full bg-orange-100 text-orange-600">
            <Settings className="h-6 w-6" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">Pending Approvals</p>
            <p className="text-2xl font-semibold text-gray-900">{dashboardMetrics.pending_approvals}</p>
          </div>
        </div>
      </div>

      <div className="card mb-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Quick Actions</h3>
        <div className="space-y-3">
          {quickActions.map((action) => {
            const isActionDisabled = action.isWriteAction && isGracePeriod;
            const Icon = action.icon;
            
            const actionCard = (
                <div className={`flex items-center p-3 rounded-lg transition-colors duration-200 ${isActionDisabled ? 'bg-gray-100 text-gray-400 cursor-not-allowed' : 'bg-gray-50 hover:bg-gray-100'}`}>
                    <Icon className={`h-5 w-5 mr-3 ${isActionDisabled ? 'text-gray-400' : 'text-blue-600'}`} />
                    <div>
                        <p className={`font-medium ${isActionDisabled ? 'text-gray-400' : 'text-gray-800'}`}>{action.title}</p>
                        <p className={`text-sm ${isActionDisabled ? 'text-gray-400' : 'text-gray-500'}`}>{action.description}</p>
                    </div>
                </div>
            );

            if (isActionDisabled) {
              return (
                <GracePeriodTooltip key={action.id} isGracePeriod={true}>
                  {actionCard}
                </GracePeriodTooltip>
              );
            } else {
              return (
                <Link key={action.id} to={action.link}>
                  {actionCard}
                </Link>
              );
            }
          })}
        </div>
      </div>

      <div className="card mt-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Organization LG Overview (Placeholder)</h3>
        <p className="text-gray-500 italic">This section will show a summary of LGs specific to your organization.</p>
      </div>
    </div>
  );
}

export default CorporateAdminDashboard;