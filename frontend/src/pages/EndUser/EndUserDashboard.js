// In EndUserDashboard.js, around line 55

import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { PlusCircle, FileText, BarChart2, AlertCircle, Save, Loader2, Clock, Printer } from 'lucide-react';
import { apiRequest } from 'services/apiService.js';

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

function EndUserDashboard({ isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const navigate = useNavigate();

  const [dashboardInfo, setDashboardInfo] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchDashboardInfo = async () => {
      setIsLoading(true);
      setError('');
      try {
        const info = await apiRequest('/end-user/users/me_dashboard_info', 'GET');
        setDashboardInfo(info);
      } catch (err) {
        console.error('Failed to fetch dashboard info:', err);
        setError(`Failed to load dashboard data: ${err.message || 'An unexpected error occurred.'}`);
      } finally {
        setIsLoading(false);
      }
    };

    fetchDashboardInfo();
  }, []);

  // NEW: Updated quickActions to conditionally disable the 'Record New LG' action
  const quickActions = [
    {
      title: "Record New LG",
      description: "Initiate the process to record a new Letter of Guarantee.",
      icon: <PlusCircle className="h-6 w-6 text-blue-500" />,
      link: "/end-user/lg-records/new",
      isWriteAction: true,
    },
    {
      title: "View My LGs",
      description: "Access and manage your active Letters of Guarantee.",
      icon: <FileText className="h-6 w-6 text-green-500" />,
      link: "/end-user/lg-records",
      isWriteAction: false,
    },
    {
      title: "View Reports",
      description: "Access various reports and analytics related to LGs.",
      icon: <BarChart2 className="h-6 w-6 text-purple-500" />,
      link: "/end-user/reports",
      isWriteAction: false,
    },
  ];

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
        <p className="text-gray-600 mt-2">Loading dashboard data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
        <AlertCircle className="h-5 w-5 mr-2" />
        <span className="block sm:inline">{error}</span>
      </div>
    );
  }

  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">Welcome to Your Dashboard</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
        <Link
          to="/end-user/lg-records"
          className="bg-blue-50 p-4 rounded-lg shadow-sm flex items-center hover:bg-blue-100 transition-colors duration-200 cursor-pointer"
        >
          <div className="p-3 bg-blue-100 rounded-full mr-4">
            <FileText className="h-6 w-6 text-blue-600" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">Active LGs</p>
            <p className="text-xl font-bold text-gray-900">
              {dashboardInfo?.active_lgs_count !== undefined ? dashboardInfo.active_lgs_count : 'N/A'}
            </p>
          </div>
        </Link>

        <Link
          to="/end-user/action-center"
          className="bg-orange-50 p-4 rounded-lg shadow-sm flex items-center hover:bg-orange-100 transition-colors duration-200 cursor-pointer"
        >
          <div className="p-3 bg-orange-100 rounded-full mr-4">
            <Clock className="h-6 w-6 text-orange-600" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">LGs for Renewal</p>
            <p className="text-xl font-bold text-gray-900">
              {dashboardInfo?.lgs_for_renewal_count !== undefined ? dashboardInfo.lgs_for_renewal_count : 'N/A'}
            </p>
          </div>
        </Link>

        <Link
          to="/end-user/action-center"
          className="bg-purple-50 p-4 rounded-lg shadow-sm flex items-center hover:bg-purple-100 transition-colors duration-200 cursor-pointer"
        >
          <div className="p-3 bg-purple-100 rounded-full mr-4">
            <Printer className="h-6 w-6 text-purple-600" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-500">Pending Print Actions</p>
            <p className="text-xl font-bold text-gray-900">
              {dashboardInfo?.pending_prints_count !== undefined ? dashboardInfo.pending_prints_count : 'N/A'}
            </p>
          </div>
        </Link>
      </div>

      <h3 className="text-xl font-semibold text-gray-800 mb-4">Quick Actions</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {quickActions.map((action, index) => {
          const isActionDisabled = action.isWriteAction && isGracePeriod;
          const linkClasses = `flex items-start p-4 rounded-lg shadow-sm transition-colors duration-200 ${
            isActionDisabled
              ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
              : 'bg-gray-50 hover:bg-gray-100'
          }`;

          const content = (
            <>
              <div className="mr-3 flex-shrink-0">
                {action.icon}
              </div>
              <div>
                <h4 className="font-medium">{action.title}</h4>
                <p className="text-sm">{action.description}</p>
              </div>
            </>
          );

          if (isActionDisabled) {
            return (
              <GracePeriodTooltip key={index} isGracePeriod={true}>
                <div className={linkClasses}>{content}</div>
              </GracePeriodTooltip>
            );
          } else {
            return (
              <Link key={index} to={action.link} className={linkClasses}>
                {content}
              </Link>
            );
          }
        })}
      </div>

      <div className="mt-8 p-4 bg-gray-50 rounded-lg shadow-sm">
        <h3 className="text-xl font-semibold text-gray-800 mb-4">Recent LG Activity</h3>
        <p className="text-gray-600">Coming soon: A list of your most recent LG records and actions.</p>
      </div>
    </div>
  );
}

export default EndUserDashboard;