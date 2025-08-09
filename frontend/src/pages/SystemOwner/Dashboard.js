import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { Users, FileText, TrendingUp, DollarSign, Clock, Calendar, Briefcase, Settings, Loader2, BarChart, Building, HardDrive, Mail } from 'lucide-react';

function SystemOwnerDashboard({ onLogout }) {
  const [dashboardData, setDashboardData] = useState({
    total_active_customers: 0,
    customers_change_percent: '0%',
    total_active_users: 0,
    users_change_percent: '0%',
    total_active_lgs: '0',
    lgs_change_percent: '0%',
    expiring_soon_lgs: '0',
    total_lg_value: '$0',
    recent_activity: [],
  });
  const [systemUsageData, setSystemUsageData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  const MAX_RECENT_ACTIVITIES_DISPLAY = 8;

	useEffect(() => {
	  const fetchAllDashboardData = async () => {
		setIsLoading(true);
		setError('');
		try {
		  const [dashboardMetrics, usageMetrics] = await Promise.all([
			apiRequest('/system-owner/dashboard-metrics', 'GET'),
			apiRequest('/reports/system-owner/system-usage-overview', 'GET')
		  ]);
		  
		  setDashboardData({
			total_active_customers: dashboardMetrics.total_active_customers || 0,
			customers_change_percent: dashboardMetrics.customers_change_percent || '0%',
			total_active_users: dashboardMetrics.total_active_users || 0,
			users_change_percent: dashboardMetrics.users_change_percent || '0%',
			total_active_lgs: dashboardMetrics.total_active_lgs || '0',
			lgs_change_percent: dashboardMetrics.lgs_change_percent || '0%',
			expiring_soon_lgs: dashboardMetrics.expiring_soon_lgs || '0',
			total_lg_value: dashboardMetrics.total_lg_value || '$0',
			recent_activity: dashboardMetrics.recent_activity || [],
		  });
		  
		  setSystemUsageData(usageMetrics.data);
		} catch (err) {
		  console.error('Failed to fetch dashboard data:', err);
		  setError(`Failed to load dashboard data. ${err.message || 'An unexpected error occurred.'}`);
		} finally {
		  setIsLoading(false);
		}
	  };

	  fetchAllDashboardData();
	}, []);
  
	const metricsToDisplay = [
	  {
		id: 1,
		title: 'Active Customers',
		value: dashboardData.total_active_customers,
		change: dashboardData.customers_change_percent,
		icon: Users,
		color: 'blue',
		changeColor: dashboardData.customers_change_percent && dashboardData.customers_change_percent.startsWith('+') ? 'text-green-500' : 'text-red-500'
	  },
	  {
		id: 2,
		title: 'Active Users',
		value: dashboardData.total_active_users,
		change: dashboardData.users_change_percent,
		icon: Users,
		color: 'purple',
		changeColor: dashboardData.users_change_percent && dashboardData.users_change_percent.startsWith('+') ? 'text-green-500' : 'text-red-500'
	  },
	  {
		id: 3,
		title: 'Expiring Soon LGs',
		value: dashboardData.expiring_soon_lgs,
		unit: 'Next 30 days',
		icon: Clock,
		color: 'orange',
	  },
	  {
		id: 4,
		title: 'Total LG Value',
		value: dashboardData.total_lg_value,
		change: dashboardData.lgs_change_percent,
		icon: DollarSign,
		color: 'green',
		changeColor: dashboardData.lgs_change_percent && dashboardData.lgs_change_percent.startsWith('+') ? 'text-green-500' : 'text-red-500'
	  },
	];

  const quickActions = [
    { id: 1, title: 'Add Customer', description: 'Create new customer profile', icon: Users, link: '/system-owner/customers/onboard' },
    { id: 2, title: 'Add New Subscriptions', description: 'Create new subscription plans', icon: Briefcase, link: '/system-owner/subscription-plans/new' },
	{ id: 3, title: 'Add New Template', description: 'Create new template', icon: FileText, link: '/system-owner/global-configurations/Templates/new' },
    { id: 4, title: 'Set Global Configurations', description: 'Define system-wide settings and lists', icon: Settings, link: '/system-owner/global-configurations' },
  ];

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <Loader2 className="h-8 w-8 text-blue-600 mx-auto animate-spin" />
        <p className="text-gray-600 mt-2">Loading dashboard data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative" role="alert">
        <span className="block sm:inline">{error}</span>
      </div>
    );
  }
  
  const kpiCards = systemUsageData ? [
    { name: "Total Customers", value: systemUsageData.total_customers, icon: <Building /> },
    { name: "Total Users", value: systemUsageData.total_users, icon: <Users /> },
    { name: "Total LGs Managed", value: systemUsageData.total_lgs_managed, icon: <HardDrive /> },
    { name: "Total Instructions Issued", value: systemUsageData.total_instructions_issued, icon: <BarChart /> },
    { name: "Total Emails Sent", value: systemUsageData.total_emails_sent, icon: <Mail /> },
  ] : [];

  return (
    <div className="space-y-4">
      {/* Dashboard Metrics Section */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {metricsToDisplay.map((metric) => (
          <div key={metric.id} className="card flex items-center p-3 space-x-3">
            <div className={`p-2 rounded-full bg-${metric.color}-100 text-${metric.color}-600`}>
              <metric.icon className="h-5 w-5" />
            </div>
            <div>
              <p className="text-sm font-medium text-gray-500">{metric.title}</p>
              <p className="text-2xl font-semibold text-gray-900">{metric.value}</p>
              <p className="text-xs text-gray-500 flex items-center">
                {metric.change && (
                  <TrendingUp className={`h-3 w-3 mr-1 ${metric.changeColor}`} />
                )}
                {metric.change || metric.unit}
              </p>
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Recent Activity Card */}
        <div className="lg:col-span-2 card">
          <h3 className="text-lg font-semibold text-gray-800 mb-2">Recent Activity</h3>
          {dashboardData.recent_activity.length > 0 ? (
            <ul className="space-y-1 text-sm">
              {dashboardData.recent_activity.slice(0, MAX_RECENT_ACTIVITIES_DISPLAY).map((activity, index) => (
                <li key={activity.id || index} className="flex items-start text-gray-700">
                  <Calendar className="h-4 w-4 mr-2 text-gray-500 flex-shrink-0 mt-1" />
                  <span>
                    {activity.description} - <span className="text-xs text-gray-500">{new Date(activity.timestamp).toLocaleString()}</span>
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-gray-500 italic text-sm">No recent activity found.</p>
          )}
        </div>

        {/* Quick Actions Card */}
        <div className="lg:col-span-1 card">
          <h3 className="text-lg font-semibold text-gray-800 mb-2">Quick Actions</h3>
          <div className="space-y-1">
            {quickActions.map((action) => (
              <Link
                key={action.id}
                to={action.link}
                className="flex items-center p-2 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors duration-200"
              >
                <action.icon className="h-4 w-4 mr-2 text-blue-600" />
                <div>
                  <p className="font-medium text-gray-800 text-sm">{action.title}</p>
                  <p className="text-xs text-gray-500">{action.description}</p>
                </div>
              </Link>
            ))}
          </div>
        </div>
      </div>
      
      {/* System Usage Overview section, now native to this file */}
      <div className="card mt-4">
        <h2 className="mb-2	text-2xl font-bold text-gray-800">System Usage Overview</h2>
        <p className="text-xs text-gray-600">A high-level summary of business traction, growth, and adoption.</p>
        
        {systemUsageData ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4 mt-4">
            {kpiCards.map((kpi, index) => (
              <div key={index} className="bg-white rounded-xl shadow-md p-4 flex flex-col items-center justify-center text-center">
                <div className="text-blue-600 mb-2">{kpi.icon}</div>
                <p className="text-xs font-medium text-gray-500 mb-1">{kpi.name}</p>
                <h3 className="text-2xl font-bold text-gray-800">{kpi.value}</h3>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-gray-500 italic text-sm mt-4">No system usage data available.</p>
        )}
      </div>

    </div>
  );
}

export default SystemOwnerDashboard;