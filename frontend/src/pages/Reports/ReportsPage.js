// frontend/src/pages/Reports/ReportsPage.js
import React from 'react';
import { Link, Outlet } from 'react-router-dom'; // Import Outlet for nested routes
import { LayoutDashboard, FileText, BarChart, Clock, Truck, Package, Activity, PieChart, Hourglass } from 'lucide-react';

// Report icons mapping for dynamic use
const reportIcons = {
  LayoutDashboard: <LayoutDashboard className="h-5 w-5 text-blue-500" />,
  Clock: <Clock className="h-5 w-5 text-orange-500" />,
  Hourglass: <Hourglass className="h-5 w-5 text-red-500" />,
  Truck: <Truck className="h-5 w-5 text-purple-500" />,
  FileText: <FileText className="h-5 w-5 text-green-500" />,
  Package: <Package className="h-5 w-5 text-indigo-500" />,
  Activity: <Activity className="h-5 w-5 text-yellow-500" />,
  PieChart: <PieChart className="h-5 w-5 text-pink-500" />,
  BarChart: <BarChart className="h-5 w-5 text-blue-500" />,
};

function ReportsPage({ reports }) {
  // If no reports are passed, show a message or redirect.
  if (!reports || reports.length === 0) {
    return (
      <div className="container mx-auto p-4">
        <h2 className="text-3xl font-bold text-gray-800 mb-6">Reports Dashboard</h2>
        <p className="text-gray-600 mb-8">No reports available for your role.</p>
      </div>
    );
  }

  // If there's only one report, we should render the Outlet directly
  // This removes the need for a report selection page
  if (reports.length === 1) {
    return (
      <div className="container mx-auto p-4">
        <Outlet />
      </div>
    );
  }

  // If there's more than one report (e.g., for System Owner in the future),
  // render the grid for selection.
  return (
    <div className="container mx-auto p-4">
      <h2 className="text-3xl font-bold text-gray-800 mb-6">Reports Dashboard</h2>
      <p className="text-gray-600 mb-8">Select a report to view detailed insights.</p>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {reports.map((report, index) => (
          <Link to={report.path} key={index} className="block">
            <div className="bg-white rounded-lg shadow-md p-6 flex items-start space-x-4 hover:shadow-lg transition-shadow duration-200 h-full">
              <div className="flex-shrink-0">
                {reportIcons[report.iconName]}
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-800 mb-1">{report.name}</h3>
                <p className="text-sm text-gray-600">{report.description}</p>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}

export default ReportsPage;