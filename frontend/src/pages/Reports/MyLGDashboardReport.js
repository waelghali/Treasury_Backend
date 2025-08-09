// frontend/src/pages/Reports/MyLGDashboardReport.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService';
import { toast } from 'react-toastify';
import { LayoutDashboard, Clock, FolderKanban, ListTodo } from 'lucide-react';
import LoadingSpinner from 'components/LoadingSpinner';

const MyLGDashboardReport = () => {
  const [reportData, setReportData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchReport = async () => {
      try {
        setLoading(true);
        // FIX: Removed redundant '/reports' prefix from API call
        const response = await apiRequest('/reports/end-user/my-lg-dashboard');
        setReportData(response.data);
        toast.success("Report data loaded successfully!");
      } catch (err) {
        setError(err);
        toast.error(`Error fetching report: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };
    fetchReport();
  }, []);

  if (loading) {
    return <LoadingSpinner />;
  }

  if (error) {
    return <div className="text-red-500">Error: {error.message}</div>;
  }

  if (!reportData) {
    return <div className="text-gray-500">No report data available.</div>;
  }

  const kpiCards = [
    { name: "My LGs", value: reportData.my_lgs_count, icon: <FolderKanban /> },
    { name: "LGs Near Expiry", value: reportData.lgs_near_expiry_count, icon: <Clock /> },
    { name: "Undelivered Instructions", value: reportData.undelivered_instructions_count, icon: <ListTodo /> },
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-3xl font-bold text-gray-800">My LG Dashboard</h2>
      <p className="text-gray-600">A personalized overview of your assigned LGs and pending tasks.</p>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {kpiCards.map((kpi, index) => (
          <div key={index} className="bg-white rounded-xl shadow-md p-6 flex flex-col items-center justify-center text-center">
            <div className="text-blue-600 mb-2">{kpi.icon}</div>
            <p className="text-sm font-medium text-gray-500 mb-1">{kpi.name}</p>
            <h3 className="text-3xl font-bold text-gray-800">{kpi.value}</h3>
          </div>
        ))}
      </div>

      <div className="bg-white rounded-xl shadow-md p-6">
        <h3 className="text-xl font-semibold text-gray-800 mb-4">Recent Actions</h3>
        <ul className="divide-y divide-gray-200">
          {reportData.recent_actions.length > 0 ? (
            reportData.recent_actions.map((action, index) => (
              <li key={index} className="py-4 text-sm text-gray-700">{action}</li>
            ))
          ) : (
            <li className="py-4 text-sm text-gray-500">No recent actions found.</li>
          )}
        </ul>
      </div>
    </div>
  );
};

export default MyLGDashboardReport;