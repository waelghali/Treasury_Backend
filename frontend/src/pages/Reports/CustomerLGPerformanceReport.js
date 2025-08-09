// frontend/src/pages/Reports/CustomerLGPerformanceReport.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService';
import { toast } from 'react-toastify';
import LoadingSpinner from 'components/LoadingSpinner';

const CustomerLGPerformanceReport = () => {
  const [reportData, setReportData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchReport = async () => {
      try {
        setLoading(true);
        // FIX: Removed redundant '/reports' prefix from API call
        const response = await apiRequest('/reports/corporate-admin/lg-performance');
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

  const { lgs_by_status, instructions_by_type, total_value_of_active_lgs, users_with_action_counts } = reportData;

  return (
    <div className="space-y-6">
      <h2 className="text-3xl font-bold text-gray-800">Customer LG Performance</h2>
      <p className="text-gray-600">A detailed overview of LG activity and user performance within your organization.</p>
      
      {/* KPI Cards for LG Statuses */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {Object.entries(lgs_by_status).map(([statusName, count]) => (
          <div key={statusName} className="bg-white rounded-xl shadow-md p-6">
            <p className="text-sm font-medium text-gray-500">{statusName.replace(/_/g, ' ').toUpperCase()}</p>
            <h3 className="text-3xl font-bold text-gray-800 mt-1">{count}</h3>
          </div>
        ))}
        <div className="bg-white rounded-xl shadow-md p-6">
          <p className="text-sm font-medium text-gray-500">TOTAL ACTIVE LG VALUE</p>
          <div className="mt-1">
            {Object.entries(total_value_of_active_lgs).map(([currency, value]) => (
			  <h3 key={currency} className="text-xl font-bold text-gray-800 text-right">
				{Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} {currency}
			  </h3>
			))}  	
          </div>
        </div>
      </div>

      {/* Instructions by Type Table */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <h3 className="text-xl font-semibold text-gray-800 mb-4">Instructions Issued by Type</h3>
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Type</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Count</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {Object.entries(instructions_by_type).map(([type, count]) => (
              <tr key={type}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{type.replace(/_/g, ' ')}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* User Activity Table */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <h3 className="text-xl font-semibold text-gray-800 mb-4">User Action Counts</h3>
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">User Email</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action Count</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {Object.entries(users_with_action_counts).map(([email, count]) => (
              <tr key={email}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{email}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default CustomerLGPerformanceReport;