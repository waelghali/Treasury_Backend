// frontend/src/pages/Reports/SystemUsageOverviewReport.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService';
import { BarChart, Users, Building, HardDrive, Mail } from 'lucide-react';
import { toast } from 'react-toastify';
import LoadingSpinner from 'components/LoadingSpinner';

const SystemUsageOverviewReport = () => {
  const [reportData, setReportData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchReport = async () => {
      try {
        setLoading(true);
        // FIX: Removed redundant '/reports' prefix from API call
        const response = await apiRequest('/reports/system-owner/system-usage-overview');
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
    { name: "Total Customers", value: reportData.total_customers, icon: <Building /> },
    { name: "Total Users", value: reportData.total_users, icon: <Users /> },
    { name: "Total LGs Managed", value: reportData.total_lgs_managed, icon: <HardDrive /> },
    { name: "Total Instructions Issued", value: reportData.total_instructions_issued, icon: <BarChart /> },
    { name: "Total Emails Sent", value: reportData.total_emails_sent, icon: <Mail /> },
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-gray-800">System Usage Overview</h2>
      <p className="text-gray-600">A high-level summary of business traction, growth, and adoption.</p>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-6">
        {kpiCards.map((kpi, index) => (
          <div key={index} className="bg-white rounded-xl shadow-md p-6 flex flex-col items-center justify-center text-center">
            <div className="text-blue-600 mb-2">{kpi.icon}</div>
            <p className="text-sm font-medium text-gray-500 mb-1">{kpi.name}</p>
            <h3 className="text-3xl font-bold text-gray-800">{kpi.value}</h3>
          </div>
        ))}
      </div>
    </div>
  );
};

export default SystemUsageOverviewReport;