import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService.js';
import { RefreshCcw, Filter, XCircle, ChevronDown, ChevronUp, Loader2, AlertCircle } from 'lucide-react';
import moment from 'moment';
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
            <polygon className="fill-current" points="0,0 127.5,127.5 255,0" />
          </svg>
        </div>
      </div>
    );
  }
  return children;
};

function AuditLogsCorporate({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const [logs, setLogs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [filters, setFilters] = useState({
    user_id: '',
    action_type: '',
    entity_type: '',
    entity_id: '',
    start_date: '',
    end_date: '',
  });
  const [showFilters, setShowFilters] = useState(false);

  const fetchAuditLogs = async () => {
    setIsLoading(true);
    setError('');
    try {
      const queryParams = new URLSearchParams();
      for (const key in filters) {
        if (filters[key]) {
          queryParams.append(key, filters[key]);
        }
      }
      const fetchedLogs = await apiRequest(`/corporate-admin/audit-logs/?${queryParams.toString()}`, 'GET');
      setLogs(fetchedLogs);
    } catch (err) {
      console.error('Failed to fetch audit logs:', err);
      setError(`Failed to load audit logs. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    // Only fetch logs if not in grace period, to prevent unnecessary API calls if filters are disabled.
    // However, the initial fetch should still happen to show the logs.
    fetchAuditLogs();
  }, [filters]);

  const handleFilterChange = (e) => {
    if (isGracePeriod) {
      toast.warn("Filtering is disabled during your subscription's grace period.");
      return;
    }
    const { name, value } = e.target;
    setFilters(prev => ({ ...prev, [name]: value }));
  };

  const handleClearFilters = () => {
    if (isGracePeriod) {
      toast.warn("Clearing filters is disabled during your subscription's grace period.");
      return;
    }
    setFilters({
      user_id: '',
      action_type: '',
      entity_type: '',
      entity_id: '',
      start_date: '',
      end_date: '',
    });
  };

  const formatTimestamp = (timestamp) => {
    return moment(timestamp).format('YYYY-MM-DD HH:mm:ss');
  };

  const renderDetails = (details) => {
    if (!details) return 'N/A';
    try {
      const parsedDetails = typeof details === 'string' ? JSON.parse(details) : details;
      
      if (parsedDetails.ai_token_usage) {
        const usage = parsedDetails.ai_token_usage;
        return (
          <div className="text-xs bg-gray-50 p-2 rounded-md font-mono">
            <p><strong>Action:</strong> {parsedDetails.action_type || 'N/A'}</p>
            {parsedDetails.file_name && <p><strong>File:</strong> {parsedDetails.file_name}</p>}
            <p><strong>OCR Chars:</strong> {usage.ocr_characters}</p>
            <p><strong>Gemini Prompt:</strong> {usage.gemini_prompt_tokens} tokens</p>
            <p><strong>Gemini Completion:</strong> {usage.gemini_completion_tokens} tokens</p>
            {usage.total_pages_processed > 0 && <p><strong>Pages:</strong> {usage.total_pages_processed}</p>}
            {parsedDetails.reason && <p className="text-red-600"><strong>Reason:</strong> {parsedDetails.reason}</p>}
          </div>
        );
      }
      
      return (
        <pre className="text-xs bg-gray-50 p-2 rounded-md overflow-auto max-h-24">
          {JSON.stringify(parsedDetails, null, 2)}
        </pre>
      );
    } catch (e) {
      console.error("Failed to parse audit log details:", e, details);
      return String(details);
    }
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">Audit Logs (Corporate)</h2>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
          <AlertCircle className="h-5 w-5 mr-2" />
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <div className="bg-gray-50 p-4 rounded-lg shadow-sm mb-6">
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="flex items-center text-blue-600 hover:text-blue-800 font-medium mb-4"
        >
          <Filter className="h-5 w-5 mr-2" />
          {showFilters ? 'Hide Filters' : 'Show Filters'}
          {showFilters ? <ChevronUp className="w-4 h-4 ml-2" /> : <ChevronDown className="w-4 h-4 ml-2" />}
        </button>

        {showFilters && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-4">
            <div>
              <label htmlFor="user_id" className="block text-sm font-medium text-gray-700">User ID</label>
              <input
                type="text"
                name="user_id"
                id="user_id"
                value={filters.user_id}
                onChange={handleFilterChange}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
                placeholder="e.g., 1"
                disabled={isGracePeriod} // NEW: Disable input
              />
            </div>
            <div>
              <label htmlFor="action_type" className="block text-sm font-medium text-gray-700">Action Type</label>
              <input
                type="text"
                name="action_type"
                id="action_type"
                value={filters.action_type}
                onChange={handleFilterChange}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
                placeholder="e.g., CREATE_BY_CA"
                disabled={isGracePeriod} // NEW: Disable input
              />
            </div>
            <div>
              <label htmlFor="entity_type" className="block text-sm font-medium text-gray-700">Entity Type</label>
              <input
                type="text"
                name="entity_type"
                id="entity_type"
                value={filters.entity_type}
                onChange={handleFilterChange}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
                placeholder="e.g., User"
                disabled={isGracePeriod} // NEW: Disable input
              />
            </div>
            <div>
              <label htmlFor="entity_id" className="block text-sm font-medium text-gray-700">Entity ID</label>
              <input
                type="text"
                name="entity_id"
                id="entity_id"
                value={filters.entity_id}
                onChange={handleFilterChange}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
                placeholder="e.g., 5"
                disabled={isGracePeriod} // NEW: Disable input
              />
            </div>
          </div>
        )}

        <div className="flex justify-end space-x-2">
          {showFilters && (
            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
              <button
                onClick={handleClearFilters}
                className={`inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                disabled={isGracePeriod} // NEW: Disable button
              >
                <XCircle className="h-5 w-5 mr-2" />
                Clear Filters
              </button>
            </GracePeriodTooltip>
          )}
          <GracePeriodTooltip isGracePeriod={isGracePeriod}>
            <button
              onClick={fetchAuditLogs}
              className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
              disabled={isGracePeriod} // NEW: Disable button
            >
              <RefreshCcw className="h-5 w-5 mr-2" />
              Apply Filters
            </button>
          </GracePeriodTooltip>
        </div>
      </div>

      {isLoading ? (
        <div className="text-center py-8">
          <Loader2 className="h-8 w-8 text-blue-600 mx-auto animate-spin" />
          <p className="text-gray-600 mt-2">Loading audit logs...</p>
        </div>
      ) : logs.length === 0 ? (
        <div className="text-center py-8 text-gray-500">
          No audit logs found for your organization.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg shadow">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">User ID</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action Type</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Entity Type</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Entity ID</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Details</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">IP Address</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {logs.map((log) => (
                <tr key={log.id}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{formatTimestamp(log.created_at)}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{log.user_id}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{log.action_type}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{log.entity_type}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{log.entity_id || 'N/A'}</td>
                  <td className="px-6 py-4 text-sm text-gray-900">
                    {renderDetails(log.details)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{log.ip_address || 'N/A'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default AuditLogsCorporate;