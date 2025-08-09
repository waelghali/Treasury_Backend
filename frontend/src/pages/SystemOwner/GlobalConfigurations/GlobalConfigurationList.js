import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js'; // Correct path
import { PlusCircle, Edit, Trash, RotateCcw } from 'lucide-react'; // Icons

// NOTE: This component no longer includes its own Layout wrapper.
// The Layout (SidebarLayout) is applied at a higher level in App.js via ProtectedLayout.
function GlobalConfigurationList({ onLogout }) { // onLogout is passed from the parent Layout
  const [configs, setConfigs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  // Function to fetch global configurations from the backend
  const fetchGlobalConfigurations = async () => {
    setIsLoading(true);
    setError('');
    try {
      const response = await apiRequest('/system-owner/global-configurations', 'GET');
      setConfigs(response);
    } catch (err) {
      console.error('Failed to fetch global configurations:', err);
      setError('Failed to load global configurations. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch configs when the component mounts
  useEffect(() => {
    fetchGlobalConfigurations();
  }, []);

  // Handle Edit action
  const handleEdit = (configId) => {
    navigate(`/system-owner/global-configurations/edit/${configId}`);
  };

  // Handle Delete (Soft Delete) action
  const handleDelete = async (configId, configKey) => {
    if (window.confirm(`Are you sure you want to soft-delete the configuration "${configKey}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/global-configurations/${configId}`, 'DELETE');
        fetchGlobalConfigurations(); // Refresh the list after deletion
        alert(`Global configuration "${configKey}" soft-deleted successfully.`);
      } catch (err) {
        console.error('Failed to soft-delete global configuration:', err);
        setError(`Failed to soft-delete configuration "${configKey}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  // Handle Restore action
  const handleRestore = async (configId, configKey) => {
    if (window.confirm(`Are you sure you want to restore the configuration "${configKey}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/global-configurations/${configId}/restore`, 'POST');
        fetchGlobalConfigurations(); // Refresh the list after restoration
        alert(`Global configuration "${configKey}" restored successfully.`);
      } catch (err) {
        console.error('Failed to restore global configuration:', err);
        setError(`Failed to restore configuration "${configKey}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  return (
    <div> {/* Root div, no Layout wrapper here */}
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">Global Ranges Configurations</h2>
        <button
          onClick={() => navigate('/system-owner/global-configurations/new')}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
        >
          <PlusCircle className="h-5 w-5 mr-2" /> Add New Configuration
        </button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {isLoading ? (
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-600 mt-2">Loading global configurations...</p>
        </div>
      ) : configs.length === 0 ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No global configurations found. Click "Add New Configuration" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
				<th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-16">
				  Key
				</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Description
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Min Value
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Max Value
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Default Value
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Unit
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status {/* ADDED BACK: Status column */}
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {configs.map((config) => (
                <tr key={config.id} className={config.is_deleted ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 text-sm font-medium text-gray-900 break-all w-[480px]">
					{config.key}
				  </td>
				  <td className="px-6 py-4 text-sm text-gray-500 w-[320px]">{config.description || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{config.value_min || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{config.value_max || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{config.value_default || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{config.unit || 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {config.is_deleted ? (
                      <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">
                        Deleted
                      </span>
                    ) : (
                      <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">
                        Active
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    {config.is_deleted ? (
                      <button
                        onClick={() => handleRestore(config.id, config.key)}
                        className="text-green-600 hover:text-green-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                        title="Restore Configuration"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </button>
                    ) : (
                      <>
                        <button
                          onClick={() => handleEdit(config.id)}
                          className="text-indigo-600 hover:text-indigo-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                          title="Edit Configuration"
                        >
                          <Edit className="h-5 w-5" />
                        </button>
                        <button
                          onClick={() => handleDelete(config.id, config.key)}
                          className="text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100"
                          title="Delete Configuration"
                        >
                          <Trash className="h-5 w-5" />
                        </button>
                      </>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default GlobalConfigurationList;
