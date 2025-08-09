import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { PlusCircle, Edit, Trash, RotateCcw } from 'lucide-react';

function TemplateList({ onLogout }) {
  const navigate = useNavigate();
  const [templates, setTemplates] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  const fetchTemplates = async () => {
    setIsLoading(true);
    setError('');
    try {
      // The backend endpoint for templates is /system-owner/templates
      const response = await apiRequest('/system-owner/templates', 'GET');
      setTemplates(response);
    } catch (err) {
      console.error('Failed to fetch templates:', err);
      setError(`Failed to load templates. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchTemplates();
  }, []);

  const handleEdit = (templateId) => {
    // Navigate to the template form in edit mode
    navigate(`/system-owner/global-configurations/templates/edit/${templateId}`);
  };

  const handleDelete = async (templateId, templateName) => {
    if (window.confirm(`Are you sure you want to soft-delete the template "${templateName}"?`)) {
      try {
        setIsLoading(true); // Show loading during deletion
        await apiRequest(`/system-owner/templates/${templateId}`, 'DELETE');
        alert(`Template "${templateName}" soft-deleted successfully.`);
        fetchTemplates(); // Refresh the list
      } catch (err) {
        console.error('Failed to soft-delete template:', err);
        setError(`Failed to soft-delete template "${templateName}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false); // Hide loading on error
      }
    }
  };

  const handleRestore = async (templateId, templateName) => {
    if (window.confirm(`Are you sure you want to restore the template "${templateName}"?`)) {
      try {
        setIsLoading(true); // Show loading during restoration
        await apiRequest(`/system-owner/templates/${templateId}/restore`, 'POST');
        alert(`Template "${templateName}" restored successfully.`);
        fetchTemplates(); // Refresh the list
      } catch (err) {
        console.error('Failed to restore template:', err);
        setError(`Failed to restore template "${templateName}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false); // Hide loading on error
      }
    }
  };

  if (isLoading) {
    return (
      <div onLogout={onLogout}>
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-600 mt-2">Loading templates...</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">Templates Management</h2>
        <button
          onClick={() => navigate('/system-owner/global-configurations/templates/new')}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
        >
          <PlusCircle className="h-5 w-5 mr-2" /> Add New Template
        </button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {templates.length === 0 && !isLoading ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No templates found. Click "Add New Template" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Name
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Action Type
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Global
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Customer
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {templates.map((template) => (
                <tr key={template.id} className={template.is_deleted ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {template.name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 capitalize">
                    {template.template_type || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 capitalize">
                    {template.action_type || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {template.is_global ? 'Yes' : 'No'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {template.customer ? template.customer.name : 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {template.is_deleted ? (
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
                    {template.is_deleted ? (
                      <button
                        onClick={() => handleRestore(template.id, template.name)}
                        className="text-green-600 hover:text-green-900 p-1 rounded-md hover:bg-gray-100"
                        title="Restore"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </button>
                    ) : (
                      <>
                        <button
                          onClick={() => handleEdit(template.id)}
                          className="text-indigo-600 hover:text-indigo-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                          title="Edit"
                        >
                          <Edit className="h-5 w-5" />
                        </button>
                        <button
                          onClick={() => handleDelete(template.id, template.name)}
                          className="text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100"
                          title="Delete"
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

export default TemplateList;
