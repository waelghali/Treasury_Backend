import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js'; // Correct path

function GlobalConfigurationForm({ onLogout }) {
  const { id } = useParams(); // Get config ID from URL for editing (will be undefined for new)
  const navigate = useNavigate();

  // State for form fields
  const [formData, setFormData] = useState({
    key: '',
    value_min: '',
    value_max: '',
    value_default: '',
    unit: '',
    description: '',
  });

  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [formTitle, setFormTitle] = useState('Create New Global Configuration');

  // Fetch config data if in edit mode
  useEffect(() => {
    const fetchConfig = async () => {
      if (id) { // If ID exists, we are in edit mode
        setFormTitle('Edit Global Configuration');
        setIsLoading(true);
        setError('');
        try {
          const config = await apiRequest(`/system-owner/global-configurations/${id}`, 'GET');
          setFormData({
            key: config.key || '',
            value_min: config.value_min || '',
            value_max: config.value_max || '',
            value_default: config.value_default || '',
            unit: config.unit || '',
            description: config.description || '',
          });
        } catch (err) {
          console.error('Failed to fetch config for editing:', err);
          setError('Failed to load configuration details for editing. Please try again.');
        } finally {
          setIsLoading(false);
        }
      } else {
        // Not in edit mode, so no data to fetch, just set loading to false
        setIsLoading(false);
        setFormTitle('Create New Global Configuration');
      }
    };

    fetchConfig();
  }, [id]); // Re-run effect if ID changes (e.g., navigating from new to edit)

  // Handle form field changes
  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prevData) => ({
      ...prevData,
      [name]: value,
    }));
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSaving(true);
    setError('');

    try {
      if (id) {
        // Update existing config
        await apiRequest(`/system-owner/global-configurations/${id}`, 'PUT', formData);
        alert('Global configuration updated successfully!');
      } else {
        // Create new config
        await apiRequest('/system-owner/global-configurations', 'POST', formData);
        alert('Global configuration created successfully!');
      }
      navigate('/system-owner/global-configurations'); // Go back to the list after success
    } catch (err) {
      console.error('Error saving configuration:', err);
      setError(`Error saving configuration: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false);
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
          <p className="text-gray-600 mt-2">Loading configuration data...</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">{formTitle}</h2>
      
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <div className="bg-white p-6 rounded-lg shadow-md">
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Key */}
          <div>
            <label htmlFor="key" className="block text-sm font-medium text-gray-700">Configuration Key</label>
            <input
              type="text"
              name="key"
              id="key"
              value={formData.key}
              onChange={handleChange}
              required
              readOnly={!!id} // Key should be read-only when editing existing config
              className={`mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 ${id ? 'bg-gray-100' : ''} `}
            />
          </div>

          {/* Description */}
          <div>
            <label htmlFor="description" className="block text-sm font-medium text-gray-700">Description</label>
            <textarea
              name="description"
              id="description"
              rows="3"
              value={formData.description}
              onChange={handleChange}
              className="mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 shadow-sm:text-sm"
            ></textarea>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Min Value */}
            <div>
              <label htmlFor="value_min" className="block text-sm font-medium text-gray-700">Minimum Value (as string)</label>
              <input
                type="text"
                name="value_min"
                id="value_min"
                value={formData.value_min}
                onChange={handleChange}
                className="mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 shadow-sm:text-sm"
              />
            </div>

            {/* Max Value */}
            <div>
              <label htmlFor="value_max" className="block text-sm font-medium text-gray-700">Maximum Value (as string)</label>
              <input
                type="text"
                name="value_max"
                id="value_max"
                value={formData.value_max}
                onChange={handleChange}
                className="mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 shadow-sm:text-sm"
              />
            </div>

            {/* Default Value */}
            <div>
              <label htmlFor="value_default" className="block text-sm font-medium text-gray-700">Default Value (as string)</label>
              <input
                type="text"
                name="value_default"
                id="value_default"
                value={formData.value_default}
                onChange={handleChange}
                className="mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 shadow-sm:text-sm"
              />
            </div>
          </div>

          {/* Unit */}
          <div>
            <label htmlFor="unit" className="block text-sm font-medium text-gray-700">Unit (e.g., days, percentage)</label>
            <input
              type="text"
              name="unit"
              id="unit"
              value={formData.unit}
              onChange={handleChange}
              className="mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200 shadow-sm:text-sm"
            />
          </div>

          {/* Action Buttons */}
          <div className="flex justify-end space-x-3">
            <button
              type="button"
              onClick={() => navigate('/system-owner/global-configurations')}
              className="inline-flex items-center px-4 py-1 border border-gray-300 text-base font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              disabled={isSaving}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="inline-flex items-center px-4 py-1 border border-transparent text-base font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              disabled={isSaving}
            >
              {isSaving && (
                <svg className="animate-spin -ml-1 mr-2 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
              )}
              {id ? (isSaving ? 'Updating...' : 'Update Configuration') : (isSaving ? 'Creating...' : 'Create Configuration')}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default GlobalConfigurationForm;
