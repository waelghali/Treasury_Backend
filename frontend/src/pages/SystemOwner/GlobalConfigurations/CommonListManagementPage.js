import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { PlusCircle, Edit, Trash, RotateCcw, Loader2 } from 'lucide-react'; // Added Loader2

// Common input field styling classes
const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200";
const labelClassNames = "block text-sm font-medium text-gray-700";
const requiredSpan = <span className="text-red-500">*</span>;

// Custom Toggle Switch Component (reusable) - Not used directly in this form, but good to have if needed
const ToggleSwitch = ({ id, name, checked, onChange, label }) => (
  <label htmlFor={id} className="relative inline-flex items-center cursor-pointer mb-2">
    <input
      type="checkbox"
      name={name}
      id={id}
      checked={checked}
      onChange={onChange}
      className="sr-only peer"
    />
    <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
    <span className="ml-3 text-sm font-medium text-gray-900">{label}</span>
  </label>
);


// Component to manage any common list (Currencies, LG Types, etc.)
function CommonListManagementPage({ onLogout }) {
  const { listType } = useParams(); // Get list type from URL (e.g., 'currencies', 'lg-types')
  const navigate = useNavigate();

  const [items, setItems] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [formMode, setFormMode] = useState(null); // 'add' or 'edit'
  const [formData, setFormData] = useState({}); // Form data for add/edit
  const [editingItemId, setEditingItemId] = useState(null); // ID of item being edited
  const [isSaving, setIsSaving] = useState(false); // Separate state for save operation

  // Determine API endpoint and title based on listType
  // This configuration defines the fields for the table display and the add/edit form
  const getListConfig = (type) => {
    switch (type) {
      case 'currencies': return {
        endpoint: '/system-owner/currencies',
        title: 'Currencies',
        fields: ['name', 'iso_code', 'symbol'],
        uniqueField: 'iso_code',
      };
      case 'lg-types': return {
        endpoint: '/system-owner/lg-types',
        title: 'LG Types',
        fields: ['name', 'description'],
        uniqueField: 'name',
      };
      case 'rules': return {
        endpoint: '/system-owner/rules',
        title: 'Rules',
        fields: ['name', 'description'],
        uniqueField: 'name',
      };
      case 'issuing-methods': return {
        endpoint: '/system-owner/issuing-methods',
        title: 'Issuing Methods',
        fields: ['name', 'description'],
        uniqueField: 'name',
      };
      case 'lg-statuses': return {
        endpoint: '/system-owner/lg-statuses',
        title: 'LG Statuses',
        fields: ['name', 'description'],
        uniqueField: 'name',
      };
      case 'lg-operational-statuses': return {
        endpoint: '/system-owner/lg-operational-statuses',
        title: 'LG Operational Statuses',
        fields: ['name', 'description'],
        uniqueField: 'name',
      };
      case 'universal-categories': return {
        endpoint: '/system-owner/universal-categories',
        title: 'Universal Categories',
        fields: ['category_name', 'code', 'extra_field_name', 'is_mandatory', 'communication_list'], // Added 'code'
        uniqueField: 'category_name',
      };
      case 'banks': return {
        endpoint: '/system-owner/banks',
        title: 'Banks',
        fields: ['name', 'address', 'phone_number', 'fax', 'former_names', 'swift_code', 'short_name'],
        uniqueField: 'name',
      };
      case 'templates':
        // Templates require a dedicated page. This component is not designed to manage them.
        return {
          endpoint: '/system-owner/templates', // Still provide endpoint for consistency, but will redirect
          title: 'Templates',
          fields: [], // No fields to display here
          uniqueField: 'name',
        };
      default: return { endpoint: '', title: 'Unknown List', fields: [] };
    }
  };

  const currentListConfig = getListConfig(listType);

  // Helper function to get singular name for button text
  const getSingularName = (pluralName) => {
    if (pluralName.endsWith('ies')) {
      return pluralName.slice(0, -3) + 'y';
    } else if (pluralName.endsWith('s') && pluralName.length > 1) {
      return pluralName.slice(0, -1);
    }
    return pluralName;
  };

  const singularListName = getSingularName(currentListConfig.title);


  // Fetch list items from the backend API
  const fetchItems = async () => {
    if (!currentListConfig.endpoint) {
      setError(`Invalid list type "${listType}". Please check the URL.`);
      setIsLoading(false);
      return;
    }
    setIsLoading(true);
    setError('');
    try {
      const response = await apiRequest(currentListConfig.endpoint, 'GET');
      setItems(response);
    } catch (err) {
      console.error(`Failed to fetch ${listType}:`, err);
      setError(`Failed to load ${currentListConfig.title}. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Effect hook to fetch items whenever the listType changes (e.g., navigating between sub-menus)
  useEffect(() => {
    if (listType === 'templates') {
      setIsLoading(false);
      return;
    }
    fetchItems();
  }, [listType]);

  // Handle form field changes for both text inputs, checkboxes, and textareas for arrays
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;

    setFormData((prevData) => {
      if (type === 'checkbox') {
        return { ...prevData, [name]: checked };
      }
      
      if (name === 'communication_list' || name === 'former_names') {
        return { 
          ...prevData, 
          [name]: value.split(',').map(item => item.trim()).filter(item => item !== '') 
        };
      }
      
      return { ...prevData, [name]: value };
    });
  };

  // Handle form submission (Add/Edit) for any list item
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSaving(true); // Set saving state
    setError('');

    const payload = { ...formData };
    
    currentListConfig.fields.forEach(field => {
        if (['is_mandatory', 'is_active', 'is_global'].includes(field)) {
            if (payload[field] === undefined) {
                payload[field] = false;
            }
        }
    });

    if (payload.communication_list && payload.communication_list.length === 0) {
      payload.communication_list = null;
    }
    if (payload.former_names && payload.former_names.length === 0) {
      payload.former_names = null;
    }

    try {
      if (formMode === 'add') {
        await apiRequest(currentListConfig.endpoint, 'POST', payload);
        alert(`${singularListName} created successfully!`);
      } else if (formMode === 'edit') {
        await apiRequest(`${currentListConfig.endpoint}/${editingItemId}`, 'PUT', payload);
        alert(`${singularListName} updated successfully!`);
      }
      setFormMode(null);
      setFormData({});
      setEditingItemId(null);
      fetchItems();
    } catch (err) {
      console.error(`Error saving ${listType}:`, err);
      setError(`Error saving ${currentListConfig.title}: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false); // Reset saving state
    }
  };

  // Handle Delete (Soft Delete) for a list item
  const handleDelete = async (itemId, itemName) => {
    if (window.confirm(`Are you sure you want to soft-delete "${itemName}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`${currentListConfig.endpoint}/${itemId}`, 'DELETE');
        fetchItems();
        alert(`"${itemName}" soft-deleted successfully.`);
      } catch (err) {
        console.error(`Failed to soft-delete ${listType}:`, err);
        setError(`Failed to soft-delete "${itemName}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false);
      }
    }
  };

  // Handle Restore for a soft-deleted list item
  const handleRestore = async (itemId, itemName) => {
    if (window.confirm(`Are you sure you want to restore "${itemName}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`${currentListConfig.endpoint}/${itemId}/restore`, 'POST');
        fetchItems();
        alert(`"${itemName}" restored successfully.`);
      } catch (err) {
        console.error(`Failed to restore ${listType}:`, err);
        setError(`Failed to restore "${itemName}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false);
      }
    }
  };

  // Set form data when entering edit mode, converting array fields to strings for textarea display
  const handleEditClick = (item) => {
    setFormMode('edit');
    setEditingItemId(item.id);
    const transformedData = { ...item };
    if (item.communication_list && Array.isArray(item.communication_list)) {
      transformedData.communication_list = item.communication_list.join(', ');
    }
    if (item.former_names && Array.isArray(item.former_names)) {
      transformedData.former_names = item.former_names.join(', ');
    }
    setFormData(transformedData);
  };

  // Display loading spinner for initial data fetch
  if (isLoading && !items.length && listType !== 'templates') { 
    return (
      <div className="text-center py-8">
        <Loader2 className="h-8 w-8 text-blue-600 mx-auto animate-spin" />
        <p className="text-gray-600 mt-2">Loading {currentListConfig.title}...</p>
      </div>
    );
  }

  // Handle template specific redirect or message
  if (listType === 'templates') {
    return (
      <div>
        <h2 className="text-2xl font-semibold text-gray-800 mb-6">{currentListConfig.title} Management</h2>
        <div className="bg-blue-100 border border-blue-400 text-blue-700 p-3 rounded-md text-sm">
          <p className="font-semibold">Note:</p>
          <p className="mt-1">Template management is a more complex feature due to dynamic content, placeholders, and global/customer-specific variants. It requires a dedicated UI not covered by this generic list manager.</p>
          <p className="mt-1">Please use the appropriate component/page for template management once it is developed.</p>
        </div>
      </div>
    );
  }

  // Handle unknown list types
  if (!currentListConfig.endpoint) {
    return (
        <div>
            <h2 className="text-2xl font-semibold text-gray-800 mb-6">Error: Unknown List Type</h2>
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
                <span className="block sm:inline">The requested list type "{listType}" is not recognized. Please check the URL or navigation link.</span>
            </div>
        </div>
    );
  }


  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">{currentListConfig.title}</h2>
        <button
          onClick={() => { setFormMode('add'); setFormData({}); setEditingItemId(null); setError(''); }}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
        >
          <PlusCircle className="h-5 w-5 mr-2" /> Add New {singularListName}
        </button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {/* Add/Edit Form */}
      {(formMode === 'add' || formMode === 'edit') && (
        <div className="bg-white p-6 rounded-lg shadow-md mb-6">
          <h3 className="text-lg font-medium text-gray-800 mb-4">{formMode === 'add' ? `Add New ${singularListName}` : `Edit ${singularListName}`}</h3>
          <form onSubmit={handleSubmit} className="space-y-4">
            {currentListConfig.fields.map((field) => (
              <div key={field} className="mb-2"> {/* Added mb-2 to each field container */}
                <label htmlFor={field} className={labelClassNames}>
                  {field.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                </label>
                {['description', 'address', 'communication_list', 'former_names'].includes(field) ? (
                  <textarea
                    name={field}
                    id={field}
                    value={Array.isArray(formData[field]) ? formData[field].join(', ') : (formData[field] || '')}
                    onChange={handleChange}
                    rows="2"
                    placeholder={
                        field === 'communication_list' || field === 'former_names'
                            ? "Comma-separated values (e.g., email1@example.com, email2@example.com)"
                            : ""
                    }
                    className={inputClassNames}
                  />
                ) : 
                ['is_mandatory', 'is_active', 'is_global'].includes(field) ? (
                  <ToggleSwitch
                    id={field}
                    name={field}
                    checked={formData[field] || false}
                    onChange={handleChange}
                    label={field.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                  />
                ) : (
                  <input
                    type="text"
                    name={field}
                    id={field}
                    value={formData[field] || ''}
                    onChange={handleChange}
                    required={field === currentListConfig.uniqueField || field === 'name' || field === 'category_name' || field === 'key'}
                    readOnly={formMode === 'edit' && (field === currentListConfig.uniqueField || field === 'key')}
                    className={`${inputClassNames} ${formMode === 'edit' && (field === currentListConfig.uniqueField || field === 'key') ? 'bg-gray-100' : ''}`}
                  />
                )}
              </div>
            ))}
            <div className="flex justify-end space-x-3 mt-4"> {/* Added mt-4 for spacing */}
              <button
                type="button"
                onClick={() => { setFormMode(null); setFormData({}); setError(''); }}
                className="inline-flex items-center px-4 py-1 border border-gray-300 text-base font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              >
                Cancel
              </button>
              <button
                type="submit"
                className="inline-flex items-center px-4 py-1 border border-transparent text-base font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
                disabled={isSaving}
              >
                {isSaving ? (
                  <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                ) : null}
                {formMode === 'add' ? 'Create' : 'Update'} {singularListName}
              </button>
            </div>
          </form>
        </div>
      )}

      {items.length === 0 && !isLoading && currentListConfig.endpoint ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No {currentListConfig.title.toLowerCase()} found. Click "Add New {singularListName}" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {currentListConfig.fields.map((field) => (
                  <th key={field} scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {field.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                  </th>
                ))}
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {items.map((item) => (
                <tr key={item.id} className={item.is_deleted ? 'bg-gray-50 opacity-60' : ''}>
                  {currentListConfig.fields.map((field) => (
                    <td key={field} className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      {['is_mandatory', 'is_active', 'is_global'].includes(field) ? (item[field] ? 'Yes' : 'No') :
                       (['communication_list', 'former_names'].includes(field) ? (Array.isArray(item[field]) ? item[field].join(', ') : (item[field] || 'N/A')) :
                        (item[field] || 'N/A'))}
                    </td>
                  ))}
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {item.is_deleted ? (
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
                    {item.is_deleted ? (
                      <button
                        onClick={() => handleRestore(item.id, item[currentListConfig.uniqueField] || item.name || item.category_name)}
                        className="text-green-600 hover:text-green-900 p-1 rounded-md hover:bg-gray-100"
                        title="Restore"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </button>
                    ) : (
                      <>
                        <button
                          onClick={() => handleEditClick(item)}
                          className="text-indigo-600 hover:text-indigo-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                          title="Edit"
                        >
                          <Edit className="h-5 w-5" />
                        </button>
                        <button
                          onClick={() => handleDelete(item.id, item[currentListConfig.uniqueField] || item.name || item.category_name)}
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

export default CommonListManagementPage;
