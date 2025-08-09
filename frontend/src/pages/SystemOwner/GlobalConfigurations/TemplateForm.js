import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { Save, XCircle, Loader2 } from 'lucide-react'; // Added Loader2

// Common input field styling classes
const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200";
const labelClassNames = "block text-sm font-medium text-gray-700";
const requiredSpan = <span className="text-red-500">*</span>;

// Custom Toggle Switch Component (reusable)
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


function TemplateForm({ onLogout }) {
  const { id } = useParams(); // Get template ID from URL for editing (will be undefined for new)
  const navigate = useNavigate();
  const contentTextAreaRef = useRef(null); // Ref for the content textarea to manage cursor position

  const [formData, setFormData] = useState({
    name: '',
    template_type: 'email', // Default to email as a common type
    action_type: '',
    content: '', // For now, a single textarea for content
    is_global: true, // Default to global
    customer_id: null, // Only set if is_global is false
  });

  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [formTitle, setFormTitle] = useState('Create New Template');
  const [customers, setCustomers] = useState([]); // For customer dropdown if is_global is false
  const [placeholders, setPlaceholders] = useState([]); // State for dynamic placeholders


  // Fetch template data if in edit mode
  useEffect(() => {
    const fetchTemplate = async () => {
      if (id) { // If ID exists, we are in edit mode
        setFormTitle('Edit Template');
        setIsLoading(true);
        setError('');
        try {
          const template = await apiRequest(`/system-owner/templates/${id}`, 'GET');
          setFormData({
            name: template.name || '',
            template_type: template.template_type || 'email',
            action_type: template.action_type || '',
            content: template.content || '',
            is_global: template.is_global,
            customer_id: template.customer_id || null,
          });
          // After loading existing template, fetch relevant placeholders
          fetchPlaceholders(template.action_type);
        } catch (err) {
          console.error('Failed to fetch template for editing:', err);
          setError('Failed to load template details for editing. Please try again.');
        } finally {
          setIsLoading(false);
        }
      } else {
        // Not in edit mode, no data to fetch, but still fetch initial placeholders (no action_type filter)
        fetchPlaceholders(null);
        setIsLoading(false);
        setFormTitle('Create New Template');
      }
    };

    fetchTemplate();
  }, [id]); // Re-run effect if ID changes

  // Fetch customers for the dropdown (if not global template)
  useEffect(() => {
    const fetchCustomers = async () => {
      try {
        if (!formData.is_global) { // Only fetch if we are not creating a global template
          const fetchedCustomers = await apiRequest('/system-owner/customers', 'GET');
          setCustomers(fetchedCustomers);
          // If in add mode and not global, and no customer_id selected, default to first customer
          if (!id && !formData.customer_id && fetchedCustomers.length > 0) {
            setFormData(prev => ({ ...prev, customer_id: fetchedCustomers[0].id }));
          }
        }
      } catch (err) {
        console.error('Failed to fetch customers for template assignment:', err);
        setError('Failed to load customer list for template assignment.');
      }
    };

    fetchCustomers();
  }, [formData.is_global, id]); // Re-fetch if is_global changes or entering edit mode


  // NEW: Function to fetch placeholders based on action_type
  const fetchPlaceholders = async (actionType) => {
    try {
      const params = actionType ? `?action_type=${actionType}` : '';
      const fetchedPlaceholders = await apiRequest(`/system-owner/template-placeholders${params}`, 'GET');
      setPlaceholders(fetchedPlaceholders);
    } catch (err) {
      console.error('Failed to fetch placeholders:', err);
      setError('Failed to load available placeholders.');
      setPlaceholders([]); // Clear placeholders on error
    }
  };

  // Handle form field changes
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prevData) => {
      let newState = { ...prevData };
      if (type === 'checkbox') {
        newState[name] = checked;
        // If is_global is unchecked, set customer_id to null initially, and vice-versa
        if (name === 'is_global') {
          newState.customer_id = checked ? null : (customers.length > 0 ? customers[0].id : null);
        }
      } else {
        newState[name] = value;
        // If action_type changes, refetch placeholders
        if (name === 'action_type') {
            fetchPlaceholders(value);
        }
      }
      return newState;
    });
  };

  // NEW: Handle placeholder insertion into textarea
  const handleInsertPlaceholder = (placeholderName) => {
    const textarea = contentTextAreaRef.current;
    if (!textarea) return;

    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const newContent = formData.content.substring(0, start) + placeholderName + formData.content.substring(end);

    setFormData(prev => ({ ...prev, content: newContent }));

    // Manually set cursor position after insertion
    setTimeout(() => {
      textarea.focus();
      textarea.selectionStart = textarea.selectionEnd = start + placeholderName.length;
    }, 0);
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSaving(true);
    setError('');

    // Frontend validation: If not global, customer_id must be selected
    if (!formData.is_global && !formData.customer_id) {
      setError('Please select a customer for non-global templates.');
      setIsSaving(false);
      return;
    }

    try {
      const payload = { ...formData };
      // Ensure customer_id is null if is_global is true, as per backend schema
      if (payload.is_global) {
        payload.customer_id = null;
      }

      if (id) {
        // Update existing template
        await apiRequest(`/system-owner/templates/${id}`, 'PUT', payload);
        alert('Template updated successfully!');
      } else {
        // Create new template
        await apiRequest('/system-owner/templates', 'POST', payload);
        alert('Template created successfully!');
      }
      navigate('/system-owner/global-configurations/templates'); // Go back to the list after success
    } catch (err) {
      console.error('Error saving template:', err);
      setError(`Error saving template: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <Loader2 className="h-8 w-8 text-blue-600 mx-auto animate-spin" />
        <p className="text-gray-600 mt-2">Loading template data...</p>
      </div>
    );
  }

  // Define available template types and action types based on BRD for dropdowns
  const templateTypes = ['email', 'letter', 'attachment'];
  const actionTypes = [
    'LG_EXTENSION', 'LG_AMENDMENT', 'LG_LIQUIDATION', 'LG_RELEASE', 'ACTIVATION',
    'REMINDER', 'ACKNOWLEDMENT', 'CANCELLATION', 'LOGIN_SUCCESS', 'CUSTOMER_ONBOARD',
    'ENTITY_DEACTIVATED_BY_PLAN_CHANGE', 'CUSTOMER_PLAN_CHANGE_ENTITY_ADJUSTMENT',
    'CREATE', 'UPDATE', 'DELETE', 'SOFT_DELETE', 'RESTORE'
  ];
  
  // Common input field styling classes
  const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200";
  const labelClassNames = "block text-sm font-medium text-gray-700";
  const requiredSpan = <span className="text-red-500">*</span>;


  return (
    <div>
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">{formTitle}</h2>
      
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Template Form (Left and Middle Column) */}
        <div className="lg:col-span-2 bg-white p-6 rounded-lg shadow-md">
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Template Name */}
            <div className="mb-2">
              <label htmlFor="name" className={labelClassNames}>Template Name {requiredSpan}</label>
              <input
                type="text"
                name="name"
                id="name"
                value={formData.name}
                onChange={handleChange}
                required
                className={inputClassNames}
              />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Template Type */}
              <div className="mb-2">
                <label htmlFor="template_type" className={labelClassNames}>Template Type {requiredSpan}</label>
                <select
                  name="template_type"
                  id="template_type"
                  value={formData.template_type}
                  onChange={handleChange}
                  required
                  className={inputClassNames}
                >
                  {templateTypes.map(type => (
                    <option key={type} value={type}>{type.charAt(0).toUpperCase() + type.slice(1)}</option>
                  ))}
                </select>
              </div>

              {/* Action Type */}
              <div className="mb-2">
                <label htmlFor="action_type" className={labelClassNames}>Action Type {requiredSpan}</label>
                <select
                  name="action_type"
                  id="action_type"
                  value={formData.action_type}
                  onChange={handleChange}
                  required
                  className={inputClassNames}
                >
                  <option value="">Select an Action Type</option>
                  {actionTypes.map(action => (
                    <option key={action} value={action}>{action.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</option>
                  ))}
                </select>
              </div>
            </div>

            {/* Is Global Checkbox */}
            <div className="flex items-center mb-2">
              <ToggleSwitch
                id="is_global"
                name="is_global"
                checked={formData.is_global}
                onChange={handleChange}
                label="Apply to all customers (Global Template)"
              />
            </div>

            {/* Customer Selection (Conditional) */}
            {!formData.is_global && (
              <div className="mb-2">
                <label htmlFor="customer_id" className={labelClassNames}>Assign to Specific Customer {requiredSpan}</label>
                <select
                  name="customer_id"
                  id="customer_id"
                  value={formData.customer_id || ''}
                  onChange={handleChange}
                  required={!formData.is_global}
                  className={inputClassNames}
                >
                  <option value="">Select a Customer</option>
                  {customers.map(customer => (
                    <option key={customer.id} value={customer.id}>{customer.name}</option>
                  ))}
                </select>
                {customers.length === 0 && (
                  <p className="mt-1 text-sm text-red-600">No customers available. Please create a customer first to assign a non-global template.</p>
                )}
              </div>
            )}

            {/* Content Textarea */}
            <div className="mb-2">
              <label htmlFor="content" className={labelClassNames}>Template Content {requiredSpan}</label>
              <textarea
                ref={contentTextAreaRef}
                name="content"
                id="content"
                rows="6"
                value={formData.content}
                onChange={handleChange}
                required
                className={`${inputClassNames} font-mono text-sm`}
                placeholder="Enter your template content here. Use placeholders like {{lg_serial_number}}."
              ></textarea>
              <p className="mt-1 text-sm text-gray-500">
                **Note on Content Structure:** This field currently accepts plain text. For structured content (header, body1, table, body2, footer) or dynamic placeholder insertion, further development is required.
              </p>
            </div>

            {/* Action Buttons */}
            <div className="flex justify-end space-x-3 mt-4">
              <button
                type="button"
                onClick={() => navigate('/system-owner/global-configurations/templates')}
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
                  <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                )}
                {id ? (isSaving ? 'Updating...' : 'Update Template') : (isSaving ? 'Creating...' : 'Create Template')}
              </button>
            </div>
          </form>
        </div>

        {/* Placeholders Section (Right Column) */}
        <div className="lg:col-span-1 bg-white p-6 rounded-lg shadow-md max-h-[600px] overflow-y-auto">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Available Placeholders</h3>
          {formData.action_type ? (
            placeholders.length > 0 ? (
              <ul className="space-y-2">
                {placeholders.map((p, index) => (
                  <li key={p.name || index} className="flex flex-col p-2 bg-gray-50 hover:bg-gray-100 rounded-md cursor-pointer transition-colors duration-150"
                      onClick={() => handleInsertPlaceholder(p.name)}>
                    <span className="font-mono text-sm text-blue-700 font-semibold">{p.name}</span>
                    <span className="text-xs text-gray-600">{p.description}</span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-gray-500 italic">No specific placeholders found for selected action type.</p>
            )
          ) : (
            <p className="text-gray-500 italic">Select an "Action Type" to view relevant placeholders.</p>
          )}
        </div>
      </div>
    </div>
  );
}

export default TemplateForm;
