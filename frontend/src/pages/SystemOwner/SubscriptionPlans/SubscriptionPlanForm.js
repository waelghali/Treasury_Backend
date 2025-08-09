import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { Loader2 } from 'lucide-react'; // Added Loader2

// Common input field styling classes
const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-200 transition-all duration-200";
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


function SubscriptionPlanForm({ onLogout }) {
  const { id } = useParams();
  const navigate = useNavigate();

  // State for form fields
  const [formData, setFormData] = useState({
    name: '',
    description: '',
    duration_months: 12,
    monthly_price: 0.00,
    annual_price: 0.00,
    max_users: 1,
    max_records: 0,
    can_maker_checker: false,
    can_multi_entity: false,
    can_ai_integration: false,
    can_image_storage: false,
  });

  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [formTitle, setFormTitle] = useState('Create New Subscription Plan');

  // Fetch plan data if in edit mode
  useEffect(() => {
    const fetchPlan = async () => {
      if (id) { // If ID exists, we are in edit mode
        setFormTitle('Edit Subscription Plan');
        setIsLoading(true);
        setError('');
        try {
          const plan = await apiRequest(`/system-owner/subscription-plans/${id}`, 'GET');
          setFormData({
            name: plan.name || '',
            description: plan.description || '',
            duration_months: plan.duration_months || 12,
            monthly_price: plan.monthly_price || 0.00,
            annual_price: plan.annual_price || 0.00,
            max_users: plan.max_users || 1,
            max_records: plan.max_records || 0,
            can_maker_checker: plan.can_maker_checker || false,
            can_multi_entity: plan.can_multi_entity || false,
            can_ai_integration: plan.can_ai_integration || false,
            can_image_storage: plan.can_image_storage || false,
          });
        } catch (err) {
          console.error('Failed to fetch plan for editing:', err);
          setError('Failed to load plan details for editing. Please try again.');
        } finally {
          setIsLoading(false);
        }
      } else {
        // Not in edit mode, so no data to fetch, just set loading to false
        setIsLoading(false);
        setFormTitle('Create New Subscription Plan');
      }
    };

    fetchPlan();
  }, [id]); // Re-run effect if ID changes (e.g., navigating from new to edit)

  // Handle form field changes
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prevData) => ({
      ...prevData,
      [name]: type === 'checkbox' ? checked : value,
    }));
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSaving(true);
    setError('');

    try {
      if (id) {
        // Update existing plan
        await apiRequest(`/system-owner/subscription-plans/${id}`, 'PUT', formData);
        alert('Subscription plan updated successfully!');
      } else {
        // Create new plan
        await apiRequest('/system-owner/subscription-plans', 'POST', formData);
        alert('Subscription plan created successfully!');
      }
      navigate('/system-owner/subscription-plans'); // Go back to the list after success
    } catch (err) {
      console.error('Error saving plan:', err);
      setError(`Error saving plan: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <Loader2 className="h-8 w-8 text-blue-600 mx-auto animate-spin" />
        <p className="text-gray-600 mt-2">Loading plan data...</p>
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
          {/* Plan Name */}
          <div className="mb-2">
            <label htmlFor="name" className={labelClassNames}>Plan Name {requiredSpan}</label>
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

          {/* Description */}
          <div className="mb-2">
            <label htmlFor="description" className={labelClassNames}>Description</label>
            <textarea
              name="description"
              id="description"
              rows="3"
              value={formData.description}
              onChange={handleChange}
              className={inputClassNames}
            ></textarea>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Duration Months */}
            <div className="mb-2">
              <label htmlFor="duration_months" className={labelClassNames}>Duration (Months) {requiredSpan}</label>
              <input
                type="number"
                name="duration_months"
                id="duration_months"
                value={formData.duration_months}
                onChange={handleChange}
                required
                min="1"
                className={inputClassNames}
              />
            </div>

            {/* Monthly Price */}
            <div className="mb-2">
              <label htmlFor="monthly_price" className={labelClassNames}>Monthly Price {requiredSpan}</label>
              <input
                type="number"
                name="monthly_price"
                id="monthly_price"
                value={formData.monthly_price}
                onChange={handleChange}
                required
                min="0"
                step="0.01"
                className={inputClassNames}
              />
            </div>

            {/* Annual Price */}
            <div className="mb-2">
              <label htmlFor="annual_price" className={labelClassNames}>Annual Price {requiredSpan}</label>
              <input
                type="number"
                name="annual_price"
                id="annual_price"
                value={formData.annual_price}
                onChange={handleChange}
                required
                min="0"
                step="0.01"
                className={inputClassNames}
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Max Users */}
            <div className="mb-2">
              <label htmlFor="max_users" className={labelClassNames}>Max Users {requiredSpan}</label>
              <input
                type="number"
                name="max_users"
                id="max_users"
                value={formData.max_users}
                onChange={handleChange}
                required
                min="1"
                className={inputClassNames}
              />
            </div>

            {/* Max Records */}
            <div className="mb-2">
              <label htmlFor="max_records" className={labelClassNames}>Max Records {requiredSpan}</label>
              <input
                type="number"
                name="max_records"
                id="max_records"
                value={formData.max_records}
                onChange={handleChange}
                required
                min="0"
                className={inputClassNames}
              />
            </div>
          </div>

          {/* Feature Checkboxes (now using ToggleSwitch) */}
          <div className="border border-gray-200 rounded-md p-4 space-y-2">
            <p className="text-base font-medium text-gray-800 mb-2">Features</p>
            <ToggleSwitch
              id="can_maker_checker"
              name="can_maker_checker"
              checked={formData.can_maker_checker}
              onChange={handleChange}
              label="Maker/Checker Workflow"
            />
            <ToggleSwitch
              id="can_multi_entity"
              name="can_multi_entity"
              checked={formData.can_multi_entity}
              onChange={handleChange}
              label="Multi-Entity Support"
            />
            <ToggleSwitch
              id="can_ai_integration"
              name="can_ai_integration"
              checked={formData.can_ai_integration}
              onChange={handleChange}
              label="AI Integration"
            />
            <ToggleSwitch
              id="can_image_storage"
              name="can_image_storage"
              checked={formData.can_image_storage}
              onChange={handleChange}
              label="Image Storage"
            />
          </div>

          {/* Action Buttons */}
          <div className="flex justify-end space-x-3 mt-4">
            <button
              type="button"
              onClick={() => navigate('/system-owner/subscription-plans')}
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
              {id ? (isSaving ? 'Updating...' : 'Update Plan') : (isSaving ? 'Creating...' : 'Create Plan')}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default SubscriptionPlanForm;
