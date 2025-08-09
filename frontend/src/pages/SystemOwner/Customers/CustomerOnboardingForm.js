import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { PlusCircle, MinusCircle } from 'lucide-react';

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

function CustomerOnboardingForm({ onLogout }) {
  const navigate = useNavigate();

  const [customerData, setCustomerData] = useState({
    name: '',
    address: '',
    contact_email: '',
    contact_phone: '',
    subscription_plan_id: '',
    initial_corporate_admin: {
      email: '',
      password: '',
      role: 'corporate_admin', // Default role for initial admin
    },
    initial_entities: [{
      entity_name: '',
      code: '',
      contact_person: '',
      contact_email: '',
      is_active: true,
      address: '', // New field
      commercial_register_number: '', // New field
      tax_id: '', // New field
    }],
  });

  const [subscriptionPlans, setSubscriptionPlans] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [selectedPlanCanMultiEntity, setSelectedPlanCanMultiEntity] = useState(false);

  // Fetch subscription plans on component mount
  useEffect(() => {
    const fetchPlans = async () => {
      try {
        const plans = await apiRequest('/system-owner/subscription-plans', 'GET');
        setSubscriptionPlans(plans);
        // Pre-select first plan if available
        if (plans.length > 0) {
          setCustomerData((prev) => ({
            ...prev,
            subscription_plan_id: plans[0].id,
          }));
          setSelectedPlanCanMultiEntity(plans[0].can_multi_entity);
        }
      } catch (err) {
        console.error('Failed to fetch subscription plans for form:', err);
        setError('Failed to load subscription plans. Cannot onboard customer.');
      } finally {
        setIsLoading(false);
      }
    };
    fetchPlans();
  }, []);

  // Effect to update canMultiEntity flag when subscription_plan_id changes
  useEffect(() => {
    const currentPlan = subscriptionPlans.find(
      (plan) => plan.id === customerData.subscription_plan_id
    );
    setSelectedPlanCanMultiEntity(currentPlan ? currentPlan.can_multi_entity : false);
  }, [customerData.subscription_plan_id, subscriptionPlans]);


  // Handle change for main customer fields
  const handleCustomerChange = (e) => {
    const { name, value } = e.target;
    setCustomerData((prev) => ({ ...prev, [name]: value }));
  };

  // Handle change for initial corporate admin fields
  const handleAdminChange = (e) => {
    const { name, value } = e.target;
    setCustomerData((prev) => ({
      ...prev,
      initial_corporate_admin: {
        ...prev.initial_corporate_admin,
        [name]: value,
      },
    }));
  };

  // Handle adding a new entity field
  const handleAddEntity = () => {
    setCustomerData((prev) => ({
      ...prev,
      initial_entities: [
        ...prev.initial_entities, 
        { 
          entity_name: '', 
          code: '', 
          contact_person: '', 
          contact_email: '', 
          is_active: true,
          address: '',
          commercial_register_number: '',
          tax_id: ''
        }
      ],
    }));
  };

  // Handle changing an entity's name or other fields
  const handleEntityChange = (index, e) => {
    const { name, value, type, checked } = e.target;
    const newEntities = [...customerData.initial_entities];
    newEntities[index][name] = type === 'checkbox' ? checked : value;
    setCustomerData((prev) => ({ ...prev, initial_entities: newEntities }));
  };

  // Handle removing an entity field
  const handleRemoveEntity = (index) => {
    if (customerData.initial_entities.length > 1) {
      const newEntities = [...customerData.initial_entities];
      newEntities.splice(index, 1);
      setCustomerData((prev) => ({ ...prev, initial_entities: newEntities }));
    } else {
      setError("Every customer must have at least one entity.");
    }
  };

  // Handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSaving(true);
    setError('');

    try {
      if (customerData.initial_entities.length === 0 || customerData.initial_entities.some(entity => !entity.entity_name.trim())) {
        setError("Every customer must have at least one entity with a name.");
        setIsSaving(false);
        return;
      }
      if (!selectedPlanCanMultiEntity && customerData.initial_entities.length > 1) {
        setError("Selected subscription plan does not support multiple entities.");
        setIsSaving(false);
        return;
      }

      await apiRequest('/system-owner/customers/onboard', 'POST', customerData);
      alert('Customer onboarded successfully!');
      navigate('/system-owner/customers');
    } catch (err) {
      console.error('Error onboarding customer:', err);
      setError(`Onboarding failed: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading data...</p>
      </div>
    );
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold text-gray-800 mb-6">Onboard New Customer</h2>
      
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <div className="bg-white p-6 rounded-lg shadow-md">
        <form onSubmit={handleSubmit}>
          {/* Customer Details Section */}
          <div className="border border-gray-200 rounded-md p-4 mb-6">
            <h3 className="text-lg font-medium text-gray-800 mb-3">Customer Information</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="mb-2">
                <label htmlFor="name" className={labelClassNames}>Organization Name {requiredSpan}</label>
                <input type="text" name="name" id="name" value={customerData.name} onChange={handleCustomerChange} required className={inputClassNames} />
              </div>
              <div className="mb-2">
                <label htmlFor="address" className={labelClassNames}>Address</label>
                <input type="text" name="address" id="address" value={customerData.address} onChange={handleCustomerChange} className={inputClassNames} />
              </div>
              <div className="mb-2">
                <label htmlFor="contact_email" className={labelClassNames}>Contact Email {requiredSpan}</label>
                <input type="email" name="contact_email" id="contact_email" value={customerData.contact_email} onChange={handleCustomerChange} required className={inputClassNames} />
              </div>
              <div className="mb-2">
                <label htmlFor="contact_phone" className={labelClassNames}>Contact Phone</label>
                <input type="text" name="contact_phone" id="contact_phone" value={customerData.contact_phone} onChange={handleCustomerChange} className={inputClassNames} />
              </div>
              <div className="md:col-span-2 mb-2"> {/* Span 2 columns */}
                <label htmlFor="subscription_plan_id" className={labelClassNames}>Subscription Plan {requiredSpan}</label>
                <select
                  name="subscription_plan_id"
                  id="subscription_plan_id"
                  value={customerData.subscription_plan_id}
                  onChange={handleCustomerChange}
                  required
                  className={inputClassNames}
                >
                  {subscriptionPlans.map((plan) => (
                    <option key={plan.id} value={plan.id}>
                      {plan.name} (Max Users: {plan.max_users}, Max Records: {plan.max_records}, Multi-Entity: {plan.can_multi_entity ? 'Yes' : 'No'})
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {/* Initial Corporate Admin Section */}
          <div className="border border-gray-200 rounded-md p-4 mb-6">
            <h3 className="text-lg font-medium text-gray-800 mb-3">Initial Corporate Admin User</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="mb-2">
                <label htmlFor="admin_email" className={labelClassNames}>Admin Email {requiredSpan}</label>
                <input type="email" name="email" id="admin_email" value={customerData.initial_corporate_admin.email} onChange={handleAdminChange} required className={inputClassNames} />
              </div>
              <div className="mb-2">
                <label htmlFor="admin_password" className={labelClassNames}>Admin Password {requiredSpan}</label>
                <input type="password" name="password" id="admin_password" value={customerData.initial_corporate_admin.password} onChange={handleAdminChange} required minLength="8" className={inputClassNames} />
              </div>
            </div>
            <p className="mt-2 text-sm text-gray-500">Note: This user will be created with the 'Corporate Admin' role.</p>
          </div>

          {/* Initial Entities Section */}
          <div className="border border-gray-200 rounded-md p-4 mb-6">
            <h3 className="text-lg font-medium text-gray-800 mb-3">Customer Entities</h3>
            <p className="text-sm text-gray-500 mb-3">Every customer must have at least one entity. More entities can be added if the selected subscription plan supports multi-entity.</p>
            <div className="space-y-3">
              {customerData.initial_entities.map((entity, index) => (
                <div key={index} className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 items-center border border-gray-200 p-3 rounded-md">
                  <div className="mb-2">
                    <label htmlFor={`entity_name_${index}`} className={labelClassNames}>Entity Name {requiredSpan}</label>
                    <input
                      type="text"
                      name="entity_name"
                      id={`entity_name_${index}`}
                      value={entity.entity_name}
                      onChange={(e) => handleEntityChange(index, e)}
                      required
                      className={inputClassNames}
                      placeholder={`Main Entity` + (index > 0 ? ` ${index+1}` : '')}
                    />
                  </div>
                  <div className="mb-2">
                    <label htmlFor={`code_${index}`} className={labelClassNames}>Code (Optional)</label>
                    <input
                      type="text"
                      name="code"
                      id={`code_${index}`}
                      value={entity.code}
                      onChange={(e) => handleEntityChange(index, e)}
                      maxLength="2"
                      className={inputClassNames}
                    />
                  </div>
                  <div className="mb-2">
                    <label htmlFor={`address_${index}`} className={labelClassNames}>Address</label>
                    <input
                      type="text"
                      name="address"
                      id={`address_${index}`}
                      value={entity.address}
                      onChange={(e) => handleEntityChange(index, e)}
                      className={inputClassNames}
                    />
                  </div>
                  <div className="mb-2">
                    <label htmlFor={`commercial_register_number_${index}`} className={labelClassNames}>Commercial Register Number</label>
                    <input
                      type="text"
                      name="commercial_register_number"
                      id={`commercial_register_number_${index}`}
                      value={entity.commercial_register_number}
                      onChange={(e) => handleEntityChange(index, e)}
                      className={inputClassNames}
                    />
                  </div>
                  <div className="mb-2">
                    <label htmlFor={`tax_id_${index}`} className={labelClassNames}>Tax ID</label>
                    <input
                      type="text"
                      name="tax_id"
                      id={`tax_id_${index}`}
                      value={entity.tax_id}
                      onChange={(e) => handleEntityChange(index, e)}
                      className={inputClassNames}
                    />
                  </div>
                  <div className="mb-2">
                    <label htmlFor={`contact_person_${index}`} className={labelClassNames}>Contact Person (Optional)</label>
                    <input
                      type="text"
                      name="contact_person"
                      id={`contact_person_${index}`}
                      value={entity.contact_person}
                      onChange={(e) => handleEntityChange(index, e)}
                      className={inputClassNames}
                    />
                  </div>
                  <div className="flex items-end h-full">
                    <div className="flex-grow mb-2">
                      <label htmlFor={`contact_email_${index}`} className={labelClassNames}>Contact Email (Optional)</label>
                      <input
                        type="email"
                        name="contact_email"
                        id={`contact_email_${index}`}
                        value={entity.contact_email}
                        onChange={(e) => handleEntityChange(index, e)}
                        className={inputClassNames}
                      />
                    </div>
                    {(selectedPlanCanMultiEntity && customerData.initial_entities.length > 1) && (
                      <button
                        type="button"
                        onClick={() => handleRemoveEntity(index)}
                        className="ml-2 p-2 rounded-full text-red-600 hover:bg-red-50 focus:outline-none"
                        title="Remove entity"
                      >
                        <MinusCircle className="h-5 w-5" />
                      </button>
                    )}
                  </div>
                </div>
              ))}
            </div>
            {selectedPlanCanMultiEntity && (
              <button
                type="button"
                onClick={handleAddEntity}
                className="mt-4 inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
              >
                <PlusCircle className="h-4 w-4 mr-2" /> Add Another Entity
              </button>
            )}
            {!selectedPlanCanMultiEntity && customerData.initial_entities.length === 1 && (
               <p className="mt-4 text-sm text-gray-500 italic">Your selected plan supports only one entity. To add more, please upgrade your subscription plan.</p>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex justify-end space-x-3">
            <button
              type="button"
              onClick={() => navigate('/system-owner/customers')}
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
              {isSaving ? 'Onboarding...' : 'Onboard Customer'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default CustomerOnboardingForm;