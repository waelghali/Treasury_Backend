import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { Edit, Trash, PlusCircle, RotateCcw } from 'lucide-react';

function SubscriptionPlanList({ onLogout }) {
  const [plans, setPlans] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  // Function to fetch subscription plans from the backend
  const fetchSubscriptionPlans = async () => {
    setIsLoading(true);
    setError('');
    try {
      const response = await apiRequest('/system-owner/subscription-plans', 'GET');
      setPlans(response);
    } catch (err) {
      console.error('Failed to fetch subscription plans:', err);
      setError('Failed to load subscription plans. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch plans when the component mounts
  useEffect(() => {
    fetchSubscriptionPlans();
  }, []);

  // Handle Edit action
  const handleEdit = (planId) => {
    navigate(`/system-owner/subscription-plans/edit/${planId}`);
  };

  // Handle Delete (Soft Delete) action
  const handleDelete = async (planId, planName) => {
    if (window.confirm(`Are you sure you want to soft-delete the plan "${planName}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/subscription-plans/${planId}`, 'DELETE');
        fetchSubscriptionPlans(); // Refresh the list after deletion
        alert(`Subscription plan "${planName}" soft-deleted successfully.`);
      } catch (err) {
        console.error('Failed to soft-delete subscription plan:', err);
        setError(`Failed to soft-delete plan "${planName}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  // Handle Restore action
  const handleRestore = async (planId, planName) => {
    if (window.confirm(`Are you sure you want to restore the plan "${planName}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/subscription-plans/${planId}/restore`, 'POST');
        fetchSubscriptionPlans(); // Refresh the list after restoration
        alert(`Subscription plan "${planName}" restored successfully.`);
      } catch (err) {
        console.error('Failed to restore subscription plan:', err);
        setError(`Failed to restore plan "${planName}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">Subscription Plans</h2>
        <button
          onClick={() => navigate('/system-owner/subscription-plans/new')}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
        >
          <PlusCircle className="h-5 w-5 mr-2" /> Add New Plan
        </button>
      </div>

      {isLoading ? (
        <div className="text-center py-8">
          <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p className="text-gray-600 mt-2">Loading plans...</p>
        </div>
      ) : error ? (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      ) : plans.length === 0 ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No subscription plans found. Click "Add New Plan" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Plan Name
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Duration (Months)
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Monthly Price
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Annual Price
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Max Users
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Max Records
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Features
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {plans.map((plan) => (
                <tr key={plan.id} className={plan.is_deleted ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {plan.name} {plan.is_deleted && <span className="ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Deleted</span>}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{plan.duration_months}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${plan.monthly_price.toFixed(2)}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${plan.annual_price.toFixed(2)}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{plan.max_users}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{plan.max_records}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div className="flex flex-wrap gap-1">
                      {plan.can_maker_checker && <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">M/C</span>}
                      {plan.can_multi_entity && <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">Multi-Entity</span>}
                      {plan.can_ai_integration && <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">AI</span>}
                      {plan.can_image_storage && <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">Images</span>}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    {plan.is_deleted ? (
                      <button
                        onClick={() => handleRestore(plan.id, plan.name)}
                        className="text-green-600 hover:text-green-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                        title="Restore Plan"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </button>
                    ) : (
                      <>
                        <button
                          onClick={() => handleEdit(plan.id)}
                          className="text-indigo-600 hover:text-indigo-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                          title="Edit Plan"
                        >
                          <Edit className="h-5 w-5" />
                        </button>
                        <button
                          onClick={() => handleDelete(plan.id, plan.name)}
                          className="text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100"
                          title="Delete Plan"
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

export default SubscriptionPlanList;
