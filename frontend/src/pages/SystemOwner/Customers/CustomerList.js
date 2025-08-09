import React, { useState, useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js'; // Use absolute path
import { PlusCircle, Edit, Trash, RotateCcw, Eye } from 'lucide-react'; // Added Eye icon

// NOTE: This component no longer includes its own Layout wrapper.
// The Layout (SidebarLayout) is applied at a higher level in App.js via ProtectedLayout.
function CustomerList({ onLogout }) { // onLogout is passed from the parent Layout
  const navigate = useNavigate();
  const [customers, setCustomers] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  const fetchCustomers = async () => {
    setIsLoading(true);
    setError('');
    try {
      // Fetch customers, including their relations (entities, users, subscription_plan)
      const response = await apiRequest('/system-owner/customers', 'GET');
      setCustomers(response);
    } catch (err) {
      console.error('Failed to fetch customers:', err);
      setError(`Failed to load customers. ${err.message || ''}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchCustomers();
  }, []);

  const handleDelete = async (customerId, customerName) => {
    if (window.confirm(`Are you sure you want to soft-delete the customer "${customerName}"? This will also soft-delete all associated entities and users.`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/customers/${customerId}`, 'DELETE');
        alert(`Customer "${customerName}" soft-deleted successfully.`);
        fetchCustomers(); // Refresh the list
      } catch (err) {
        console.error('Failed to soft-delete customer:', err);
        setError(`Failed to soft-delete customer "${customerName}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  const handleRestore = async (customerId, customerName) => {
    if (window.confirm(`Are you sure you want to restore the customer "${customerName}"? This will also reactivate associated entities and users.`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/system-owner/customers/${customerId}/restore`, 'POST');
        alert(`Customer "${customerName}" restored successfully.`);
        fetchCustomers(); // Refresh the list
      } catch (err) {
        console.error('Failed to restore customer:', err);
        setError(`Failed to restore customer "${customerName}". ${err.message || ''}`);
        setIsLoading(false);
      }
    }
  };

  const handleViewDetails = (customerId) => {
    navigate(`/system-owner/customers/${customerId}/details`);
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading customers...</p>
      </div>
    );
  }

  return (
    <div> {/* Root div, no Layout wrapper here */}
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">Customer Management</h2>
        <button
          onClick={() => navigate('/system-owner/customers/onboard')}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
        >
          <PlusCircle className="h-5 w-5 mr-2" /> Onboard New Customer
        </button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {customers.length === 0 && !isLoading ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No customers found. Click "Onboard New Customer" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Customer Name
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Contact Email
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Plan
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Entities
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Users
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {customers.map((customer) => (
                <tr 
                  key={customer.id} 
                  className={`cursor-pointer hover:bg-blue-50 ${customer.is_deleted ? 'bg-gray-50 opacity-60' : ''}`}
                  onClick={() => !customer.is_deleted && handleViewDetails(customer.id)} // Only clickable if not deleted
                >
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {customer.name} {customer.is_deleted && <span className="ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Deleted</span>}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{customer.contact_email}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{customer.subscription_plan ? customer.subscription_plan.name : 'N/A'}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{customer.entities.length}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{customer.users.length}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    {/* Always show View Details icon */}
                    <button
                      onClick={(e) => { e.stopPropagation(); handleViewDetails(customer.id); }}
                      className="text-blue-600 hover:text-blue-900 mr-3 p-1 rounded-md hover:bg-gray-100"
                      title="View Details"
                    >
                      <Eye className="h-5 w-5" />
                    </button>
                    {customer.is_deleted ? (
                      <button
                        onClick={(e) => { e.stopPropagation(); handleRestore(customer.id, customer.name); }}
                        className="text-green-600 hover:text-green-900 p-1 rounded-md hover:bg-gray-100"
                        title="Restore Customer"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </button>
                    ) : (
                      <button
                        onClick={(e) => { e.stopPropagation(); handleDelete(customer.id, customer.name); }}
                        className="text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100"
                        title="Delete Customer"
                      >
                        <Trash className="h-5 w-5" />
                      </button>
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

export default CustomerList;
