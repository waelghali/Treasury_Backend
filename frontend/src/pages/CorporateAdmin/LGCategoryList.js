import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { PlusCircle, Edit, Trash, RotateCcw } from 'lucide-react';
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
                        <polygon className="fill-current" points="0,0 127.5,127.5 255,0"/>
                    </svg>
                </div>
            </div>
        );
    }
    return children;
};

function LGCategoryList({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const navigate = useNavigate();
  const [categories, setCategories] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');

  const fetchLGCategories = async () => {
    setIsLoading(true);
    setError('');
    try {
      const response = await apiRequest('/corporate-admin/lg-categories/', 'GET');
      setCategories(response);
    } catch (err) {
      console.error('Failed to fetch LG categories:', err);
      setError(`Failed to load LG Categories. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchLGCategories();
  }, []);

  const handleEdit = (category) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (category.type === 'universal') {
      setError("System default categories cannot be edited by Corporate Admins. Please create a new custom category if needed.");
      return;
    }
    navigate(`/corporate-admin/lg-categories/edit/${category.id}`);
  };

  const handleDelete = async (category) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (category.type === 'universal') {
      setError("System default categories (Universal Categories) cannot be deleted by Corporate Admins.");
      return;
    }
    if (window.confirm(`Are you sure you want to soft-delete "${category.category_name}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/corporate-admin/lg-categories/${category.id}`, 'DELETE');
        alert(`LG Category "${category.category_name}" soft-deleted successfully.`);
        fetchLGCategories();
      } catch (err) {
        console.error('Failed to soft-delete LG category:', err);
        setError(`Failed to soft-delete "${category.category_name}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false);
      }
    }
  };

  const handleRestore = async (category) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (category.type === 'universal') {
        setError("System default categories (Universal Categories) cannot be restored as they are never deleted.");
        return;
    }
    if (window.confirm(`Are you sure you want to restore "${category.category_name}"?`)) {
      try {
        setIsLoading(true);
        await apiRequest(`/corporate-admin/lg-categories/${category.id}/restore`, 'POST');
        alert(`LG Category "${category.category_name}" restored successfully.`);
        fetchLGCategories();
      } catch (err) {
        console.error('Failed to restore LG category:', err);
        setError(`Failed to restore "${category.category_name}". ${err.message || 'An unexpected error occurred.'}`);
        setIsLoading(false);
      }
    }
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading LG Categories...</p>
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">LG Categories</h2>
        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
            <button
              onClick={() => navigate('/corporate-admin/lg-categories/new')}
              className={`btn-primary px-4 py-2 flex items-center ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
              disabled={isGracePeriod}
            >
              <PlusCircle className="h-5 w-5 mr-2" /> Add New LG Category
            </button>
        </GracePeriodTooltip>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {categories.length === 0 && !isLoading ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No LG Categories found for your customer. Click "Add New LG Category" to get started.</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white rounded-lg shadow-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Category Name
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Code
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Extra Field Name
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Mandatory?
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Communication List
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Applies To Entities
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
              {categories.map((category) => (
                <tr key={category.id} className={category.is_deleted ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {category.category_name}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {category.code}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 capitalize">
                    {category.type || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {category.extra_field_name || 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {category.is_mandatory ? 'Yes' : 'No'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {Array.isArray(category.communication_list) && category.communication_list.length > 0
                      ? category.communication_list.join(', ')
                      : 'N/A'}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {category.has_all_entity_access ? (
                      <span className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">
                        All Entities
                      </span>
                    ) : (
                      Array.isArray(category.entities_with_access) && category.entities_with_access.length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                          {category.entities_with_access.map(entity => (
                            <span key={entity.id} className="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">
                              {entity.entity_name}
                            </span>
                          ))}
                        </div>
                      ) : (
                        'N/A'
                      )
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {category.is_deleted ? (
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
                    {category.type === 'universal' ? (
                        <span className="text-gray-400">System Default</span>
                    ) : (
                        category.is_deleted ? (
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button
                                    onClick={() => handleRestore(category)}
                                    className={`text-green-600 hover:text-green-900 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    title="Restore"
                                    disabled={isGracePeriod}
                                >
                                    <RotateCcw className="h-5 w-5" />
                                </button>
                            </GracePeriodTooltip>
                        ) : (
                        <>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button
                                    onClick={() => handleEdit(category)}
                                    className={`text-indigo-600 hover:text-indigo-900 mr-3 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    title="Edit"
                                    disabled={isGracePeriod}
                                >
                                    <Edit className="h-5 w-5" />
                                </button>
                            </GracePeriodTooltip>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button
                                    onClick={() => handleDelete(category)}
                                    className={`text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    title="Delete"
                                    disabled={isGracePeriod}
                                >
                                    <Trash className="h-5 w-5" />
                                </button>
                            </GracePeriodTooltip>
                        </>
                        )
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

export default LGCategoryList;