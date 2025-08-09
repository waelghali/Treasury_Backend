import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { apiRequest } from '../../services/apiService';
import { PlusCircle, Edit, Trash2, RotateCcw, CheckCircle, XCircle, Eye } from 'lucide-react';
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

function UserManagementPage({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const [users, setUsers] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState(null);
  const navigate = useNavigate();

  const fetchUsers = async () => {
    setIsLoading(true);
    setError(null);
    setSuccessMessage(null);
    try {
      const response = await apiRequest('/corporate-admin/users', 'GET');
      const sortedUsers = response.sort((a, b) => {
        if (a.is_deleted !== b.is_deleted) {
          return a.is_deleted ? 1 : -1;
        }
        return a.email.localeCompare(b.email);
      });
      setUsers(sortedUsers);
    } catch (err) {
      console.error("Failed to fetch users:", err);
      setError(err.message || 'Failed to load users.');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const handleEdit = (userId) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    navigate(`/corporate-admin/users/edit/${userId}`);
  };

  const handleDelete = async (userId, userEmail) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (window.confirm(`Are you sure you want to soft-delete user: ${userEmail}?`)) {
      setIsLoading(true);
      setError(null);
      setSuccessMessage(null);
      try {
        await apiRequest(`/corporate-admin/users/${userId}`, 'DELETE');
        setSuccessMessage(`User '${userEmail}' soft-deleted successfully.`);
        fetchUsers();
      } catch (err) {
        console.error("Failed to delete user:", err);
        setError(err.message || `Failed to soft-delete user '${userEmail}'.`);
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handleRestore = async (userId, userEmail) => {
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (window.confirm(`Are you sure you want to restore user: ${userEmail}?`)) {
      setIsLoading(true);
      setError(null);
      setSuccessMessage(null);
      try {
        await apiRequest(`/corporate-admin/users/${userId}/restore`, 'POST');
        setSuccessMessage(`User '${userEmail}' restored successfully.`);
        fetchUsers();
      } catch (err) {
        console.error("Failed to restore user:", err);
        setError(err.message || `Failed to restore user '${userEmail}'.`);
      } finally {
        setIsLoading(false);
      }
    }
  };

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-semibold text-gray-800">Manage Users</h1>
        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
            <Link
              to="/corporate-admin/users/new"
              className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
              disabled={isGracePeriod}
            >
              <PlusCircle className="h-5 w-5 mr-2" />
              Add New User
            </Link>
        </GracePeriodTooltip>
      </div>

      {isLoading && (
        <div className="text-center py-8">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900 mx-auto"></div>
          <p className="text-gray-600 mt-4">Loading users...</p>
        </div>
      )}

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {successMessage && (
        <div className="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{successMessage}</span>
        </div>
      )}

      {!isLoading && users.length === 0 && !error && (
        <div className="bg-blue-100 border border-blue-400 text-blue-700 px-4 py-3 rounded-md relative" role="alert">
          <span className="block sm:inline">No users found for your organization.</span>
        </div>
      )}

      {!isLoading && users.length > 0 && (
        <div className="bg-white shadow-md rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Email
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Role
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Entity Access
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
              {users.map((user) => (
                <tr key={user.id} className={user.is_deleted ? 'bg-gray-50 text-gray-500 italic' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {user.email}
                    {user.must_change_password && (
                      <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                        Password Change Required
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {user.role.replace('_', ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {user.has_all_entity_access ? (
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        All Entities
                      </span>
                    ) : (
                      <div className="flex flex-wrap gap-1">
                        {user.entities_with_access && user.entities_with_access.length > 0 ? (
                          user.entities_with_access.map(entity => (
                            <span key={entity.id} className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                              {entity.entity_name}
                            </span>
                          ))
                        ) : (
                          <span className="text-gray-400">None Specified</span>
                        )}
                      </div>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    {user.is_deleted ? (
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                        <XCircle className="h-3 w-3 mr-1" />
                        Deleted
                      </span>
                    ) : (
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        <CheckCircle className="h-3 w-3 mr-1" />
                        Active
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    <div className="flex items-center space-x-2">
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={() => handleEdit(user.id)}
                                className={`text-blue-600 hover:text-blue-900 p-1 rounded-md hover:bg-blue-50 transition-colors duration-200 ${user.is_deleted || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Edit User"
                                disabled={user.is_deleted || isGracePeriod}
                            >
                                <Edit className="h-5 w-5" />
                            </button>
                        </GracePeriodTooltip>
                      {!user.is_deleted ? (
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={() => handleDelete(user.id, user.email)}
                                className={`text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-red-50 transition-colors duration-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Delete User"
                                disabled={isGracePeriod}
                            >
                                <Trash2 className="h-5 w-5" />
                            </button>
                        </GracePeriodTooltip>
                      ) : (
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={() => handleRestore(user.id, user.email)}
                                className={`text-green-600 hover:text-green-900 p-1 rounded-md hover:bg-green-50 transition-colors duration-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Restore User"
                                disabled={isGracePeriod}
                            >
                                <RotateCcw className="h-5 w-5" />
                            </button>
                        </GracePeriodTooltip>
                      )}
                    </div>
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

export default UserManagementPage;