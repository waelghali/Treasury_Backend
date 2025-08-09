import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { apiRequest } from '../../services/apiService';
import { Loader2, PlusCircle } from 'lucide-react';
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

function UserForm({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const { id } = useParams();
  const navigate = useNavigate();

  const [formData, setFormData] = useState({
    email: '',
    password: '',
    role: 'end_user',
    has_all_entity_access: true,
    entity_ids: [],
    must_change_password: true,
  });

  const [customerEntities, setCustomerEntities] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState(null);
  const [formTitle, setFormTitle] = useState('Create New User');
  const [showPasswordFields, setShowPasswordFields] = useState(true);

  useEffect(() => {
    const fetchFormData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const entitiesResponse = await apiRequest('/corporate-admin/customer-entities/', 'GET'); 
        setCustomerEntities(entitiesResponse);

        if (id) {
          setFormTitle('Edit User');
          const userResponse = await apiRequest(`/corporate-admin/users/${id}`, 'GET');
          setFormData({
            email: userResponse.email,
            role: userResponse.role,
            has_all_entity_access: userResponse.has_all_entity_access,
            entity_ids: userResponse.entities_with_access ? userResponse.entities_with_access.map(entity => entity.id) : [],
            must_change_password: userResponse.must_change_password,
            password: '',
          });
          setShowPasswordFields(false);
        } else {
          setFormTitle('Create New User');
          setFormData(prev => ({ ...prev, must_change_password: true }));
          setShowPasswordFields(true);
        }
      } catch (err) {
        console.error("Failed to fetch data:", err);
        setError(err.message || 'Failed to load data for form.');
      } finally {
        setIsLoading(false);
      }
    };

    fetchFormData();
  }, [id]);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value
    }));
  };

  const handleEntityAccessChange = (e) => {
    const { value } = e.target;
    setFormData(prev => ({
      ...prev,
      has_all_entity_access: value === 'all',
      entity_ids: value === 'all' ? [] : prev.entity_ids
    }));
  };

  const handleEntitySelection = (e) => {
    const entityId = parseInt(e.target.value, 10);
    setFormData(prev => {
      const newEntityIds = e.target.checked
        ? [...prev.entity_ids, entityId]
        : prev.entity_ids.filter(id => id !== entityId);
      return { ...prev, entity_ids: newEntityIds };
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (isGracePeriod) { // NEW: Grace period check
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    setIsSaving(true);
    setError(null);

    if (!formData.email || !formData.role) {
      setError('Email and Role are required.');
      setIsSaving(false);
      return;
    }

    if (showPasswordFields) {
      if (!formData.password) {
        setError('Password is required.');
        setIsSaving(false);
        return;
      }
      if (formData.password.length < 8) {
        setError('Password must be at least 8 characters long.');
        setIsSaving(false);
        return;
      }
    }

    if (!formData.has_all_entity_access && formData.entity_ids.length === 0) {
      setError('Please select at least one entity or grant access to all entities.');
      setIsSaving(false);
      return;
    }

    try {
      const payload = { ...formData };
      if (!showPasswordFields || payload.password === '') {
        delete payload.password;
      }
      if (payload.has_all_entity_access) {
        delete payload.entity_ids;
      }
      if (!id && payload.must_change_password === undefined) {
          payload.must_change_password = true;
      }

      if (id) {
        await apiRequest(`/corporate-admin/users/${id}`, 'PUT', payload);
        navigate('/corporate-admin/users', { state: { successMessage: 'User updated successfully!' } });
      } else {
        await apiRequest('/corporate-admin/users/', 'POST', payload);
        navigate('/corporate-admin/users', { state: { successMessage: 'User created successfully!' } });
      }
    } catch (err) {
      console.error("Failed to save user:", err);
      setError(err.message || 'Failed to save user.');
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <Loader2 className="h-12 w-12 animate-spin text-blue-500" />
        <p className="ml-4 text-gray-600">Loading user data...</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-semibold text-gray-800 mb-6">{formTitle}</h1>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      <form onSubmit={handleSubmit} className={`bg-white p-6 rounded-lg shadow-md space-y-6 ${isGracePeriod ? 'opacity-50 pointer-events-none' : ''}`}>
        <div>
          <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-1">Email</label>
          <input
            type="email"
            id="email"
            name="email"
            value={formData.email}
            onChange={handleChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm p-2"
            required
            disabled={!!id || isGracePeriod} // NEW: Disable email edit for existing users or in grace period
          />
        </div>

        <div>
          <label htmlFor="role" className="block text-sm font-medium text-gray-700 mb-1">Role</label>
          <select
            id="role"
            name="role"
            value={formData.role}
            onChange={handleChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm p-2"
            required
            disabled={isGracePeriod} // NEW: Disable role selection in grace period
          >
            <option value="end_user">End User</option>
            <option value="checker">Checker</option>
          </select>
        </div>

        <div className="border-t border-gray-200 pt-4">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-medium text-gray-900">Password</h3>
            {id && (
              <button
                type="button"
                onClick={() => setShowPasswordFields(!showPasswordFields)}
                className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                disabled={isGracePeriod} // NEW: Disable password change toggle
              >
                {showPasswordFields ? 'Hide Password Fields' : 'Change Password'}
              </button>
            )}
          </div>
          {showPasswordFields && (
            <div className="space-y-4">
              <div>
                <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                <input
                  type="password"
                  id="password"
                  name="password"
                  value={formData.password}
                  onChange={handleChange}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm p-2"
                  minLength="8"
                  required={showPasswordFields && !id}
                  disabled={isGracePeriod} // NEW: Disable password input
                />
              </div>
              <div>
                <label htmlFor="confirm_password" className="block text-sm font-medium text-gray-700 mb-1">Confirm Password</label>
                <input
                  type="password"
                  id="confirm_password"
                  name="confirm_password"
                  value={formData.confirm_password || ''}
                  onChange={(e) => {
                    setFormData(prev => ({ ...prev, confirm_password: e.target.value }));
                  }}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm p-2"
                  minLength="8"
                  required={showPasswordFields && !id}
                  disabled={isGracePeriod} // NEW: Disable confirm password input
                />
              </div>
              {id && showPasswordFields && (
                <div className="flex items-center">
                  <input
                    id="must_change_password"
                    name="must_change_password"
                    type="checkbox"
                    checked={formData.must_change_password}
                    onChange={handleChange}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    disabled={isGracePeriod} // NEW: Disable checkbox
                  />
                  <label htmlFor="must_change_password" className="ml-2 block text-sm text-gray-900">
                    Require password change on next login
                  </label>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="border-t border-gray-200 pt-4">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Entity Access</h3>
          <div className="space-y-2">
            <div className="flex items-center">
              <input
                id="access_all_entities"
                name="entity_access_type"
                type="radio"
                value="all"
                checked={formData.has_all_entity_access}
                onChange={handleEntityAccessChange}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                disabled={isGracePeriod} // NEW: Disable radio button
              />
              <label htmlFor="access_all_entities" className="ml-2 block text-sm text-gray-900">
                Access all entities under this customer
              </label>
            </div>
            <div className="flex items-center">
              <input
                id="access_specific_entities"
                name="entity_access_type"
                type="radio"
                value="specific"
                checked={!formData.has_all_entity_access}
                onChange={handleEntityAccessChange}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                disabled={isGracePeriod} // NEW: Disable radio button
              />
              <label htmlFor="access_specific_entities" className="ml-2 block text-sm text-gray-900">
                Access specific entities
              </label>
            </div>
          </div>

          {!formData.has_all_entity_access && (
            <div className="mt-4 border p-4 rounded-md bg-gray-50">
              <p className="block text-sm font-medium text-gray-700 mb-2">Select Entities:</p>
              {customerEntities.length > 0 ? (
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {customerEntities.map(entity => (
                    <div key={entity.id} className="flex items-center">
                      <input
                        id={`entity-${entity.id}`}
                        type="checkbox"
                        value={entity.id}
                        checked={formData.entity_ids.includes(entity.id)}
                        onChange={handleEntitySelection}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                        disabled={isGracePeriod} // NEW: Disable checkbox
                      />
                      <label htmlFor={`entity-${entity.id}`} className="ml-2 text-sm text-gray-900">
                        {entity.entity_name} ({entity.code})
                      </label>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-gray-500">No entities available for this customer.</p>
              )}
            </div>
          )}
        </div>

        <div className="flex justify-end space-x-4 mt-6">
          <button
            type="button"
            onClick={() => navigate('/corporate-admin/users')}
            className={`px-4 py-2 text-sm font-medium text-gray-700 bg-gray-200 rounded-md hover:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 transition-colors duration-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
            disabled={isSaving || isGracePeriod} // NEW: Disable cancel button
          >
            Cancel
          </button>
          <GracePeriodTooltip isGracePeriod={isGracePeriod}>
            <button
              type="submit"
              className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200 disabled:opacity-50 disabled:cursor-not-allowed`}
              disabled={isSaving || isGracePeriod} // NEW: Disable submit button
            >
              {isSaving ? (
                <Loader2 className="h-5 w-5 mr-2 animate-spin" />
              ) : (
                <PlusCircle className="h-5 w-5 mr-2" />
              )}
              {id ? 'Update User' : 'Create User'}
            </button>
          </GracePeriodTooltip>
        </div>
      </form>
    </div>
  );
}

export default UserForm;