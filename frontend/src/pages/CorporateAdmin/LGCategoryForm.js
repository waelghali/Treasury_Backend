import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { apiRequest } from 'services/apiService.js';
import { Save } from 'lucide-react';

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

function LGCategoryForm({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const { id } = useParams();
  const navigate = useNavigate();

  const [formData, setFormData] = useState({
    category_name: '',
    code: '',
    extra_field_name: '',
    is_mandatory: false,
    communication_list: '',
    has_all_entity_access: true,
  });

  const [allCustomerEntities, setAllCustomerEntities] = useState([]);
  const [selectedEntityIds, setSelectedEntityIds] = useState([]);

  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [formTitle, setFormTitle] = useState('Create New LG Category');
  const [isUniversalCategory, setIsUniversalCategory] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError('');

      try {
        const entitiesResponse = await apiRequest('/corporate-admin/customer-entities/', 'GET');
        setAllCustomerEntities(entitiesResponse);

        if (id) {
          setFormTitle('Edit LG Category');
          const category = await apiRequest(`/corporate-admin/lg-categories/${id}`, 'GET');
          
          if (category.type === 'universal') {
              setIsUniversalCategory(true);
              setError("This is a system default category (Universal Category) and cannot be edited by Corporate Admins.");
              setIsLoading(false);
              return;
          }

          setFormData({
            category_name: category.category_name || '',
            code: category.code || '',
            extra_field_name: category.extra_field_name || '',
            is_mandatory: category.is_mandatory,
            communication_list: Array.isArray(category.communication_list) ? category.communication_list.join(', ') : '',
            has_all_entity_access: category.has_all_entity_access,
          });

          if (!category.has_all_entity_access && category.entities_with_access) {
            setSelectedEntityIds(category.entities_with_access.map(entity => entity.id));
          } else {
            setSelectedEntityIds([]);
          }

        } else {
          setFormTitle('Create New LG Category');
          setFormData(prev => ({ ...prev, has_all_entity_access: true }));
          setSelectedEntityIds([]);
        }
      } catch (err) {
        console.error('Failed to fetch data:', err);
        setError(`Failed to load data. ${err.message || 'An unexpected error occurred.'}`);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [id]);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData((prevData) => {
      if (type === 'checkbox') {
        if (name === 'has_all_entity_access') {
          if (checked) {
            setSelectedEntityIds([]);
          }
          return { ...prevData, [name]: checked };
        }
        return { ...prevData, [name]: checked };
      }
      if (name === 'communication_list') {
        return { ...prevData, [name]: value };
      }
      return { ...prevData, [name]: value };
    });
  };

  const handleEntitySelectionChange = (e) => {
    const entityId = parseInt(e.target.value, 10);
    if (e.target.checked) {
      setSelectedEntityIds((prev) => [...prev, entityId]);
    } else {
      setSelectedEntityIds((prev) => prev.filter((id) => id !== entityId));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (isGracePeriod) { // NEW: Grace period check
      setError("This action is disabled during your subscription's grace period.");
      return;
    }
    setIsSaving(true);
    setError('');

    if (isUniversalCategory) {
        setError("System default categories cannot be updated by Corporate Admins.");
        setIsSaving(false);
        return;
    }

    const payload = { ...formData };
    if (payload.communication_list) {
      payload.communication_list = payload.communication_list.split(',').map(item => item.trim()).filter(item => item !== '');
    } else {
      payload.communication_list = null;
    }
    
    if (payload.code) {
        payload.code = payload.code.toUpperCase();
    }

    if (payload.has_all_entity_access) {
      payload.entity_ids = [];
    } else {
      payload.entity_ids = selectedEntityIds;
      if (selectedEntityIds.length === 0) {
        setError("Please select at least one entity if 'Applies to All Entities' is unchecked.");
        setIsSaving(false);
        return;
      }
    }

    try {
      if (id) {
        await apiRequest(`/corporate-admin/lg-categories/${id}`, 'PUT', payload);
        alert('LG Category updated successfully!');
      } else {
        await apiRequest('/corporate-admin/lg-categories/', 'POST', payload);
        alert('LG Category created successfully!');
      }
      navigate('/corporate-admin/lg-categories');
    } catch (err) {
      console.error('Error saving LG category:', err);
      setError(`Error saving LG Category: ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsSaving(false);
    }
  };

  const isFormDisabled = isUniversalCategory || isGracePeriod;

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading LG category data...</p>
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

      {isUniversalCategory && (
        <div className="bg-yellow-100 border border-yellow-400 text-yellow-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">This is a system default category (Universal Category) and cannot be edited by Corporate Admins.</span>
        </div>
      )}

      <div className={`bg-white p-6 rounded-lg shadow-md ${isFormDisabled ? 'opacity-50 pointer-events-none' : ''}`}>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="category_name" className="block text-sm font-medium text-gray-700">Category Name</label>
            <input
              type="text"
              name="category_name"
              id="category_name"
              value={formData.category_name}
              onChange={handleChange}
              required
              readOnly={isFormDisabled}
              className="mt-1 block w-full rounded-md border border-gray-200 bg-white focus:border-blue-500 focus:ring-2 focus:ring-blue-500 transition-colors duration-200 text-sm"
            />
          </div>

          <div>
            <label htmlFor="code" className="block text-sm font-medium text-gray-700">Code (1-2 Alphanumeric Chars)</label>
            <input
              type="text"
              name="code"
              id="code"
              value={formData.code}
              onChange={handleChange}
              required
              maxLength="2"
              readOnly={isFormDisabled}
              className="mt-1 block w-full rounded-md border border-gray-200 bg-white focus:border-blue-500 focus:ring-2 focus:ring-blue-500 transition-colors duration-200 text-sm uppercase"
            />
          </div>

          <div>
            <label htmlFor="extra_field_name" className="block text-sm font-medium text-gray-700">Extra Field Name (e.g., 'Project Code')</label>
            <input
              type="text"
              name="extra_field_name"
              id="extra_field_name"
              value={formData.extra_field_name}
              onChange={handleChange}
              readOnly={isFormDisabled}
              className="mt-1 block w-full rounded-md border border-gray-200 bg-white focus:border-blue-500 focus:ring-2 focus:ring-blue-500 transition-colors duration-200 text-sm"
            />
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              name="is_mandatory"
              id="is_mandatory"
              checked={formData.is_mandatory}
              onChange={handleChange}
              disabled={isFormDisabled}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="is_mandatory" className="ml-2 block text-sm text-gray-900">
              Is Extra Field Mandatory?
            </label>
          </div>

          <div>
            <label htmlFor="communication_list" className="block text-sm font-medium text-gray-700">Communication List (Comma-separated emails)</label>
            <textarea
              name="communication_list"
              id="communication_list"
              rows="3"
              value={formData.communication_list}
              onChange={handleChange}
              placeholder="e.g., email1@example.com, email2@example.com"
              readOnly={isFormDisabled}
              className="mt-1 block w-full rounded-md border border-gray-200 bg-white focus:border-blue-500 focus:ring-2 focus:ring-blue-500 transition-colors duration-200 text-sm"
            ></textarea>
            <p className="mt-1 text-sm text-gray-500">
              Emails in this list will receive notifications for LGs in this category.
            </p>
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              name="has_all_entity_access"
              id="has_all_entity_access"
              checked={formData.has_all_entity_access}
              onChange={handleChange}
              disabled={isFormDisabled}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="has_all_entity_access" className="ml-2 block text-sm text-gray-900">
              Applies to All Entities for this Customer
            </label>
          </div>

          {!formData.has_all_entity_access && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Select Specific Entities (Leave unchecked to apply to all)
              </label>
              {allCustomerEntities.length > 0 ? (
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3 bg-gray-50 p-4 rounded-md border border-gray-200">
                  {allCustomerEntities.map((entity) => (
                    <div key={entity.id} className="flex items-center">
                      <input
                        type="checkbox"
                        id={`entity-${entity.id}`}
                        value={entity.id}
                        checked={selectedEntityIds.includes(entity.id)}
                        onChange={handleEntitySelectionChange}
                        disabled={isFormDisabled}
                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                      />
                      <label htmlFor={`entity-${entity.id}`} className="ml-2 text-sm text-gray-900">
                        {entity.entity_name} ({entity.code})
                      </label>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-gray-500 mt-1">No entities found for this customer. All categories will apply to all entities by default.</p>
              )}
            </div>
          )}

          <div className="flex justify-end space-x-3">
            <button
              type="button"
              onClick={() => navigate('/corporate-admin/lg-categories')}
              className="btn-secondary px-4 py-2"
              disabled={isSaving}
            >
              Cancel
            </button>
            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
              <button
                type="submit"
                className={`btn-primary px-4 py-2 flex items-center justify-center ${isSaving || isFormDisabled ? 'opacity-50 cursor-not-allowed' : ''}`}
                disabled={isSaving || isFormDisabled}
              >
                {isSaving && (
                  <svg className="animate-spin -ml-1 mr-2 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                )}
                {id ? (isSaving ? 'Updating...' : 'Update LG Category') : (isSaving ? 'Creating...' : 'Create LG Category')}
              </button>
            </GracePeriodTooltip>
          </div>
        </form>
      </div>
    </div>
  );
}

export default LGCategoryForm;