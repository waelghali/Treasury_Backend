import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService.js';
import { Edit, Save, XCircle, Loader2, AlertCircle, Mail, Trash2, Globe, Tag, Plus, Filter, ChevronDown, ChevronUp } from 'lucide-react';
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

const inputClassNames = "mt-1 block w-full rounded-md border-blue-200 shadow-sm focus:border-blue-500 focus:ring focus:ring-blue-500 focus:ring-opacity-50 text-base p-1";
const labelClassNames = "block text-sm font-medium text-gray-700";
const buttonBaseClassNames = "px-3 py-1 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

function CustomerConfigurationManagementPage({ onLogout, isGracePeriod }) { // NEW: Accept isGracePeriod prop
  const [configurations, setConfigurations] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [editingConfigId, setEditingConfigId] = useState(null);
  const [editValue, setEditValue] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState('');

  const [showEmailSettingsModal, setShowEmailSettingsModal] = useState(false);
  const [emailSettings, setEmailSettings] = useState(null);
  const [isEmailSettingsLoading, setIsEmailSettingsLoading] = useState(true);
  const [emailSettingsError, setEmailSettingsError] = useState('');
  const [isEmailSettingsSaving, setIsEmailSettingsSaving] = useState(false);
  const [emailSettingsForm, setEmailSettingsForm] = useState({
    smtp_host: '',
    smtp_port: 587,
    smtp_username: '',
    smtp_password: '',
    sender_email: '',
    sender_display_name: '',
    is_active: true
  });
  const [isNewSettings, setIsNewSettings] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  const [showEmailListModal, setShowEmailListModal] = useState(false);
  const [editEmailList, setEditEmailList] = useState([]);
  const [newEmail, setNewEmail] = useState('');
  const [emailListError, setEmailListError] = useState('');
  const [currentConfigToEdit, setCurrentConfigToEdit] = useState(null);

  const [sortKey, setSortKey] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');
  const [filterText, setFilterText] = useState('');

  const fetchConfigurations = async () => {
    setIsLoading(true);
    setError('');
    try {
      const response = await apiRequest('/corporate-admin/customer-configurations/', 'GET');
      setConfigurations(response);
    } catch (err) {
      console.error('Failed to fetch customer configurations:', err);
      setError(`Failed to load configurations. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const fetchEmailSettings = async () => {
    setIsEmailSettingsLoading(true);
    setEmailSettingsError('');
    try {
      const response = await apiRequest('/corporate-admin/email-settings/', 'GET');
      if (response) {
        setEmailSettings(response);
        setEmailSettingsForm({
          smtp_host: response.smtp_host,
          smtp_port: response.smtp_port,
          smtp_username: response.smtp_username,
          smtp_password: '',
          sender_email: response.sender_email,
          sender_display_name: response.sender_display_name || '',
          is_active: response.is_active,
        });
        setIsNewSettings(false);
      } else {
        setEmailSettings(null);
        setIsNewSettings(true);
      }
    } catch (err) {
      console.error('Failed to fetch email settings:', err);
      setEmailSettingsError(`Failed to load email settings. ${err.message || 'An unexpected error occurred.'}`);
    } finally {
      setIsEmailSettingsLoading(false);
    }
  };

  useEffect(() => {
    fetchConfigurations();
    fetchEmailSettings();
  }, []);

  const handleEditClick = (config) => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (config.global_config_key === 'COMMON_COMMUNICATION_LIST') {
      setCurrentConfigToEdit(config);
      try {
        const parsed = JSON.parse(config.effective_value);
        setEditEmailList(Array.isArray(parsed) ? parsed : []);
      } catch (e) {
        setEditEmailList([]);
      }
      setShowEmailListModal(true);
    } else {
      setEditingConfigId(config.global_config_id);
      setEditValue(config.configured_value !== null ? String(config.configured_value) : String(config.global_value_default));
      setSaveError('');
    }
  };

  const handleCancelEdit = () => {
    setEditingConfigId(null);
    setEditValue('');
    setSaveError('');
    setEditEmailList([]);
  };

  const handleSave = async (config) => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    setIsSaving(true);
    setSaveError('');

    let valueToSave;

    if (config.global_config_key === 'COMMON_COMMUNICATION_LIST') {
      if (editEmailList.length === 0) {
        setSaveError('The communication list cannot be empty.');
        setIsSaving(false);
        return;
      }
      valueToSave = JSON.stringify(editEmailList);
    } else {
      valueToSave = editValue;
      if (config.global_unit === 'days' || config.global_unit === 'percentage') {
        const parsedValue = parseFloat(editValue);
        if (isNaN(parsedValue)) {
          setSaveError('Value must be a valid number.');
          setIsSaving(false);
          return;
        }
        if (config.global_value_min !== null && parsedValue < parseFloat(config.global_value_min)) {
          setSaveError(`Value must be at least ${config.global_value_min} ${config.global_unit}.`);
          setIsSaving(false);
          return;
        }
        if (config.global_value_max !== null && parsedValue > parseFloat(config.global_value_max)) {
          setSaveError(`Value must be at most ${config.global_value_max} ${config.global_unit}.`);
          setIsSaving(false);
          return;
        }
        valueToSave = String(parsedValue);
      } else if (config.global_unit === 'boolean') {
        if (!['true', 'false'].includes(String(editValue).toLowerCase())) {
          setSaveError('Value must be either "true" or "false".');
          setIsSaving(false);
          return;
        }
        valueToSave = String(editValue).toLowerCase();
      } else {
        valueToSave = String(editValue);
      }
    }

    try {
      await apiRequest(`/corporate-admin/customer-configurations/${config.global_config_key}`, 'PUT', {
        configured_value: valueToSave,
      });
      alert(`Configuration "${config.global_config_key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase())}" updated successfully!`);
      setEditingConfigId(null);
      setEditValue('');
      setEditEmailList([]);
      fetchConfigurations();
    } catch (err) {
      console.error('Failed to save configuration:', err);
      setSaveError(err.message || 'Failed to save configuration.');
    } finally {
      setIsSaving(false);
    }
  };

  const handleEmailSettingsChange = (e) => {
    const { name, value, type, checked } = e.target;
    setEmailSettingsForm(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value,
    }));
  };

  const handleSaveEmailSettings = async () => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    setIsEmailSettingsSaving(true);
    setEmailSettingsError('');

    try {
      const url = emailSettings?.id ? `/corporate-admin/email-settings/${emailSettings.id}` : '/corporate-admin/email-settings/';
      const method = emailSettings?.id ? 'PUT' : 'POST';

      const payload = emailSettingsForm.smtp_password 
        ? emailSettingsForm 
        : { ...emailSettingsForm, smtp_password: null };

      await apiRequest(url, method, payload);
      alert('Email settings saved successfully!');
      setShowEmailSettingsModal(false);
      fetchEmailSettings();
    } catch (err) {
      console.error('Failed to save email settings:', err);
      setEmailSettingsError(err.message || 'Failed to save email settings.');
    } finally {
      setIsEmailSettingsSaving(false);
    }
  };
  
  const handleDeleteEmailSettings = async () => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    if (!emailSettings || !window.confirm("Are you sure you want to delete these email settings? The system will revert to using global settings.")) {
      return;
    }
    setIsEmailSettingsSaving(true);
    setEmailSettingsError('');
    try {
      await apiRequest(`/corporate-admin/email-settings/${emailSettings.id}`, 'DELETE');
      alert('Email settings deleted successfully! The system will now use global settings.');
      setShowEmailSettingsModal(false);
      fetchEmailSettings();
    } catch (err) {
      console.error('Failed to delete email settings:', err);
      setEmailSettingsError(err.message || 'Failed to delete email settings.');
    } finally {
      setIsEmailSettingsSaving(false);
    }
  };

  const handleAddEmail = () => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    const emailRegex = /^\S+@\S+\.\S+$/;
    if (newEmail && emailRegex.test(newEmail) && !editEmailList.includes(newEmail)) {
      setEditEmailList([...editEmailList, newEmail]);
      setNewEmail('');
      setEmailListError('');
    } else if (editEmailList.includes(newEmail)) {
      setEmailListError('Email is already in the list.');
    } else {
      setEmailListError('Please enter a valid email address.');
    }
  };

  const handleRemoveEmail = (emailToRemove) => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    setEditEmailList(editEmailList.filter(email => email !== emailToRemove));
    setEmailListError('');
  };
  
  const handleSaveEmailList = async () => {
    if (isGracePeriod) {
        toast.warn("This action is disabled during your subscription's grace period.");
        return;
    }
    setIsSaving(true);
    setEmailListError('');

    if (editEmailList.length === 0) {
      setEmailListError('The communication list cannot be empty.');
      setIsSaving(false);
      return;
    }

    try {
      await apiRequest(`/corporate-admin/customer-configurations/${currentConfigToEdit.global_config_key}`, 'PUT', {
        configured_value: JSON.stringify(editEmailList),
      });
      alert(`Configuration "${currentConfigToEdit.global_config_key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase())}" updated successfully!`);
      setShowEmailListModal(false);
      setEditEmailList([]);
      setNewEmail('');
      setCurrentConfigToEdit(null);
      fetchConfigurations();
    } catch (err) {
      console.error('Failed to save configuration:', err);
      setEmailListError(err.message || 'Failed to save configuration.');
    } finally {
      setIsSaving(false);
    }
  };

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDirection('asc');
    }
  };
  
  const handleFilterChange = (e) => {
    setFilterText(e.target.value);
  };
  
  const getSortIcon = (key) => {
    if (sortKey !== key) {
      return null;
    }
    return sortDirection === 'asc' ? <ChevronUp className="h-4 w-4 ml-1" /> : <ChevronDown className="h-4 w-4 ml-1" />;
  };

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
        <p className="text-gray-600 mt-2">Loading configurations...</p>
      </div>
    );
  }

  const getEffectiveValue = (config) => {
    if (config.global_config_key === 'COMMON_COMMUNICATION_LIST') {
      try {
        const parsed = JSON.parse(config.effective_value);
        if (Array.isArray(parsed)) {
          return parsed.join(', ');
        }
      } catch (e) {
      }
    }
    return String(config.effective_value);
  };
  
  const getPlaceholderText = (config) => {
      if (config.global_unit === 'boolean') return "true or false";
      if (config.global_unit === 'days') return "e.g., 30";
      if (config.global_unit === 'percentage') return "e.g., 10";
      return "";
  };

  const filteredAndSortedConfigurations = [...configurations]
    .filter(config => 
      config.global_config_key.toLowerCase().includes(filterText.toLowerCase()) ||
      (config.global_description && config.global_description.toLowerCase().includes(filterText.toLowerCase())) ||
      (config.effective_value && String(config.effective_value).toLowerCase().includes(filterText.toLowerCase()))
    )
    .sort((a, b) => {
      if (!sortKey) return 0;
      const aValue = a[sortKey];
      const bValue = b[sortKey];
      
      if (aValue === null || aValue === undefined) return sortDirection === 'asc' ? 1 : -1;
      if (bValue === null || bValue === undefined) return sortDirection === 'asc' ? -1 : 1;
      
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortDirection === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue);
      }
      
      return sortDirection === 'asc' ? aValue - bValue : bValue - aValue;
    });


  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">Module Settings (Customer Configurations)</h2>
        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
            <button
              onClick={() => setShowEmailSettingsModal(true)}
              className={`${buttonBaseClassNames} bg-blue-600 text-white hover:bg-blue-700 flex items-center ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
              disabled={isGracePeriod}
            >
              <Mail className="h-4 w-4 mr-2" />
              Manage Email Settings
            </button>
        </GracePeriodTooltip>
      </div>
      
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
          <AlertCircle className="h-5 w-5 mr-2" />
          <span className="block sm:inline">{error}</span>
        </div>
      )}
      
      <div className="mb-4">
        <div className="flex items-center space-x-2">
          <Filter className="h-5 w-5 text-gray-500" />
          <input
            type="text"
            placeholder="Filter by setting, description, or value..."
            value={filterText}
            onChange={handleFilterChange}
            className={`${inputClassNames} flex-1 max-w-sm`}
            disabled={isGracePeriod}
          />
        </div>
      </div>


      {configurations.length === 0 && !isLoading ? (
        <div className="bg-gray-50 p-6 rounded-lg text-center border border-gray-200">
          <p className="text-gray-500">No configurable settings found for your customer.</p>
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_config_key')}>
                  <div className="flex items-center">
                    Setting
                    {getSortIcon('global_config_key')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_description')}>
                  <div className="flex items-center">
                    Description
                    {getSortIcon('global_description')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_value_min')}>
                  <div className="flex items-center">
                    Min Value
                    {getSortIcon('global_value_min')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_value_max')}>
                  <div className="flex items-center">
                    Max Value
                    {getSortIcon('global_value_max')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_value_default')}>
                  <div className="flex items-center">
                    Default Value
                    {getSortIcon('global_value_default')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('effective_value')}>
                  <div className="flex items-center">
                    Current Value
                    {getSortIcon('effective_value')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer" onClick={() => handleSort('global_unit')}>
                  <div className="flex items-center">
                    Unit
                    {getSortIcon('global_unit')}
                  </div>
                </th>
                <th scope="col" className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredAndSortedConfigurations.map((config) => (
                <tr key={config.global_config_id}>
                  <td className="px-3 py-2 text-sm font-medium text-gray-900">
                    {config.global_config_key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase())}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-500 max-w-xs" title={config.global_description}>
                    {config.global_description || 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-500 text-center">
                    {config.global_value_min !== null ? config.global_value_min : 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-500 text-center">
                    {config.global_value_max !== null ? config.global_value_max : 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-500 text-center">
                    {config.global_value_default !== null ? config.global_value_default : 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-900 text-center">
                    {editingConfigId === config.global_config_id && config.global_config_key !== 'COMMON_COMMUNICATION_LIST' ? (
                      config.global_unit === 'boolean' ? (
                        <select
                          value={editValue}
                          onChange={(e) => setEditValue(e.target.value)}
                          className={`${inputClassNames} w-24 text-center`}
                          autoFocus
                          disabled={isGracePeriod}
                        >
                          <option value="true">True</option>
                          <option value="false">False</option>
                        </select>
                      ) : (
                        <input
                          type="text"
                          value={editValue}
                          onChange={(e) => setEditValue(e.target.value)}
                          className={`${inputClassNames} w-24 text-center`}
                          placeholder={getPlaceholderText(config)}
                          autoFocus
                          disabled={isGracePeriod}
                        />
                      )
                    ) : (
                      <span className="font-semibold">{getEffectiveValue(config)}</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-sm text-gray-500 text-center">
                    {config.global_unit || 'N/A'}
                  </td>
                  <td className="px-3 py-2 text-right text-sm font-medium">
                    {editingConfigId === config.global_config_id && config.global_config_key !== 'COMMON_COMMUNICATION_LIST' ? (
                      <div className="flex items-center justify-end space-x-1">
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                          <button
                            type="button"
                            onClick={() => handleSave(config)}
                            className={`${buttonBaseClassNames} bg-green-600 text-white hover:bg-green-700 ${isSaving || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                            disabled={isSaving || isGracePeriod}
                          >
                            {isSaving ? <Loader2 className="animate-spin h-4 w-4" /> : <Save className="h-4 w-4" />}
                          </button>
                        </GracePeriodTooltip>
                        <button
                          type="button"
                          onClick={handleCancelEdit}
                          className={`${buttonBaseClassNames} bg-gray-200 text-gray-700 hover:bg-gray-300 ${isSaving ? 'opacity-50 cursor-not-allowed' : ''}`}
                          disabled={isSaving}
                        >
                          <XCircle className="h-4 w-4" />
                        </button>
                      </div>
                    ) : (
                      <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                        <button
                          type="button"
                          onClick={() => handleEditClick(config)}
                          className={`${buttonBaseClassNames} bg-blue-500 text-white hover:bg-blue-600 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                          disabled={isGracePeriod}
                        >
                          <Edit className="h-4 w-4" />
                        </button>
                      </GracePeriodTooltip>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {saveError && (
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mt-4 flex items-center" role="alert">
              <AlertCircle className="h-5 w-5 mr-2" />
              <span className="block sm:inline">{saveError}</span>
            </div>
          )}
        </div>
      )}

      {showEmailListModal && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full flex items-center justify-center z-50">
          <div className="relative bg-white p-6 rounded-lg shadow-xl max-w-md w-full">
            <h3 className="text-xl font-semibold mb-4 text-gray-800">
              Edit {currentConfigToEdit?.global_config_key.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase())}
            </h3>
            
            <p className="text-sm text-gray-600 mb-4">{currentConfigToEdit?.global_description}</p>
            
            {emailListError && (
              <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                <AlertCircle className="h-5 w-5 mr-2" />
                <span className="block sm:inline">{emailListError}</span>
              </div>
            )}
            
            <div className={`border border-gray-300 rounded-md p-3 flex flex-wrap gap-2 mb-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
              {editEmailList.map((email, index) => (
                <span key={index} className="inline-flex items-center text-sm font-medium bg-blue-100 text-blue-800 rounded-full py-1 pl-3 pr-2">
                  {email}
                  <button type="button" onClick={() => handleRemoveEmail(email)} className="ml-2 text-blue-500 hover:text-blue-700" disabled={isGracePeriod}>
                    <XCircle className="h-4 w-4" />
                  </button>
                </span>
              ))}
            </div>

            <div className={`flex space-x-2 ${isGracePeriod ? 'opacity-50' : ''}`}>
              <input
                type="email"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        handleAddEmail();
                    }
                }}
                className={`${inputClassNames} flex-1`}
                placeholder="Enter new email address"
                disabled={isGracePeriod}
              />
              <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                <button
                  type="button"
                  onClick={handleAddEmail}
                  className={`${buttonBaseClassNames} bg-blue-500 text-white hover:bg-blue-600 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                  disabled={isGracePeriod}
                >
                  <Plus className="h-4 w-4" />
                </button>
              </GracePeriodTooltip>
            </div>

            <div className="mt-6 flex justify-end space-x-2">
              <button
                type="button"
                onClick={() => {
                  setShowEmailListModal(false);
                  setEditEmailList([]);
                  setNewEmail('');
                  setEmailListError('');
                  setCurrentConfigToEdit(null);
                }}
                className={`${buttonBaseClassNames} bg-gray-200 text-gray-700 hover:bg-gray-300`}
                disabled={isSaving}
              >
                Cancel
              </button>
              <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                <button
                  type="button"
                  onClick={handleSaveEmailList}
                  className={`${buttonBaseClassNames} bg-green-600 text-white hover:bg-green-700 flex items-center ${isSaving || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                  disabled={isSaving || isGracePeriod}
                >
                  {isSaving ? <Loader2 className="animate-spin h-4 w-4" /> : <Save className="h-4 w-4 mr-1" />}
                  Save List
                </button>
              </GracePeriodTooltip>
            </div>
          </div>
        </div>
      )}

      {showEmailSettingsModal && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full flex items-center justify-center z-50">
          <div className="relative bg-white p-6 rounded-lg shadow-xl max-w-md w-full">
            <h3 className="text-xl font-semibold mb-4 text-gray-800">Manage Email Settings</h3>
            <div className="text-sm text-gray-600 mb-4 flex items-center p-3 border border-blue-200 rounded-md bg-blue-50">
              <Globe className="h-5 w-5 mr-2 text-blue-500" />
              <span>
                These settings override the global defaults. If inactive or not configured, the system will use global settings.
              </span>
            </div>
            
            {isEmailSettingsLoading ? (
              <div className="text-center py-8">
                <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
                <p className="text-gray-600 mt-2">Loading settings...</p>
              </div>
            ) : (
              <form className={isGracePeriod ? 'opacity-50 pointer-events-none' : ''}>
                {emailSettingsError && (
                  <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                    <AlertCircle className="h-5 w-5 mr-2" />
                    <span className="block sm:inline">{emailSettingsError}</span>
                  </div>
                )}
                <div className="space-y-4">
                  <div>
                    <label htmlFor="sender_email" className={labelClassNames}>Sender Email</label>
                    <input
                      type="email"
                      id="sender_email"
                      name="sender_email"
                      value={emailSettingsForm.sender_email}
                      onChange={handleEmailSettingsChange}
                      className={inputClassNames}
                      required
                      disabled={isGracePeriod}
                    />
                  </div>
                  <div>
                    <label htmlFor="sender_display_name" className={labelClassNames}>Sender Display Name (Optional)</label>
                    <input
                      type="text"
                      id="sender_display_name"
                      name="sender_display_name"
                      value={emailSettingsForm.sender_display_name}
                      onChange={handleEmailSettingsChange}
                      className={inputClassNames}
                      disabled={isGracePeriod}
                    />
                  </div>
                  <div>
                    <label htmlFor="smtp_host" className={labelClassNames}>SMTP Host</label>
                    <input
                      type="text"
                      id="smtp_host"
                      name="smtp_host"
                      value={emailSettingsForm.smtp_host}
                      onChange={handleEmailSettingsChange}
                      className={inputClassNames}
                      required
                      disabled={isGracePeriod}
                    />
                  </div>
                  <div>
                    <label htmlFor="smtp_port" className={labelClassNames}>SMTP Port</label>
                    <input
                      type="number"
                      id="smtp_port"
                      name="smtp_port"
                      value={emailSettingsForm.smtp_port}
                      onChange={handleEmailSettingsChange}
                      className={inputClassNames}
                      required
                      disabled={isGracePeriod}
                    />
                  </div>
                  <div>
                    <div className="flex justify-between items-center">
                        <label htmlFor="smtp_password" className={labelClassNames}>SMTP Password {isNewSettings ? '' : '(Leave blank to keep existing)'}</label>
                        <button
                          type="button"
                          onClick={() => setShowPassword(!showPassword)}
                          className="text-xs text-blue-500 hover:text-blue-700"
                          disabled={isGracePeriod}
                        >
                          {showPassword ? 'Hide' : 'Show'}
                        </button>
                    </div>
                    <input
                      type={showPassword ? "text" : "password"}
                      id="smtp_password"
                      name="smtp_password"
                      value={emailSettingsForm.smtp_password}
                      onChange={handleEmailSettingsChange}
                      className={inputClassNames}
                      {...(isNewSettings ? { required: true } : {})}
                      disabled={isGracePeriod}
                    />
                  </div>
                  <div className="flex items-center">
                    <input
                      id="is_active"
                      name="is_active"
                      type="checkbox"
                      checked={emailSettingsForm.is_active}
                      onChange={handleEmailSettingsChange}
                      className="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                      disabled={isGracePeriod}
                    />
                    <label htmlFor="is_active" className="ml-2 block text-sm text-gray-900">
                      Activate Custom Settings
                    </label>
                  </div>
                </div>
              </form>
            )}

            <div className="mt-6 flex justify-between items-center">
              <button
                type="button"
                onClick={() => setShowEmailSettingsModal(false)}
                className={`${buttonBaseClassNames} bg-gray-200 text-gray-700 hover:bg-gray-300`}
                disabled={isEmailSettingsSaving}
              >
                Cancel
              </button>
              <div className="flex space-x-2">
                {emailSettings && (
                  <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                    <button
                      type="button"
                      onClick={handleDeleteEmailSettings}
                      className={`${buttonBaseClassNames} bg-red-600 text-white hover:bg-red-700 ${isEmailSettingsSaving || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                      disabled={isEmailSettingsSaving || isGracePeriod}
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </GracePeriodTooltip>
                )}
                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                  <button
                    type="button"
                    onClick={handleSaveEmailSettings}
                    className={`${buttonBaseClassNames} bg-green-600 text-white hover:bg-green-700 flex items-center ${isEmailSettingsSaving || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                    disabled={isEmailSettingsSaving || isGracePeriod}
                  >
                    {isEmailSettingsSaving ? <Loader2 className="animate-spin h-4 w-4" /> : <Save className="h-4 w-4 mr-1" />}
                    Save Settings
                  </button>
                </GracePeriodTooltip>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default CustomerConfigurationManagementPage;