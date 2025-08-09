// frontend/src/pages/EndUser/ManageInternalOwnersPage.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from '../../services/apiService';
import { Loader2, AlertCircle, PlusCircle, Edit, Users, Eye, Trash2 } from 'lucide-react';
import { toast } from 'react-toastify';
import moment from 'moment';
import { useNavigate } from 'react-router-dom';

import InternalOwnerFormModal from '../../components/Modals/InternalOwnerFormModal';
import BulkChangeLGOwnerModal from '../../components/Modals/BulkChangeLGOwnerModal';

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

const buttonBaseClassNames = "inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

function ManageInternalOwnersPage({ isGracePeriod }) { // NEW: Accept isGracePeriod prop
    const navigate = useNavigate();
    const [owners, setOwners] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');
    const [showFormModal, setShowFormModal] = useState(false);
    const [selectedOwner, setSelectedOwner] = useState(null);

    const [showBulkChangeLGOwnerModal, setShowBulkChangeLGOwnerModal] = useState(false);
    const [selectedOwnerForBulkChange, setSelectedOwnerForBulkChange] = useState(null);

    const fetchInternalOwners = async () => {
        setIsLoading(true);
        setError('');
        try {
            const response = await apiRequest('/end-user/internal-owner-contacts/with-lg-count', 'GET');
            setOwners(response);
        } catch (err) {
            console.error("Failed to fetch internal owner contacts:", err);
            setError(`Failed to load internal owner contacts. ${err.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to load internal owner contacts: ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchInternalOwners();
    }, []);

    const handleCreateNew = () => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        setSelectedOwner(null);
        setShowFormModal(true);
    };

    const handleEdit = (owner) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        setSelectedOwner(owner);
        setShowFormModal(true);
    };

    const handleBulkChangeOwner = (owner) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        setSelectedOwnerForBulkChange(owner);
        setShowBulkChangeLGOwnerModal(true);
    };
    
    const handleViewOwnedLGs = (ownerId) => {
        navigate(`/end-user/lg-records?ownerId=${ownerId}`);
    };

    const handleDelete = async (ownerId) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        if (!window.confirm("Are you sure you want to delete this internal owner contact? This action is irreversible.")) {
            return;
        }
        try {
            await apiRequest(`/end-user/internal-owner-contacts/${ownerId}`, 'DELETE');
            toast.success("Internal owner contact deleted successfully!");
            fetchInternalOwners();
        } catch (err) {
            console.error("Failed to delete internal owner contact:", err);
            toast.error(`Failed to delete contact: ${err.message || 'An unexpected error occurred.'}`);
        }
    };

    const handleFormSuccess = () => {
        setShowFormModal(false);
        setSelectedOwner(null);
        setShowBulkChangeLGOwnerModal(false);
        setSelectedOwnerForBulkChange(null);
        fetchInternalOwners();
    };

    return (
        <div className="card">
            <div className="flex justify-between items-center mb-6">
                <h2 className="text-2xl font-semibold text-gray-800">Manage Internal Owners</h2>
                <div className="flex space-x-3">
                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                        <button
                            onClick={handleCreateNew}
                            className={`${buttonBaseClassNames} bg-blue-600 text-white hover:bg-blue-700 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                            disabled={isGracePeriod} // NEW: Disable button
                        >
                            <PlusCircle className="h-5 w-5 mr-2" />
                            Add New Owner
                        </button>
                    </GracePeriodTooltip>
                </div>
            </div>

            {error && (
                <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                    <AlertCircle className="h-5 w-5 mr-2" />
                    <span className="block sm:inline">{error}</span>
                </div>
            )}

            {isLoading ? (
                <div className="text-center py-8">
                    <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
                    <p className="text-gray-600 mt-2">Loading internal owners...</p>
                </div>
            ) : owners.length === 0 ? (
                <div className="bg-gray-50 p-6 rounded-lg text-center border border-gray-200">
                    <p className="text-gray-500">No internal owner contacts found for your customer.</p>
                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                        <button
                            onClick={handleCreateNew}
                            className={`${buttonBaseClassNames} bg-blue-600 text-white hover:bg-blue-700 mt-4 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                            disabled={isGracePeriod} // NEW: Disable button
                        >
                            Add First Owner
                        </button>
                    </GracePeriodTooltip>
                </div>
            ) : (
                <div className="overflow-x-auto rounded-lg shadow">
                    <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                            <tr>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Phone Number</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Internal ID</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Manager Email</th>
                                <th scope="col" className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">View LGs</th>
                                <th scope="col" className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                            </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                            {owners.map((owner) => (
                                <tr key={owner.id}>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{owner.email}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{owner.phone_number || 'N/A'}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{owner.internal_id || 'N/A'}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{owner.manager_email || 'N/A'}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-center text-sm text-gray-500">
                                        <button
                                            onClick={() => handleViewOwnedLGs(owner.id)}
                                            className={`inline-flex items-center space-x-1 font-semibold
                                                        ${owner.owned_lgs_total_count > 0 ? 'text-blue-600 hover:text-blue-900' : 'text-gray-400 cursor-not-allowed'}`}
                                            title={owner.owned_lgs_total_count > 0 ? "Click to view all LGs for this owner" : "No LGs to display"}
                                            disabled={owner.owned_lgs_total_count === 0}
                                        >
                                            <span>
                                                {owner.owned_lgs_count || 0}
                                                {owner.owned_lgs_total_count > owner.owned_lgs_count ? ` (${owner.owned_lgs_total_count})` : ''}
                                            </span>
                                            <Eye className="h-4 w-4" />
                                        </button>
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleEdit(owner)}
                                                className={`text-indigo-600 hover:text-indigo-900 mr-2 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                title="Edit Owner Details"
                                                disabled={isGracePeriod} // NEW: Disable button
                                            >
                                                <Edit className="h-5 w-5" />
                                            </button>
                                        </GracePeriodTooltip>
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleBulkChangeOwner(owner)}
                                                className={`text-purple-600 hover:text-purple-900 mr-2 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod || owner.owned_lgs_total_count === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                title="Reassign all LGs for this owner"
                                                disabled={isGracePeriod || owner.owned_lgs_total_count === 0} // NEW: Disable button
                                            >
                                                <Users className="h-5 w-5" />
                                            </button>
                                        </GracePeriodTooltip>
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleDelete(owner.id)}
                                                className={`text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                title="Delete Owner"
                                                disabled={isGracePeriod} // NEW: Disable button
                                            >
                                                <Trash2 className="h-5 w-5" />
                                            </button>
                                        </GracePeriodTooltip>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
            
            {showFormModal && (
                <InternalOwnerFormModal
                    owner={selectedOwner}
                    onClose={() => setShowFormModal(false)}
                    onSuccess={handleFormSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop to modal
                />
            )}

            {showBulkChangeLGOwnerModal && selectedOwnerForBulkChange && (
                <BulkChangeLGOwnerModal
                    owner={selectedOwnerForBulkChange}
                    onClose={() => setShowBulkChangeLGOwnerModal(false)}
                    onSuccess={handleFormSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop to modal
                />
            )}
        </div>
    );
}

export default ManageInternalOwnersPage;