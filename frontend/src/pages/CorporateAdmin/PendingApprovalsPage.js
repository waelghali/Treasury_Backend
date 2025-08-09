// frontend/src/pages/CorporateAdmin/PendingApprovalsPage.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from '../../services/apiService';
import { Loader2, AlertCircle, Eye, Check, X } from 'lucide-react';
import moment from 'moment';
import { toast } from 'react-toastify';
import ApprovalRequestDetailsModal from '../../components/Modals/ApprovalRequestDetailsModal';

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

const buttonBaseClassNames = "inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

function PendingApprovalsPage({ isGracePeriod }) { // NEW: Accept isGracePeriod prop
    const [approvalRequests, setApprovalRequests] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');
    const [showDetailsModal, setShowDetailsModal] = useState(false);
    const [selectedRequest, setSelectedRequest] = useState(null);

    const fetchApprovalRequests = async () => {
        setIsLoading(true);
        setError('');
        try {
            const response = await apiRequest('/corporate-admin/approval-requests/', 'GET');
            const sortedRequests = response.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            setApprovalRequests(sortedRequests);
        } catch (err) {
            console.error("Failed to fetch approval requests:", err);
            setError(`Failed to load approval requests. ${err.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to load approval requests: ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchApprovalRequests();
    }, []);

    const handleViewDetails = (request) => {
        setSelectedRequest(request);
        setShowDetailsModal(true);
    };

    const handleApprove = async (requestId) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        if (!window.confirm("Are you sure you want to APPROVE this request?")) {
            return;
        }
        try {
            await apiRequest(`/corporate-admin/approval-requests/${requestId}/approve`, 'POST');
            toast.success("Approval request approved successfully!");
            fetchApprovalRequests();
            if (selectedRequest && selectedRequest.id === requestId) {
                setShowDetailsModal(false);
            }
        } catch (err) {
            console.error("Failed to approve request:", err);
            toast.error(`Failed to approve request: ${err.message || 'An unexpected error occurred.'}`);
        }
    };

    const handleReject = async (requestId, reason = "") => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        const rejectionReason = window.prompt("Are you sure you want to REJECT this request? Please provide a reason (optional):", reason);
        if (rejectionReason === null) {
            return;
        }

        try {
            await apiRequest(`/corporate-admin/approval-requests/${requestId}/reject`, 'POST', { reason: rejectionReason });
            toast.success("Approval request rejected successfully!");
            fetchApprovalRequests();
            if (selectedRequest && selectedRequest.id === requestId) {
                setShowDetailsModal(false);
            }
        } catch (err) {
            console.error("Failed to reject request:", err);
            toast.error(`Failed to reject request: ${err.message || 'An unexpected error occurred.'}`);
        }
    };

    const formatActionType = (actionType) => {
        if (!actionType) return 'N/A';
        return actionType.replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
    };

    return (
        <div className="card">
            <div className="flex justify-between items-center mb-6">
                <h2 className="text-2xl font-semibold text-gray-800">Pending Approval Requests</h2>
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
                    <p className="text-gray-600 mt-2">Loading approval requests...</p>
                </div>
            ) : approvalRequests.length === 0 ? (
                <div className="bg-green-100 p-6 rounded-lg text-center border border-green-200">
                    <p className="text-green-700">No pending approval requests at this time. All clear!</p>
                </div>
            ) : (
                <div className="overflow-x-auto rounded-lg shadow">
                    <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                            <tr>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LG Number</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action Type</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Requested By</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Requested On</th>
                                <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                                <th scope="col" className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                            </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                            {approvalRequests.map((request) => (
                                <tr key={request.id}>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                        {request.lg_record?.lg_number ? <a href={`/end-user/lg-records/${request.lg_record_id}`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">{request.lg_record.lg_number}</a> : 'N/A'}
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatActionType(request.action_type)}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {request.maker_user?.email || 'N/A'}
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{moment(request.timestamp).format('DD-MMM-YYYY HH:mm')}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm">
                                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                                            request.status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                                            request.status === 'APPROVED' ? 'bg-green-100 text-green-800' :
                                            request.status === 'REJECTED' ? 'bg-red-100 text-red-800' :
                                            'bg-gray-100 text-gray-800'
                                        }`}>
                                            {request.status}
                                        </span>
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                        <button
                                            onClick={() => handleViewDetails(request)}
                                            className="text-blue-600 hover:text-blue-900 mr-2 p-1 rounded-md hover:bg-gray-100"
                                            title="View Details"
                                        >
                                            <Eye className="h-5 w-5" />
                                        </button>
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleApprove(request.id)}
                                                className={`text-green-600 hover:text-green-900 mr-2 p-1 rounded-md hover:bg-gray-100 ${request.status !== 'PENDING' || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                title="Approve"
                                                disabled={request.status !== 'PENDING' || isGracePeriod}
                                            >
                                                <Check className="h-5 w-5" />
                                            </button>
                                        </GracePeriodTooltip>
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleReject(request.id)}
                                                className={`text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100 ${request.status !== 'PENDING' || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                title="Reject"
                                                disabled={request.status !== 'PENDING' || isGracePeriod}
                                            >
                                                <X className="h-5 w-5" />
                                            </button>
                                        </GracePeriodTooltip>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            {showDetailsModal && selectedRequest && (
                <ApprovalRequestDetailsModal
                    request={selectedRequest}
                    onClose={() => setShowDetailsModal(false)}
                    onApprove={handleApprove}
                    onReject={handleReject}
                    isGracePeriod={isGracePeriod}
                />
            )}
        </div>
    );
}

export default PendingApprovalsPage;