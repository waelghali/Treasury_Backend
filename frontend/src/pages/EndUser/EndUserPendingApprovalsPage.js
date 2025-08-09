// frontend/src/pages/EndUser/EndUserPendingApprovalsPage.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from '../../services/apiService';
import { Loader2, AlertCircle, Eye, XCircle } from 'lucide-react';
import moment from 'moment';
import { toast } from 'react-toastify';
import ApprovalRequestDetailsModal from '../../components/Modals/ApprovalRequestDetailsModal';
import { jwtDecode } from 'jwt-decode';

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

function EndUserPendingApprovalsPage({ isGracePeriod }) { // NEW: Accept isGracePeriod prop
    const [approvalRequests, setApprovalRequests] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');
    const [showDetailsModal, setShowDetailsModal] = useState(false);
    const [selectedRequest, setSelectedRequest] = useState(null);
    const [currentUserId, setCurrentUserId] = useState(null);

    useEffect(() => {
        const token = localStorage.getItem('jwt_token');
        if (token) {
            try {
                const decoded = jwtDecode(token);
                setCurrentUserId(decoded.user_id);
            } catch (error) {
                console.error("Failed to decode token for user ID:", error);
                toast.error("Failed to retrieve user information. Please log in again.");
            }
        }
    }, []);

    const fetchApprovalRequests = async () => {
        setIsLoading(true);
        setError('');
        try {
            const response = await apiRequest('/end-user/approval-requests/my-pending', 'GET');
            setApprovalRequests(response);
        } catch (err) {
            console.error("Failed to fetch approval requests:", err);
            setError(`Failed to load approval requests. ${err.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to load approval requests: ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        if (currentUserId) {
            fetchApprovalRequests();
        }
    }, [currentUserId]);

    const handleViewDetails = (request) => {
        setSelectedRequest(request);
        setShowDetailsModal(true);
    };

    const handleWithdraw = async (requestId) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        if (!window.confirm("Are you sure you want to WITHDRAW this request? This action cannot be undone.")) {
            return;
        }
        try {
            await apiRequest(`/end-user/approval-requests/${requestId}/withdraw`, 'POST');
            toast.success("Approval request withdrawn successfully!");
            fetchApprovalRequests();
            if (selectedRequest && selectedRequest.id === requestId) {
                setShowDetailsModal(false);
            }
        } catch (err) {
            console.error("Failed to withdraw request:", err);
            toast.error(`Failed to withdraw request: ${err.message || 'An unexpected error occurred.'}`);
        }
    };

    const formatActionType = (actionType) => {
        if (!actionType) return 'N/A';
        return actionType.replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
    };

    const canWithdraw = (request) => {
        return request.status === 'PENDING' && request.maker_user && request.maker_user.id === currentUserId;
    };

    return (
        <div className="card">
            <div className="flex justify-between items-center mb-6">
                <h2 className="text-2xl font-semibold text-gray-800">My Pending Approval Requests</h2>
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
                    <p className="text-green-700">No pending approval requests submitted by you at this time. All clear!</p>
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
                                        {request.lg_record?.lg_number || 'N/A'}
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatActionType(request.action_type)}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {request.maker_user?.email || 'N/A'}
                                    </td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{moment(request.created_at).format('DD-MMM-YYYY HH:mm')}</td>
                                    <td className="px-4 py-4 whitespace-nowrap text-sm">
                                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                                            request.status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                                            request.status === 'APPROVED' ? 'bg-green-100 text-green-800' :
                                            request.status === 'REJECTED' ? 'bg-red-100 text-red-800' :
                                            request.status === 'WITHDRAWN' ? 'bg-gray-100 text-gray-800' :
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
                                        {canWithdraw(request) ? (
                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                <button
                                                    onClick={() => handleWithdraw(request.id)}
                                                    className={`text-red-600 hover:text-red-900 p-1 rounded-md hover:bg-gray-100 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                    title="Withdraw Request"
                                                    disabled={isGracePeriod} // NEW: Disable the button
                                                >
                                                    <XCircle className="h-5 w-5" />
                                                </button>
                                            </GracePeriodTooltip>
                                        ) : (
                                            <span className="text-gray-400 text-xs italic">No actions</span>
                                        )}
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
                    onApprove={null}
                    onReject={null}
                    onWithdraw={canWithdraw(selectedRequest) ? handleWithdraw : null}
                    isEndUserView={true}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
        </div>
    );
}

export default EndUserPendingApprovalsPage;