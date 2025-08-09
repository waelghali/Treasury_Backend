// frontend/src/pages/EndUser/EndUserActionCenter.js
import React, { useState, useEffect, useCallback } from 'react';
import { apiRequest, API_BASE_URL, getAuthToken } from '../../services/apiService';
import { Loader2, AlertCircle, Clock, FileText, Repeat, CalendarPlus, Truck, Building, Mail, Printer, Eye } from 'lucide-react';
import { toast } from 'react-toastify';
import moment from 'moment';

import ExtendLGModal from '../../components/Modals/ExtendLGModal';
import RecordDeliveryModal from '../../components/Modals/RecordDeliveryModal';
import RecordBankReplyModal from '../../components/Modals/RecordBankReplyModal';
import BulkRemindersModal from '../../components/Modals/BulkRemindersModal';
import RunAutoRenewalModal from '../../components/Modals/RunAutoRenewalModal';

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

function EndUserActionCenter({ isGracePeriod }) { // NEW: Accept isGracePeriod prop
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');
    
    const [lgRenewalList, setLgRenewalList] = useState([]);
    const [instructionsUndelivered, setInstructionsUndelivered] = useState([]);
    const [instructionsNoReply, setInstructionsNoReply] = useState([]);
    const [approvedPendingPrints, setApprovedPendingPrints] = useState([]);

    const [showExtendModal, setShowExtendModal] = useState(false);
    const [selectedLgRecordForExtend, setSelectedLgRecordForExtend] = useState(null);

    const [showRecordDeliveryModal, setShowRecordDeliveryModal] = useState(false);
    const [selectedInstructionForDelivery, setSelectedInstructionForDelivery] = useState(null);

    const [showRecordBankReplyModal, setShowRecordBankReplyModal] = useState(false);
    const [selectedInstructionForReply, setSelectedInstructionForReply] = useState(null);

    const [showBulkRemindersModal, setShowBulkRemindersModal] = useState(false);
    const [showRunAutoRenewalModal, setShowRunAutoRenewalModal] = useState(false);

    const formatActionTypeLabel = useCallback((actionType) => {
        if (!actionType) return '';
        return actionType.replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
    }, []);

    const getLatestInstruction = useCallback((record) => {
        if (!record || !record.instructions || record.instructions.length === 0) {
            return null;
        }
        const sortedInstructions = [...record.instructions].sort((a, b) => {
            const dateA = new Date(a.created_at);
            const dateB = new Date(b.created_at);
            if (dateA.getTime() > dateB.getTime()) return 1;
            if (dateA.getTime() < dateB.getTime()) return -1;
            return a.id - b.id;
        });
        return sortedInstructions[sortedInstructions.length - 1];
    }, []);

    const handleViewLetter = useCallback(async (instructionId, lgNumber = 'N/A') => {
        if (!instructionId) {
            toast.error('Instruction ID is missing. Cannot open letter.');
            return;
        }
        try {
            await apiRequest(
                `/end-user/lg-records/instructions/${instructionId}/mark-as-accessed-for-print`,
                'POST'
            );
            toast.success(`LG Instruction ${lgNumber} marked for print. Opening letter...`);

            const authToken = getAuthToken();
            let letterUrl = `${API_BASE_URL}/end-user/lg-records/instructions/${instructionId}/view-letter`;
            if (authToken) {
                letterUrl += `?token=${authToken}&print=true`;
            }
            window.open(letterUrl, '_blank');

        } catch (error) {
            console.error("Failed to mark instruction as accessed for print or open letter:", error);
            toast.error(`Could not open letter for LG ${lgNumber}. ${error.message || 'Please try again later.'}`);
        }
    }, []);

    const fetchAllActionCenterData = useCallback(async () => {
        setIsLoading(true);
        setError('');
        try {
            const renewalResponse = await apiRequest('/end-user/action-center/lg-for-renewal', 'GET');
            setLgRenewalList(renewalResponse);

            const undeliveredResponse = await apiRequest('/end-user/action-center/instructions-undelivered', 'GET');
            setInstructionsUndelivered(undeliveredResponse);

            const noReplyResponse = await apiRequest('/end-user/action-center/instructions-awaiting-reply', 'GET');
            setInstructionsNoReply(noReplyResponse);
            
            const pendingPrintsResponse = await apiRequest('/end-user/action-center/approved-requests-pending-print', 'GET');
            setApprovedPendingPrints(pendingPrintsResponse);

        } catch (err) {
            console.error("Failed to fetch action center data:", err);
            setError(`Failed to load tasks: ${err.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to load tasks: ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsLoading(false);
        }
    }, []);

    const handleActionSuccess = useCallback((updatedRecordFromBackend = null, latestInstructionId = null) => {
        setShowExtendModal(false);
        setSelectedLgRecordForExtend(null);
        setShowRecordDeliveryModal(false);
        setSelectedInstructionForDelivery(null);
        setShowRecordBankReplyModal(false);
        setSelectedInstructionForReply(null);
        setShowBulkRemindersModal(false);
        setShowRunAutoRenewalModal(false);

        fetchAllActionCenterData(); 

        if (latestInstructionId) {
            const lgNumber = selectedLgRecordForExtend?.lg_number || 'N/A';
            setTimeout(() => {
                handleViewLetter(latestInstructionId, lgNumber);
            }, 100);
        }
    }, [fetchAllActionCenterData, selectedLgRecordForExtend, handleViewLetter]);

    useEffect(() => {
        fetchAllActionCenterData();
    }, [fetchAllActionCenterData]);

    const formatAmount = (amount, currencyCode) => {
        if (amount === null || currencyCode === null || currencyCode === undefined || isNaN(parseFloat(amount))) {
            return 'N/A';
        }
        try {
            const num = parseFloat(amount);
            const formattedNumber = num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return `${currencyCode} ${formattedNumber}`;
        } catch (e) {
            return `${currencyCode} ${parseFloat(amount).toFixed(2)}`;
        }
    };

    const formatDate = (dateString) => {
        if (!dateString) return 'N/A';
        return moment(dateString).format('DD-MMM-YYYY');
    };

    const handlePrintApprovedLetter = async (instructionId, lgNumber) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        try {
            await apiRequest(
                `/end-user/lg-records/instructions/${instructionId}/mark-as-accessed-for-print`,
                'POST'
            );
            toast.success(`LG Instruction ${lgNumber} marked for print. Opening letter...`);

            const authToken = getAuthToken();
            let letterUrl = `${API_BASE_URL}/end-user/lg-records/instructions/${instructionId}/view-letter`;
            if (authToken) {
                letterUrl += `?token=${authToken}&print=true`;
            }
            window.open(letterUrl, '_blank');

            fetchAllActionCenterData();

        } catch (error) {
            console.error("Failed to mark instruction as accessed for print or open letter:", error);
            toast.error(`Could not open letter for LG ${lgNumber}. ${error.message || 'Please try again later.'}`);
        }
    };

    const handleSendReminder = async (instructionId, serialNumber) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        try {
            const authToken = getAuthToken();
            if (!authToken) {
                toast.error("Authentication required to send reminder.");
                return;
            }

            let url = `${API_BASE_URL}/end-user/lg-records/instructions/${instructionId}/send-reminder-to-bank`;
            url += `?token=${authToken}&print=true`;

            const newWindow = window.open(url, '_blank', 'noopener,noreferrer');
            if (newWindow) {
                toast.info(`Generating and opening reminder PDF for instruction ${serialNumber} in a new tab. Please ensure pop-ups are allowed.`);
                
                setTimeout(() => {
                    fetchAllActionCenterData();
                }, 1000); 
            } else {
                toast.error("Failed to open new tab. Please ensure your browser allows pop-ups.");
            }

        } catch (error) {
            console.error("Failed to send reminder:", error);
            toast.error(`Failed to send reminder: ${error.message || 'An unexpected error occurred.'}`);
        }
    };

    const handleViewInstructionDocument = (filePath) => {
        if (filePath) {
            window.open(filePath, '_blank');
        } else {
            toast.error("Document path not available.");
        }
    };


    return (
        <div className="container mx-auto p-6 bg-white rounded-lg shadow-xl my-8">
            <h1 className="text-3xl font-bold text-gray-800 mb-6 border-b pb-4">Action Center</h1>

            {error && (
                <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                    <AlertCircle className="h-5 w-5 mr-2" />
                    <span className="block sm:inline">{error}</span>
                </div>
            )}

            {isLoading ? (
                <div className="text-center py-8">
                    <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
                    <p className="text-gray-600 mt-2">Loading your actionable tasks...</p>
                </div>
            ) : (
                <div className="space-y-8">
                    {/* NEW Section 4: Approved Requests Pending Print */}
                    <div className="card">
                        <div className="flex justify-between items-center mb-4">
                            <h2 className="text-xl font-semibold text-gray-800 flex items-center">
                                <Printer className="h-6 w-6 mr-2 text-purple-600" />
                                Approved Requests Pending Print ({approvedPendingPrints.length})
                            </h2>
                            {approvedPendingPrints.length > 0 && (
                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                    <button 
                                        onClick={() => toast.info("Bulk Print functionality coming soon!")}
                                        className={`inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm bg-purple-100 text-purple-700 hover:bg-purple-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                        disabled={isGracePeriod}
                                    >
                                        <Printer className="h-5 w-5 mr-2" />
                                        Bulk Print
                                    </button>
                                </GracePeriodTooltip>
                            )}
                        </div>
                        {approvedPendingPrints.length > 0 ? (
                            <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LG Number</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action Type</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Maker</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Approval Date</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Serial</th>
                                            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {approvedPendingPrints.map(request => (
                                            <tr key={request.id}>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{request.lg_record?.lg_number || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatActionTypeLabel(request.action_type)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{request.maker_user?.email || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatDate(request.updated_at)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{request.related_instruction?.serial_number || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                        <button 
                                                            onClick={() => request.related_instruction?.id && request.lg_record?.lg_number ? handlePrintApprovedLetter(request.related_instruction.id, request.lg_record.lg_number) : toast.error("Instruction details missing. Cannot print.")}
                                                            className={`inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-purple-100 text-purple-700 hover:bg-purple-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                            disabled={isGracePeriod}
                                                        >
                                                            <Printer className="h-4 w-4 mr-1" /> Print Letter
                                                        </button>
                                                    </GracePeriodTooltip>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <p className="text-gray-500 py-4">No approved requests currently awaiting print.</p>
                        )}
                    </div>

                    {/* Section 1: LGs for Renewal/Expiry */}
                    <div className="card">
                        <div className="flex justify-between items-center mb-4">
                            <h2 className="text-xl font-semibold text-gray-800 flex items-center">
                                <Clock className="h-6 w-6 mr-2 text-orange-500" />
                                LGs Approaching Expiry / Renewal ({lgRenewalList.length})
                            </h2>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button 
                                    onClick={() => setShowRunAutoRenewalModal(true)}
                                    className={`inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm bg-orange-600 text-white hover:bg-orange-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-orange-500 transition-all duration-200 ease-in-out ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    disabled={isGracePeriod}
                                >
                                    <Repeat className="h-5 w-5 mr-2" />
                                    Run Auto/Forced Renewal
                                </button>
                            </GracePeriodTooltip>
                        </div>
                        {lgRenewalList.length > 0 ? (
                            <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LG Number</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Beneficiary</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Expiry Date</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Days Remaining</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Amount</th>
                                            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {lgRenewalList.map(lg => (
                                            <tr key={lg.id}>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{lg.lg_number}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{lg.beneficiary_corporate?.entity_name || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatDate(lg.expiry_date)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                                                    {moment(lg.expiry_date).diff(moment(), 'days')}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatAmount(lg.lg_amount, lg.lg_currency?.iso_code)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                        <button 
                                                            onClick={() => {
                                                                if (!isGracePeriod) {
                                                                    setSelectedLgRecordForExtend(lg); 
                                                                    setShowExtendModal(true); 
                                                                } else {
                                                                    toast.warn("This action is disabled during your subscription's grace period.");
                                                                }
                                                            }}
                                                            className={`inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-indigo-100 text-indigo-700 hover:bg-indigo-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                            disabled={isGracePeriod}
                                                        >
                                                            <CalendarPlus className="h-4 w-4 mr-1" /> Renew
                                                        </button>
                                                    </GracePeriodTooltip>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <p className="text-gray-500 py-4">No LGs currently approaching expiry or due for renewal.</p>
                        )}
                    </div>

                    {/* Section 2: Instructions Awaiting Delivery Confirmation */}
                    <div className="card">
                        <div className="flex justify-between items-center mb-4">
                            <h2 className="text-xl font-semibold text-gray-800 flex items-center">
                                <Truck className="h-6 w-6 mr-2 text-blue-500" />
                                Instructions Awaiting Delivery Confirmation ({instructionsUndelivered.length})
                            </h2>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button 
                                    onClick={() => toast.info("Generate Undelivered Report (PDF) functionality coming soon!")}
                                    className={`inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm bg-blue-100 text-blue-700 hover:bg-blue-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    disabled={isGracePeriod}
                                >
                                    <FileText className="h-5 w-5 mr-2" />
                                    Generate Undelivered Report
                                </button>
                            </GracePeriodTooltip>
                        </div>
                        {instructionsUndelivered.length > 0 ? (
                            <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LG Number</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Type</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Serial</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Issued Date</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Days Undelivered</th>
                                            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {instructionsUndelivered.map(inst => (
                                            <tr key={inst.id}>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{inst.lg_record?.lg_number || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatActionTypeLabel(inst.instruction_type)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{inst.serial_number}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatDate(inst.instruction_date)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                                                    {moment().diff(moment(inst.instruction_date), 'days')}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                        <button 
                                                            onClick={() => {
                                                                if (!isGracePeriod) {
                                                                    setSelectedInstructionForDelivery(inst); 
                                                                    setShowRecordDeliveryModal(true); 
                                                                } else {
                                                                    toast.warn("This action is disabled during your subscription's grace period.");
                                                                }
                                                            }}
                                                            className={`inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-blue-100 text-blue-700 hover:bg-blue-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                            disabled={isGracePeriod}
                                                        >
                                                            <Truck className="h-4 w-4 mr-1" /> Record Delivery
                                                        </button>
                                                    </GracePeriodTooltip>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <p className="text-gray-500 py-4">No instructions currently awaiting delivery confirmation.</p>
                        )}
                    </div>

                    {/* Section 3: Instructions Awaiting Bank Reply */}
                    <div className="card">
                        <div className="flex justify-between items-center mb-4">
                            <h2 className="text-xl font-semibold text-gray-800 flex items-center">
                                <Building className="h-6 w-6 mr-2 text-teal-500" />
                                Instructions Awaiting Bank Reply ({instructionsNoReply.length})
                            </h2>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button 
                                    onClick={() => setShowBulkRemindersModal(true)}
                                    className={`inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm bg-teal-100 text-teal-700 hover:bg-teal-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    disabled={isGracePeriod}
                                >
                                    <Mail className="h-5 w-5 mr-2" />
                                    Generate Bulk Reminders
                                </button>
                            </GracePeriodTooltip>
                        </div>
                        {instructionsNoReply.length > 0 ? (
                            <div className="overflow-x-auto">
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">LG Number</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Type</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Instruction Serial</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Issued Date</th>
                                            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Delivered Date</th>
                                            <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {instructionsNoReply.map(inst => (
                                            <tr key={inst.id}>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{inst.lg_record?.lg_number || 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatActionTypeLabel(inst.instruction_type)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{inst.serial_number}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{formatDate(inst.instruction_date)}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">{inst.delivery_date ? formatDate(inst.delivery_date) : 'N/A'}</td>
                                                <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                                                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                        <button 
                                                            onClick={() => {
                                                                if (!isGracePeriod) {
                                                                    setSelectedInstructionForReply(inst); 
                                                                    setShowRecordBankReplyModal(true); 
                                                                } else {
                                                                    toast.warn("This action is disabled during your subscription's grace period.");
                                                                }
                                                            }}
                                                            className={`inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-green-100 text-green-700 hover:bg-green-200 mr-2 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                            disabled={isGracePeriod}
                                                        >
                                                            <Building className="h-4 w-4 mr-1" /> Record Reply
                                                        </button>
                                                    </GracePeriodTooltip>
                                                    {inst.has_reminder_sent ? (
                                                        <button 
                                                            onClick={() => handleViewLetter(inst.id, inst.lg_record?.lg_number || 'N/A')}
                                                            className="inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-yellow-100 text-yellow-800 hover:bg-yellow-200"
                                                        >
                                                            <Eye className="h-4 w-4 mr-1" /> View Issued Reminder
                                                        </button>
                                                    ) : (
                                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                            <button 
                                                                onClick={() => handleSendReminder(inst.id, inst.serial_number)}
                                                                className={`inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-orange-100 text-orange-700 hover:bg-orange-200 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                                disabled={isGracePeriod}
                                                            >
                                                                <Mail className="h-4 w-4 mr-1" /> Send Reminder
                                                            </button>
                                                        </GracePeriodTooltip>
                                                    )}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        ) : (
                            <p className="text-gray-500 py-4">No instructions currently awaiting bank replies.</p>
                        )}
                    </div>
                </div>
            )}

            {/* Modals */}
            {showExtendModal && selectedLgRecordForExtend && (
                <ExtendLGModal
                    lgRecord={selectedLgRecordForExtend}
                    onClose={() => setShowExtendModal(false)}
                    onSuccess={handleActionSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
            {showRecordDeliveryModal && selectedInstructionForDelivery && (
                <RecordDeliveryModal
                    instruction={selectedInstructionForDelivery}
                    onClose={() => setShowRecordDeliveryModal(false)}
                    onSuccess={handleActionSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
            {showRecordBankReplyModal && selectedInstructionForReply && (
                <RecordBankReplyModal
                    instruction={selectedInstructionForReply}
                    onClose={() => setShowRecordBankReplyModal(false)}
                    onSuccess={handleActionSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
            {showBulkRemindersModal && (
                <BulkRemindersModal
                    onClose={() => setShowBulkRemindersModal(false)}
                    onSuccess={handleActionSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
            {showRunAutoRenewalModal && (
                <RunAutoRenewalModal
                    onClose={() => setShowRunAutoRenewalModal(false)}
                    onSuccess={handleActionSuccess}
                    isGracePeriod={isGracePeriod} // NEW: Pass prop
                />
            )}
        </div>
    );
}

export default EndUserActionCenter;