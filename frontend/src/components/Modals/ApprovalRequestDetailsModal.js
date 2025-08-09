// frontend/src/components/Modals/ApprovalRequestDetailsModal.js
import React, { useState, useEffect } from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Check, FileText, AlertCircle, Loader2, XCircle } from 'lucide-react';
import moment from 'moment';
import { apiRequest } from '../../services/apiService';
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
                        <polygon className="fill-current" points="0,0 127.5,127.5 255,0" />
                    </svg>
                </div>
            </div>
        );
    }
    return children;
};

const buttonBaseClassNames = "inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

const formatActionType = (actionType) => {
    if (!actionType) return 'N/A';
    return String(actionType).replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
};

const ApprovalRequestDetailsModal = ({ request, onClose, onApprove, onReject, onWithdraw, isEndUserView, isGracePeriod }) => {
    const [lgRecordSnapshot, setLgRecordSnapshot] = useState(null);
    const [currentLgRecord, setCurrentLgRecord] = useState(null);
    const [isLoadingDetails, setIsLoadingDetails] = useState(true);
    const [detailsError, setDetailsError] = useState('');
    const [rejectionReason, setRejectionReason] = useState('');
    const [isProcessing, setIsProcessing] = useState(false);

    useEffect(() => {
        const fetchLgRecordDetails = async () => {
            setIsLoadingDetails(true);
            setDetailsError('');

            const lgRecordId = request?.entity_id;

            if (!lgRecordId || request.entity_type !== 'LGRecord') {
                setLgRecordSnapshot(request.lg_record || null);
                setCurrentLgRecord(null);
                if (!lgRecordId) {
                     setDetailsError("Entity ID for this request is missing. Cannot fetch current details for comparison.");
                } else if (request.entity_type !== 'LGRecord') {
                    setDetailsError(`Request is for entity type '${request.entity_type}', no direct LGRecord comparison available.`);
                }
                setIsLoadingDetails(false);
                return;
            }

            try {
                const currentLgResponse = await apiRequest(`/end-user/lg-records/${lgRecordId}`, 'GET');
                setCurrentLgRecord(currentLgResponse);
                setLgRecordSnapshot(request.lg_record || null);

            } catch (err) {
                console.error("Failed to fetch current LG record for comparison:", err);
                setDetailsError(`Failed to load current LG record details for comparison: ${err.message || 'An unexpected error occurred.'}`);
            } finally {
                setIsLoadingDetails(false);
            }
        };

        if (request) {
            fetchLgRecordDetails();
        }
    }, [request]);

    const formatAmount = (amount, currencyCode) => {
        if (amount === null || amount === undefined || currencyCode === null || currencyCode === undefined || isNaN(parseFloat(amount))) {
            return 'N/A';
        }
        try {
            const num = parseFloat(amount);
            const formattedNumber = num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return `${currencyCode || ''} ${formattedNumber}`;
        } catch (e) {
            console.error("Error formatting currency:", e);
            return `${currencyCode || ''} ${parseFloat(amount).toFixed(2)}`;
        }
    };

    const formatDate = (dateString) => {
        if (!dateString) return 'N/A';
        const date = moment(dateString);
        return date.isValid() ? date.format('DD-MMM-YYYY') : 'Invalid Date';
    };

    const getFieldDisplay = (label, snapshotValue, currentValue, type = 'text') => {
        let isChanged = false;
        let displaySnapshot = snapshotValue;
        let displayCurrent = currentValue;

        const isLgRecordComparison = request.entity_type === 'LGRecord';

        if (isLgRecordComparison && typeof snapshotValue === 'object' && snapshotValue !== null) {
            if (label === 'Beneficiary') displaySnapshot = snapshotValue.entity_name;
            else if (label === 'Currency') displaySnapshot = snapshotValue.iso_code;
            else if (label === 'Status') displaySnapshot = snapshotValue.name;
            else if (label === 'Operational Status') displaySnapshot = snapshotValue.name;
            else if (label === 'LG Type') displaySnapshot = snapshotValue.name;
            else if (label === 'Internal Owner') displaySnapshot = snapshotValue.email;
            else if (label === 'Issuing Bank') displaySnapshot = snapshotValue.name;
            else if (label === 'Issuing Method') displaySnapshot = snapshotValue.name;
            else if (label === 'Applicable Rule') displaySnapshot = snapshotValue.name;
            else if (label === 'LG Category') displaySnapshot = snapshotValue.category_name;
        }
        if (isLgRecordComparison && typeof currentValue === 'object' && currentValue !== null) {
            if (label === 'Beneficiary') displayCurrent = currentValue.entity_name;
            else if (label === 'Currency') displayCurrent = currentValue.iso_code;
            else if (label === 'Status') displayCurrent = currentValue.name;
            else if (label === 'Operational Status') displayCurrent = currentValue.name;
            else if (label === 'LG Type') displayCurrent = currentValue.name;
            else if (label === 'Internal Owner') displayCurrent = currentValue.email;
            else if (label === 'Issuing Bank') displayCurrent = currentValue.name;
            else if (label === 'Issuing Method') displayCurrent = currentValue.name;
            else if (label === 'Applicable Rule') displayCurrent = currentValue.name;
            else if (label === 'LG Category') displayCurrent = currentValue.category_name;
        }

        if (type === 'amount') {
            const snapCurrency = lgRecordSnapshot?.lg_currency?.iso_code || lgRecordSnapshot?.lg_currency?.symbol;
            const currCurrency = currentLgRecord?.lg_currency?.iso_code || currentLgRecord?.lg_currency?.symbol;

            displaySnapshot = formatAmount(snapshotValue, snapCurrency);
            displayCurrent = formatAmount(currentValue, currCurrency);
            isChanged = parseFloat(snapshotValue || 0) !== parseFloat(currentValue || 0);
        } else if (type === 'date') {
            displaySnapshot = formatDate(snapshotValue);
            displayCurrent = formatDate(currentValue);
            isChanged = (String(snapshotValue) || '') !== (String(currentValue) || '');
        } else if (type === 'boolean') {
            displaySnapshot = snapshotValue ? 'Yes' : 'No';
            displayCurrent = currentValue ? 'Yes' : 'No';
            isChanged = snapshotValue !== currentValue;
        } else if (type === 'json_object') {
            displaySnapshot = JSON.stringify(snapshotValue, null, 2);
            displayCurrent = JSON.stringify(currentValue, null, 2);
            isChanged = JSON.stringify(snapshotValue) !== JSON.stringify(currentValue);
        } else if (type === 'list') {
            displaySnapshot = Array.isArray(snapshotValue) ? snapshotValue.join(', ') : String(snapshotValue || '');
            displayCurrent = Array.isArray(currentValue) ? currentValue.join(', ') : String(currentValue || '');
            isChanged = JSON.stringify(snapshotValue) !== JSON.stringify(currentValue);
        }
         else {
            isChanged = (String(displaySnapshot || '') !== String(displayCurrent || ''));
        }

        let displayString;
        if (isChanged) {
            displayString = `${displaySnapshot || 'N/A'} ➔ ${displayCurrent || 'N/A'}`;
        } else {
            displayString = displayCurrent || 'N/A';
        }

        return (
            <p key={label}>
                <strong className={isChanged ? 'text-red-700' : 'text-gray-700'}>
                    {label}:
                </strong>{' '}
                <span className={isChanged ? 'font-bold' : ''}>
                    {displayString}
                </span>
                {isChanged && <span className="text-red-500 text-xs ml-2">(Changed)</span>}
            </p>
        );
    };

    const renderActionSpecificDetails = () => {
        if (!request || !request.request_details) return null;

        const details = request.request_details;

        switch (request.action_type) {
            case 'LG_DECREASE_AMOUNT':
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">Decrease Amount Request Details:</h4>
                        <p><strong>Amount to Decrease:</strong> {formatAmount(details.decrease_amount, request.lg_record?.lg_currency?.iso_code)}</p>
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                    </div>
                );
            case 'LG_LIQUIDATE':
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">LG Liquidation Request Details:</h4>
                        <p><strong>Liquidation Type:</strong> {details.liquidation_type || 'N/A'}</p>
                        {details.liquidation_type === 'partial' && (
                            <p><strong>New Amount (for partial):</strong> {formatAmount(details.new_amount, request.lg_record?.lg_currency?.iso_code)}</p>
                        )}
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                    </div>
                );
            case 'LG_RELEASE':
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">LG Release Request Details:</h4>
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                    </div>
                );
            case 'LG_AMEND':
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">LG Amendment Request Details:</h4>
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                        {details.amendment_details && (
                            <div>
                                <p className="font-medium mt-2">Requested Amendments:</p>
                                <ul className="list-disc list-inside ml-4">
                                    {Object.entries(details.amendment_details).map(([key, value]) => (
                                        <li key={key}>
                                            <strong>{formatActionType(key.replace(/_id$/, ''))}:</strong> {
                                                (key.includes('date') && value) ? formatDate(value) :
                                                (key.includes('amount') && value) ? formatAmount(value, request.lg_record?.lg_currency?.iso_code) :
                                                (typeof value === 'object' && value !== null) ? JSON.stringify(value) :
                                                String(value)
                                            }
                                        </li>
                                    ))}
                                </ul>
                            </div>
                        )}
                        {details.amendment_document_id && (
                            <p className="mt-2">
                                <FileText className="inline-block h-4 w-4 mr-1" />
                                <a href={`/api/v1/end-user/lg-records/documents/${details.amendment_document_id}/view`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                                    View Amendment Letter (ID: {details.amendment_document_id})
                                </a>
                            </p>
                        )}
                    </div>
                );
            case 'LG_ACTIVATE_NON_OPERATIVE':
                const paymentCurrencyCode = details.currency_id ? (currentLgRecord?.lg_currency?.iso_code || currentLgRecord?.lg_currency?.symbol) : 'N/A';
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">LG Activation Request Details:</h4>
                        <p><strong>Payment Method:</strong> {details.payment_method || 'N/A'}</p>
                        <p><strong>Amount:</strong> {formatAmount(details.amount, paymentCurrencyCode)}</p>
                        <p><strong>Payment Reference:</strong> {details.payment_reference || 'N/A'}</p>
                        <p><strong>Issuing Bank ID:</strong> {details.issuing_bank_id || 'N/A'}</p>
                        <p><strong>Payment Date:</strong> {formatDate(details.payment_date) || 'N/A'}</p>
                    </div>
                );
            case 'LG_CHANGE_OWNER_DETAILS':
                const oldOwnerSnapshot = request.lg_record_snapshot;
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">Update Internal Owner Details:</h4>
                        <p><strong>Old Email:</strong> {oldOwnerSnapshot?.email || 'N/A'}</p>
                        <p><strong>New Email:</strong> {details.email || 'N/A'}</p>
                        <p><strong>New Phone:</strong> {details.phone_number || 'N/A'}</p>
                        <p><strong>New Manager Email:</strong> {details.manager_email || 'N/A'}</p>
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                    </div>
                );
            case 'LG_CHANGE_SINGLE_LG_OWNER':
            case 'LG_CHANGE_BULK_LG_OWNER':
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">{formatActionType(request.action_type)} Details:</h4>
                        <p><strong>Change Scope:</strong> {details.change_scope ? formatActionType(details.change_scope) : 'N/A'}</p>
                        {details.old_internal_owner_contact_id && <p><strong>Old Owner ID:</strong> {details.old_internal_owner_contact_id}</p>}
                        {details.new_internal_owner_contact_id && <p><strong>New Owner ID:</strong> {details.new_internal_owner_contact_id}</p>}
                        {details.new_internal_owner_contact_details && (
                            <p><strong>New Owner Email (if new):</strong> {details.new_internal_owner_contact_details.email}</p>
                        )}
                        {details.lg_record_id && <p><strong>Affected LG Record ID:</strong> {details.lg_record_id}</p>}
                        {details.affected_lg_numbers && (
                            <p><strong>Affected LGs:</strong> {details.affected_lg_numbers.join(', ')}</p>
                        )}
                        <p><strong>Reason:</strong> {details.reason || 'No reason provided'}</p>
                    </div>
                );
            default:
                return (
                    <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                        <h4 className="font-semibold text-blue-800 mb-2">Generic Action Details:</h4>
                        {Object.keys(details || {}).length > 0 ? (
                            <pre className="text-sm overflow-x-auto p-2 bg-gray-100 rounded">
                                {JSON.stringify(details, null, 2)}
                            </pre>
                        ) : (
                            <p className="text-gray-500">No specific action details available for this type.</p>
                        )}
                    </div>
                );
        }
    };

    const areButtonsEnabled = request.status === 'PENDING';

    return (
        <Transition show={true} as={React.Fragment}>
            <Dialog as="div" className="relative z-10" onClose={onClose}>
                <TransitionChild
                    as={React.Fragment}
                    enter="ease-out duration-300"
                    enterFrom="opacity-0"
                    enterTo="opacity-100"
                    leave="ease-in duration-200"
                    leaveFrom="opacity-100"
                    leaveTo="opacity-0"
                >
                    <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" />
                </TransitionChild>

                <div className="fixed inset-0 z-10 w-screen overflow-y-auto">
                    <div className="flex min-h-full items-end justify-center p-4 text-center sm:items-center sm:p-0">
                        <TransitionChild
                            as={React.Fragment}
                            enter="ease-out duration-300"
                            enterFrom="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                            enterTo="opacity-100 translate-y-0 sm:scale-100"
                            leave="ease-in duration-200"
                            leaveFrom="opacity-100 translate-y-0 sm:scale-100"
                            leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                        >
                            <DialogPanel className="relative transform overflow-hidden rounded-lg bg-white px-4 pb-4 pt-5 text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-4xl sm:p-6">
                                <div className="absolute right-0 top-0 hidden pr-4 pt-4 sm:block">
                                    <button
                                        type="button"
                                        className="rounded-md bg-white text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2"
                                        onClick={onClose}
                                    >
                                        <span className="sr-only">Close</span>
                                        <X className="h-6 w-6" aria-hidden="true" />
                                    </button>
                                </div>
                                <div className="sm:flex sm:items-start">
                                    <div className="mt-3 text-center sm:ml-4 sm:mt-0 sm:text-left w-full">
                                        <DialogTitle as="h3" className="text-lg font-semibold leading-6 text-gray-900 border-b pb-3 mb-3">
                                            Review Approval Request: {request.lg_record?.lg_number || 'N/A'} - {formatActionType(request.action_type)}
                                            <span className={`ml-3 px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                                                request.status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                                                request.status === 'APPROVED' ? 'bg-green-100 text-green-800' :
                                                request.status === 'REJECTED' ? 'bg-red-100 text-red-800' :
                                                'bg-gray-100 text-gray-800'
                                            }`}>
                                                {request.status}
                                            </span>
                                        </DialogTitle>
                                        <div className="mt-2">
                                            <p className="text-sm text-red-700 mb-3">
                                                Review the details below. The data presented are based on the most recent information available, rather than the data at the time the request was submitted.
                                            </p>

                                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm mb-4">
                                                <div>
                                                    <h4 className="font-semibold text-gray-800 mb-2">Request Information:</h4>
                                                    <p><strong>Action Type:</strong> {formatActionType(request.action_type)}</p>
                                                    <p><strong>Requested By:</strong> {request.maker_user?.email || 'N/A'}</p>
                                                    <p><strong>Requested On:</strong> {moment(request.created_at).format('DD-MMM-YYYY HH:mm')}</p>
                                                    {request.status === 'REJECTED' && request.reason && (
                                                         <p className="text-red-600"><strong>Rejection Reason:</strong> {request.reason}</p>
                                                    )}
                                                    {request.status === 'APPROVED' && request.checker_user?.email && (
                                                        <p className="text-green-600"><strong>Approved By:</strong> {request.checker_user.email} on {moment(request.updated_at).format('DD-MMM-YYYY HH:mm')}</p>
                                                    )}
                                                    {request.status !== 'PENDING' && !request.checker_user?.email && (
                                                        <p className="text-gray-500 italic mt-2">This request was automatically {request.status.toLowerCase()}.</p>
                                                    )}
                                                </div>
                                                {renderActionSpecificDetails()}
                                            </div>


                                            <h4 className="text-md font-semibold text-gray-800 mb-3 border-b pb-2">LG Record Details (Snapshot vs. Current)</h4>

                                            {isLoadingDetails ? (
                                                <div className="flex justify-center items-center py-4">
                                                    <Loader2 className="animate-spin h-6 w-6 text-blue-600" />
                                                    <p className="ml-2 text-gray-600">Loading LG record details...</p>
                                                </div>
                                            ) : detailsError ? (
                                                <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md flex items-center">
                                                    <AlertCircle className="mr-2" size={20} />
                                                    {detailsError}
                                                </div>
                                            ) : (
                                                <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-3 text-gray-700">
                                                    {getFieldDisplay('LG Number', lgRecordSnapshot?.lg_number, currentLgRecord?.lg_number)}
                                                    {getFieldDisplay('Issuer Name', lgRecordSnapshot?.issuer_name, currentLgRecord?.issuer_name)}
                                                    {getFieldDisplay('Beneficiary', lgRecordSnapshot?.beneficiary_corporate, currentLgRecord?.beneficiary_corporate)}
                                                    {getFieldDisplay('Amount', lgRecordSnapshot?.lg_amount, currentLgRecord?.lg_amount, 'amount')}
                                                    {getFieldDisplay('Currency', lgRecordSnapshot?.lg_currency, currentLgRecord?.lg_currency)}
                                                    {getFieldDisplay('Issuance Date', lgRecordSnapshot?.issuance_date, currentLgRecord?.issuance_date, 'date')}
                                                    {getFieldDisplay('Expiry Date', lgRecordSnapshot?.expiry_date, currentLgRecord?.expiry_date, 'date')}
                                                    {getFieldDisplay('Auto Renewal', lgRecordSnapshot?.auto_renewal, currentLgRecord?.auto_renewal, 'boolean')}
                                                    {getFieldDisplay('LG Type', lgRecordSnapshot?.lg_type, currentLgRecord?.lg_type)}
                                                    {getFieldDisplay('Status', lgRecordSnapshot?.lg_status, currentLgRecord?.lg_status)}
                                                    {getFieldDisplay('Operational Status', lgRecordSnapshot?.lg_operational_status, currentLgRecord?.lg_operational_status)}
                                                    {getFieldDisplay('Payment Conditions', lgRecordSnapshot?.payment_conditions, currentLgRecord?.payment_conditions)}
                                                    {getFieldDisplay('Description Purpose', lgRecordSnapshot?.description_purpose, currentLgRecord?.description_purpose)}
                                                    {getFieldDisplay('Issuing Bank', lgRecordSnapshot?.issuing_bank, currentLgRecord?.issuing_bank)}
                                                    {getFieldDisplay('Applicable Rules Text', lgRecordSnapshot?.applicable_rules_text, currentLgRecord?.applicable_rules_text)}
                                                    {getFieldDisplay('Other Conditions', lgRecordSnapshot?.other_conditions, currentLgRecord?.other_conditions)}
                                                    {getFieldDisplay('Internal Owner', lgRecordSnapshot?.internal_owner_contact, currentLgRecord?.internal_owner_contact)}
                                                    {getFieldDisplay('LG Category', lgRecordSnapshot?.lg_category, currentLgRecord?.lg_category)}
                                                    {getFieldDisplay('Additional Fields', lgRecordSnapshot?.additional_field_values, currentLgRecord?.additional_field_values, 'json_object')}
                                                    {getFieldDisplay('Internal Contract/Project ID', lgRecordSnapshot?.internal_contract_project_id, currentLgRecord?.internal_contract_project_id)}
                                                    {getFieldDisplay('Notes', lgRecordSnapshot?.notes, currentLgRecord?.notes)}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>
                                <div className="mt-5 sm:mt-6 sm:grid sm:grid-flow-row-dense sm:grid-cols-2 sm:gap-3">
                                    {isEndUserView ? (
                                        <>
                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                <button
                                                    type="button"
                                                    className={`${buttonBaseClassNames} sm:col-start-2 bg-red-600 text-white hover:bg-red-700 ${!areButtonsEnabled || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                    onClick={() => {
                                                        if (areButtonsEnabled && onWithdraw) {
                                                            setIsProcessing(true);
                                                            onWithdraw(request.id).finally(() => setIsProcessing(false));
                                                        }
                                                    }}
                                                    disabled={!areButtonsEnabled || isProcessing || isGracePeriod}
                                                >
                                                    {isProcessing ? <Loader2 className="h-5 w-5 mr-2 animate-spin" /> : <XCircle className="h-5 w-5 mr-2" />}
                                                    {isProcessing ? 'Withdrawing...' : 'Withdraw Request'}
                                                </button>
                                            </GracePeriodTooltip>
                                            {!areButtonsEnabled && (
                                                <p className="sm:col-span-2 text-center text-gray-500 text-sm mt-3">
                                                    No actions available for this request (status is {request.status.toLowerCase()}).
                                                </p>
                                            )}
                                        </>
                                    ) : (
                                        <>
                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                <button
                                                    type="button"
                                                    className={`${buttonBaseClassNames} sm:col-start-2 bg-green-600 text-white hover:bg-green-700 ${!areButtonsEnabled || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                    onClick={() => {
                                                        if (areButtonsEnabled && onApprove) {
                                                            setIsProcessing(true);
                                                            onApprove(request.id).finally(() => setIsProcessing(false));
                                                        }
                                                    }}
                                                    disabled={!areButtonsEnabled || isProcessing || isGracePeriod}
                                                >
                                                    {isProcessing ? <Loader2 className="h-5 w-5 mr-2 animate-spin" /> : <Check className="h-5 w-5 mr-2" />}
                                                    {isProcessing ? 'Approving...' : 'Approve'}
                                                </button>
                                            </GracePeriodTooltip>
                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                <button
                                                    type="button"
                                                    className={`${buttonBaseClassNames} sm:col-start-1 text-red-600 border border-red-300 hover:bg-red-50 ${!areButtonsEnabled || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                    onClick={() => {
                                                        if (areButtonsEnabled && onReject) {
                                                            setIsProcessing(true);
                                                            onReject(request.id, rejectionReason).finally(() => setIsProcessing(false));
                                                        }
                                                    }}
                                                    disabled={!areButtonsEnabled || isProcessing || isGracePeriod}
                                                >
                                                    {isProcessing ? <Loader2 className="h-5 w-5 mr-2 animate-spin" /> : <X className="h-5 w-5 mr-2" />}
                                                    {isProcessing ? 'Rejecting...' : 'Reject'}
                                                </button>
                                            </GracePeriodTooltip>
                                            <input
                                                type="text"
                                                placeholder="Reason for rejection (optional)"
                                                value={rejectionReason}
                                                onChange={(e) => setRejectionReason(e.target.value)}
                                                className={`col-span-2 mt-3 block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-2 ${!areButtonsEnabled || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                disabled={!areButtonsEnabled || isProcessing || isGracePeriod}
                                            />
                                            {!areButtonsEnabled && (
                                                <p className="sm:col-span-2 text-center text-gray-500 text-sm mt-3">
                                                    No actions available for this request (status is {request.status.toLowerCase()}).
                                                </p>
                                            )}
                                        </>
                                    )}
                                </div>
                            </DialogPanel>
                        </TransitionChild>
                    </div>
                </div>
            </Dialog>
        </Transition>
    );
};

export default ApprovalRequestDetailsModal;