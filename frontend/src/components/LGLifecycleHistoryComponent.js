// frontend/src/components/LGLifecycleHistoryComponent.js
import React, { useState, useEffect, useCallback } from 'react';
import { apiRequest, API_BASE_URL, getAuthToken } from '../services/apiService';
import { format, differenceInDays } from 'date-fns';
import { Loader, AlertCircle, Clock, Info, CheckCircle, Edit, Repeat, Truck, Building, User, RotateCcw, FileText, ChevronDown, Download, Mail, Eye, FileMinus, MinusCircle, Users } from 'lucide-react';
import CancelInstructionModal from 'components/Modals/CancelInstructionModal';
import { toast } from 'react-toastify';

// A reusable component to provide a tooltip for disabled elements during the grace period.
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

const formatActionTypeLabel = (actionType) => {
    if (!actionType) return '';
    return actionType.replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
};

const LGLifecycleHistoryComponent = ({
    lgRecordId,
    lgInstructions = [],
    lgDocuments = [],
    onRecordDelivery,
    onRecordBankReply,
    onSendReminder,
    onViewInstructionLetter,
    onViewInstructionDocument,
    isCorporateAdminView = false,
    isGracePeriod
}) => {
    const [events, setEvents] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedActionType, setSelectedActionType] = useState('ALL');
    const [dynamicFilterOptions, setDynamicFilterOptions] = useState([]);
    const [reminderThresholds, setReminderThresholds] = useState({
        daysSinceDelivery: 7,
        daysSinceIssuance: 3,
        maxDaysSinceIssuance: 90,
    });
    const [showCancelModal, setShowCancelModal] = useState(false);
    const [selectedInstructionToCancel, setSelectedInstructionToCancel] = useState(null);

    const instructionsMap = React.useMemo(() => {
        return new Map(lgInstructions.map(inst => [inst.id, inst]));
    }, [lgInstructions]);

    const findLatestCancellableInstruction = useCallback(() => {
        const cancellableTypes = [
            'LG_EXTENSION',
            'LG_LIQUIDATION',
            'LG_RELEASE',
            'LG_DECREASE_AMOUNT',
            'LG_ACTIVATE_NON_OPERATIVE',
        ];
        
        const sortedInstructions = [...lgInstructions].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        
        const latestCancellable = sortedInstructions.find(inst => cancellableTypes.includes(inst.instruction_type));
        
        return latestCancellable || null;
    }, [lgInstructions]);

    const latestCancellableInstruction = findLatestCancellableInstruction();

	const findDocumentForInstruction = useCallback((instruction, documentType) => {
		if (!instruction || !instruction.documents) return null;
		return instruction.documents.find(doc =>
			doc.document_type === documentType &&
			doc.lg_instruction_id === instruction.id &&
			!doc.is_deleted
		);
	}, []);

    const fetchLifecycleHistoryAndConfig = useCallback(async () => {
        setIsLoading(true);
        setError(null);
        try {
            const allEventsResponse = await apiRequest(`/end-user/lg-records/${lgRecordId}/lifecycle-history`, 'GET');
            const filteredEvents = allEventsResponse.filter(event => event.action_type !== 'DOCUMENT_UPLOADED');
            const sortedEvents = filteredEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            const uniqueActionTypes = new Set(sortedEvents.map(eventItem => eventItem.action_type));
            const generatedOptions = [{ label: 'All Events', value: 'ALL' }];
            uniqueActionTypes.forEach(type => {
                generatedOptions.push({ label: formatActionTypeLabel(type), value: type });
            });
            generatedOptions.sort((a, b) => {
                if (a.value === 'ALL') return -1;
                if (b.value === 'ALL') return 1;
                return a.label.localeCompare(b.label);
            });
            setDynamicFilterOptions(generatedOptions);
            setEvents(selectedActionType === 'ALL' ? sortedEvents : sortedEvents.filter(eventItem => eventItem.action_type === selectedActionType));
            if (!isCorporateAdminView) {
                const configKeys = [
                    "REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY",
                    "REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE",
                    "REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE"
                ];
                const fetchedThresholdsPromises = configKeys.map(key =>
                    apiRequest(`/end-user/customer-configurations/${key}`, 'GET')
                        .catch(e => {
                            console.warn(`Failed to fetch config for ${key}:`, e);
                            return null;
                        })
                );
                const configResponses = await Promise.all(fetchedThresholdsPromises);
                const fetchedThresholds = {};
                configResponses.forEach((res, index) => {
                    if (res && res.effective_value !== undefined) {
                        fetchedThresholds[configKeys[index]] = parseInt(res.effective_value, 10);
                    }
                });
                setReminderThresholds(prev => ({
                    ...prev,
                    daysSinceDelivery: fetchedThresholds["REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY"] ?? prev.daysSinceDelivery,
                    daysSinceIssuance: fetchedThresholds["REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE"] ?? prev.daysSinceIssuance,
                    maxDaysSinceIssuance: fetchedThresholds["REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE"] ?? prev.maxDaysSinceIssuance,
                }));
            }
        } catch (err) {
            console.error("Failed to fetch LG lifecycle history or configurations:", err);
            setError("Failed to load lifecycle history. Please try again.");
        } finally {
            setIsLoading(false);
        }
    }, [lgRecordId, selectedActionType, isCorporateAdminView]);

    useEffect(() => {
        if (lgRecordId) {
            fetchLifecycleHistoryAndConfig();
        }
    }, [lgRecordId, fetchLifecycleHistoryAndConfig]);

    const getInstructionForEvent = useCallback((eventItem) => {
        if (eventItem.entity_type === "LGInstruction" && eventItem.entity_id) {
            return instructionsMap.get(eventItem.entity_id);
        }
        if (eventItem.details) {
            const instructionIdFromDetails = eventItem.details.generated_instruction_id || eventItem.details.instruction_id || eventItem.details.new_instruction_id;
            if (instructionIdFromDetails) {
                return instructionsMap.get(instructionIdFromDetails);
            }
        }
        return null;
    }, [instructionsMap]);

    const hasReminderBeenSent = useCallback((originalInstructionId) => {
        if (originalInstructionId === null || originalInstructionId === undefined) return false;
        const sentInAuditLog = events.some(eventItem =>
            eventItem.action_type === 'LG_REMINDER_SENT_TO_BANK' &&
            eventItem.details &&
            String(eventItem.details.original_instruction_id) === String(originalInstructionId)
        );
        const instructionObject = instructionsMap.get(originalInstructionId);
        const sentInObjectFlag = instructionObject ? instructionObject.has_reminder_sent : false;
        return sentInAuditLog || sentInObjectFlag;
    }, [events, instructionsMap]);

    const getEventIcon = (actionType) => {
        switch (actionType) {
            case 'LG_CREATED': return <CheckCircle className="text-green-500" size={20} />;
            case 'LG_EXTENDED': return <Repeat className="text-blue-500" size={20} />;
            case 'LG_AMENDED': return <Edit className="text-purple-500" size={20} />;
            case 'LG_LIQUIDATED_FULL':
            case 'LG_LIQUIDATED_PARTIAL':
            case 'LG_LIQUIDATE':
                return <FileMinus className="text-red-500" size={20} />;
            case 'LG_RELEASED':
            case 'LG_RELEASE':
                return <CheckCircle className="text-green-500" size={20} />;
            case 'LG_DECREASED_AMOUNT':
            case 'LG_DECREASE_AMOUNT':
                return <MinusCircle className="text-orange-500" size={20} />;
            case 'LG_INSTRUCTION_DELIVERED':
            case 'LG_RECORD_DELIVERY':
                return <Truck className="text-indigo-500" size={20} />;
            case 'LG_BANK_REPLY_RECORDED':
            case 'LG_RECORD_BANK_REPLY':
                return <Building className="text-teal-500" size={20} />;
            case 'LG_REMINDER_SENT_TO_BANK':
            case 'LG_REMINDER_TO_BANKS':
                return <Mail className="text-yellow-500" size={20} />;
            case 'LG_BULK_REMINDER_INITIATED': return <Mail className="text-yellow-500" size={20} />;
            case 'INTERNAL_OWNER_CHANGE':
            case 'LG_OWNER_DETAILS_UPDATED':
            case 'LG_SINGLE_OWNER_CHANGED':
            case 'LG_BULK_OWNER_CHANGED':
                return <Users className="text-gray-600" size={20} />;
            case 'INSTRUCTION_ACCESSED_FOR_PRINT': return <FileText className="text-pink-500" size={20} />;
            case 'LG_ACTIVATED':
            case 'LG_ACTIVATE_NON_OPERATIVE': return <CheckCircle className="text-green-500" size={20} />;
            case 'LG_TOGGLE_AUTO_RENEWAL': return <Repeat className="text-blue-400" size={20} />;
            case 'APPROVAL_REQUEST_SUBMITTED': return <Clock className="text-gray-400" size={20} />;
            case 'APPROVAL_REQUEST_APPROVED': return <CheckCircle className="text-green-500" size={20} />;
            case 'APPROVAL_REQUEST_REJECTED': return <AlertCircle className="text-red-500" size={20} />;
            case 'APPROVAL_REQUEST_WITHDRAWN': return <RotateCcw className="text-gray-500" size={20} />;
            case 'APPROVAL_REQUEST_AUTO_REJECTED': return <AlertCircle className="text-gray-500" size={20} />;
            case 'APPROVAL_INVALIDATED_BY_OTHER_APPROVAL': return <AlertCircle className="text-orange-500" size={20} />;
            case 'CREATE': return <CheckCircle className="text-green-500" size={20} />;
            case 'UPDATE': return <Edit className="text-blue-500" size={20} />;
            case 'SOFT_DELETE': return <AlertCircle className="text-red-500" size={20} />;
            case 'RESTORE': return <RotateCcw className="text-green-500" size={20} />;
            case 'LOGIN_SUCCESS': return <User className="text-green-500" size={20} />;
            case 'LOGIN_FAILED': return <AlertCircle className="text-red-500" size={20} />;
            case 'AI_SCAN_SUCCESS': return <Info className="text-purple-500" size={20} />;
            case 'AI_SCAN_FAILED': return <AlertCircle className="text-red-500" size={20} />;
            case 'NOTIFICATION_SENT': return <Mail className="text-teal-500" size={20} />;
            case 'NOTIFICATION_FAILED': return <AlertCircle className="text-red-500" size={20} />;
            case 'LG_RENEWAL_REMINDER_FIRST_SENT': return <Mail className="text-yellow-400" size={20} />;
            case 'LG_RENEWAL_REMINDER_SECOND_SENT': return <Mail className="text-red-500" size={20} />;
            case 'LG_OWNER_RENEWAL_REMINDER_SENT': return <Mail className="text-orange-400" size={20} />;
            case 'LG_OWNER_RENEWAL_REMINDER_SKIPPED_RECENTLY_SENT': return <Info className="text-gray-400" size={20} />;
            default: return <Info className="text-gray-400" size={20} />;
        }
    };

    const formatEventDetails = (actionType, details, userEmail) => {
        let detailString = '';
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
            try {
                return format(new Date(dateString), 'MMM dd, yyyy');
            } catch {
                return dateString;
            }
        };


        switch (actionType) {
            case 'LG_CREATED':
                detailString = `Created LG #${details.lg_number || 'N/A'} for ${details.beneficiary || 'N/A'}. Amount: ${formatAmount(details.amount, details.currency)}.`;
                break;
            case 'LG_EXTENDED':
                detailString = `Extended expiry from ${formatDate(details.old_expiry_date)} to ${formatDate(details.new_expiry_date)}. Instruction Serial: ${details.instruction_serial || 'N/A'}.`;
                break;
            case 'LG_AMENDED':
                detailString = `LG amended. Reason: ${details.reason || 'N/A'}. Instruction Serial: ${details.instruction_serial_number || details.instruction_serial || 'N/A'}.`;
                if (details.amended_fields) {
                    const fields = Object.keys(details.amended_fields).map(key => formatActionTypeLabel(key)).join(', ');
                    detailString += ` Fields changed: ${fields}.`;
                }
                break;
            case 'LG_RELEASED':
            case 'LG_RELEASE':
                detailString = `LG released. Instruction Serial: ${details.instruction_serial || details.serial_number || 'N/A'}. Approved by ${details.approved_by_user_email || 'N/A'}.`;
                break;
            case 'LG_LIQUIDATED_FULL':
            case 'LG_LIQUIDATED_PARTIAL':
            case 'LG_LIQUIDATE':
                detailString = `LG liquidated (${details.liquidation_type || 'N/A'}). Old Amount: ${formatAmount(details.old_amount, details.lg_currency)} New Amount: ${formatAmount(details.new_amount, details.lg_currency)}. Instruction Serial: ${details.instruction_serial || details.serial_number || 'N/A'}.`;
                break;
            case 'LG_DECREASED_AMOUNT':
            case 'LG_DECREASE_AMOUNT':
                detailString = `LG amount decreased by ${formatAmount(details.decrease_amount, details.lg_currency)} from ${formatAmount(details.old_amount, details.lg_currency)} to ${formatAmount(details.new_amount, details.lg_currency)}. Instruction Serial: ${details.instruction_serial || details.serial_number || 'N/A'}.`;
                break;
            case 'LG_INSTRUCTION_DELIVERED':
            case 'LG_RECORD_DELIVERY':
                detailString = `Delivery of instruction ${details.serial_number || 'N/A'} confirmed. Delivered on: ${formatDate(details.delivery_date)}. Document stored: ${details.document_stored ? 'Yes' : 'No'}.`;
                break;
            case 'LG_BANK_REPLY_RECORDED':
            case 'LG_RECORD_BANK_REPLY':
                detailString = `Bank reply recorded for instruction ${details.serial_number || 'N/A'}. Replied on: ${formatDate(details.bank_reply_date)}. Details: ${details.reply_details || 'N/A'}. Document stored: ${details.document_stored ? 'Yes' : 'No'}.`;
                break;
            case 'LG_REMINDER_SENT_TO_BANK':
            case 'LG_REMINDER_TO_BANKS':
                detailString = `Reminder sent for original instruction ${details.original_instruction_serial || 'N/A'}. Days overdue: ${details.days_overdue || 'N/A'}. Reminder Serial: ${details.reminder_serial || 'N/A'}.`;
                break;
            case 'LG_BULK_REMINDER_INITIATED':
                detailString = `Bulk reminder initiated. ${details.reminders_actually_generated || 0} reminders generated.`;
                break;
            case 'APPROVAL_REQUEST_SUBMITTED':
                detailString = `Approval request for ${formatActionTypeLabel(details.action_type_requested || details.action_type)} on ${details.entity_type_requested || details.entity_type} ID ${details.entity_id_requested || details.entity_id || 'N/A'} submitted by ${userEmail || 'N/A'}.`;
                break;
            case 'APPROVAL_REQUEST_APPROVED':
                detailString = `Approval request for ${formatActionTypeLabel(details.action_type_approved || details.action_type)} on ${details.entity_type_approved || details.entity_type} ID ${details.entity_id_approved || details.entity_id || 'N/A'} approved by ${userEmail || 'N/A'}.`;
                break;
            case 'APPROVAL_REQUEST_REJECTED':
                detailString = `Approval request for ${formatActionTypeLabel(details.action_type_rejected || details.action_type)} on ${details.entity_type_rejected || details.entity_type} ID ${details.entity_id_rejected || details.entity_id || 'N/A'} rejected by ${userEmail || 'N/A'}. Reason: ${details.rejection_reason || 'N/A'}.`;
                break;
            case 'APPROVAL_REQUEST_WITHDRAWN':
                detailString = `Approval request for ${formatActionTypeLabel(details.action_type_withdrawn || details.action_type)} on ${details.entity_type_withdrawn || details.entity_type} ID ${details.entity_id_withdrawn || details.entity_id || 'N/A'} withdrawn by ${userEmail || 'N/A'}.`;
                break;
            case 'APPROVAL_REQUEST_AUTO_REJECTED':
                detailString = `Approval request for ${formatActionTypeLabel(details.action_type_auto_rejected || details.action_type)} on ${details.entity_type_auto_rejected || details.entity_type} ID ${details.entity_id_auto_rejected || details.entity_id || 'N/A'} auto-rejected. Reason: ${details.reason || 'N/A'}.`;
                break;
            case 'APPROVAL_INVALIDATED_BY_OTHER_APPROVAL':
                detailString = `Approval request for ${formatActionTypeLabel(details.invalidated_action_type || details.action_type)} on ${details.entity_type || 'N/A'} ID ${details.entity_id || 'N/A'} invalidated by Approval ID: ${details.invalidated_by_approval_id || 'N/A'}.`;
                break;
            case 'LG_OWNER_DETAILS_UPDATED':
            case 'LG_SINGLE_OWNER_CHANGED':
            case 'LG_BULK_OWNER_CHANGED':
                detailString = `Internal owner change. Scope: ${details.scope || 'N/A'}. Old Owner: ${details.old_owner_email || details.old_owner_id || 'N/A'}. New Owner: ${details.new_owner_email || details.new_owner_id || 'N/A'}.`;
                if (details.reason) detailString += ` Reason: ${details.reason}.`;
                break;
            case 'INSTRUCTION_ACCESSED_FOR_PRINT':
                detailString = `Instruction ${details.serial_number || 'N/A'} for LG ${details.lg_number || 'N/A'} accessed for print.`;
                break;
            case 'LG_ACTIVATED':
            case 'LG_ACTIVATE_NON_OPERATIVE':
                detailString = `LG activated. Payment: ${formatAmount(details.payment_details?.amount, details.payment_details?.currency_code || details.payment_currency_code)} via ${details.payment_details?.payment_method || 'N/A'}. Instruction Serial: ${details.instruction_serial || 'N/A'}.`;
                break;
            case 'LG_TOGGLE_AUTO_RENEWAL':
                detailString = `Auto-renewal toggled from ${details.old_status ? 'ON' : 'OFF'} to ${details.new_status ? 'ON' : 'OFF'}. Reason: ${details.reason || 'N/A'}.`;
                break;
            case 'CREATE':
            case 'UPDATE':
            case 'SOFT_DELETE':
            case 'RESTORE':
                if (details.entity_type && (details.name || details.entity_name || details.lg_number || details.email)) {
                    const entityName = details.name || details.entity_name || details.lg_number || details.email;
                    detailString = `${details.entity_type} '${entityName}' was ${actionType.toLowerCase()}d.`;
                    if (details.changes) {
                        detailString += ` Changes: ${JSON.stringify(details.changes)}`;
                    }
                } else {
                    detailString = `Action on ${details.entity_type || 'entity'} ID ${details.entity_id || 'N/A'}.`;
                    if (details.changes) {
                        detailString += ` Changes: ${JSON.stringify(details.changes)}`;
                    }
                }
                break;
            case 'NOTIFICATION_SENT':
                detailString = `Notification sent to ${details.recipient || 'N/A'}. Subject: "${details.subject || 'N/A'}". Method: ${details.method || 'N/A'}. Type: ${details.notification_type || 'General'}.`;
                break;
            case 'NOTIFICATION_FAILED':
                detailString = `Notification failed to send. Reason: ${details.reason || 'N/A'}. Subject: "${details.subject || 'N/A'}".`;
                break;
            case 'LG_RENEWAL_REMINDER_FIRST_SENT':
            case 'LG_RENEWAL_REMINDER_SECOND_SENT':
            case 'LG_OWNER_RENEWAL_REMINDER_SENT':
                detailString = `Renewal reminder (${details.reminder_type || 'N/A'}) sent for LG. Days until expiry: ${details.days_until_expiry || 'N/A'}. Recipients: ${details.recipients?.join(', ') || 'N/A'}. Subject: "${details.email_subject || 'N/A'}".`;
                break;
            case 'LG_OWNER_RENEWAL_REMINDER_SKIPPED_RECENTLY_SENT':
                detailString = `Internal owner renewal reminder skipped because one was sent recently. Last sent at: ${formatDate(details.last_sent_at)}.`;
                break;
            default:
                detailString = Object.entries(details || {}).map(([key, value]) => {
                    if (typeof value === 'object' && value !== null) {
                        return `${formatActionTypeLabel(key)}: ${JSON.stringify(value)}`;
                    }
                    return `${formatActionTypeLabel(key)}: ${value}`;
                }).join(', ');
                if (!detailString) detailString = 'No specific details available.';
                break;
        }
        return detailString;
    };

    const isReminderApplicableByTime = useCallback((instr) => {
        if (isCorporateAdminView) return false;
        if (!instr || (!instr.instruction_date && !instr.delivery_date)) return false;

        const now = new Date();
        const instructionDate = new Date(instr.instruction_date);
        const deliveryDate = instr.delivery_date ? new Date(instr.delivery_date) : null;

        const daysSinceIssuance = differenceInDays(now, instructionDate);
        const daysSinceDelivery = deliveryDate ? differenceInDays(now, deliveryDate) : null;

        const isWithinMaxIssuance = daysSinceIssuance >= 0 && daysSinceIssuance <= reminderThresholds.maxDaysSinceIssuance;

        const conditionA = deliveryDate &&
                           daysSinceDelivery >= reminderThresholds.daysSinceDelivery &&
                           isWithinMaxIssuance;

        const conditionB = !deliveryDate &&
                           daysSinceIssuance >= reminderThresholds.daysSinceIssuance &&
                           isWithinMaxIssuance;

        return conditionA || conditionB;
    }, [reminderThresholds, isCorporateAdminView]);

    const isActionableInstructionType = (instructionType) => {
        return [
            'LG_EXTENSION',
            'LG_LIQUIDATION',
            'LG_RELEASE',
            'LG_DECREASE_AMOUNT',
            'LG_ACTIVATE_NON_OPERATIVE',
        ].includes(instructionType);
    };

    const handleCancelInstruction = (instruction) => {
        if (!isCorporateAdminView && !isGracePeriod) {
            setSelectedInstructionToCancel(instruction);
            setShowCancelModal(true);
        }
    };

    const filteredEvents = React.useMemo(() => {
        if (selectedActionType === 'ALL') {
            return events;
        }
        return events.filter(eventItem => eventItem.action_type === selectedActionType);
    }, [events, selectedActionType]);

    if (isLoading) {
        return (
            <div className="flex justify-center items-center h-48">
                <Loader className="animate-spin text-blue-500" size={32} />
                <p className="ml-2 text-gray-600">Loading lifecycle history...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md flex items-center">
                <AlertCircle className="mr-2" size={20} />
                {error}
            </div>
        );
    }

    if (filteredEvents.length === 0 && selectedActionType === 'ALL') {
        return (
            <div className="bg-blue-100 border border-blue-400 text-blue-700 px-4 py-3 rounded-md">
                <p>No lifecycle events found for this LG record yet.</p>
            </div>
        );
    } else if (filteredEvents.length === 0 && selectedActionType !== 'ALL') {
        return (
            <div className="bg-blue-100 border border-blue-400 text-blue-700 px-4 py-3 rounded-md">
                <p>No "{formatActionTypeLabel(selectedActionType)}" events found for this LG record.</p>
            </div>
        );
    }

    return (
        <div className="relative border-l-2 border-gray-200 pl-6 py-4">
            <div className="mb-6 flex items-center justify-end">
                <label htmlFor="actionTypeFilter" className="mr-2 text-gray-600 font-medium text-sm">Filter by Action Type:</label>
                <div className="relative inline-block text-gray-700">
                    <select
                        id="actionTypeFilter"
                        value={selectedActionType}
                        onChange={(e) => setSelectedActionType(e.target.value)}
                        className="block appearance-none w-full bg-white border border-gray-300 hover:border-gray-400 px-4 py-2 pr-8 rounded-md shadow-sm leading-tight focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 text-sm"
                    >
                        {dynamicFilterOptions.map(option => (
                            <option key={option.value} value={option.value}>
                                {option.label}
                            </option>
                        ))}
                    </select>
                    <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-700">
                        <ChevronDown className="h-4 w-4" />
                    </div>
                </div>
            </div>

            {filteredEvents.map((eventItem, index) => {
                const instruction = getInstructionForEvent(eventItem);

                const deliveryDoc = instruction ? instruction.documents?.find(doc =>
                    doc.document_type === 'DELIVERY_PROOF' && doc.lg_instruction_id === instruction.id
                ) : null;
                const bankReplyDoc = instruction ? instruction.documents?.find(doc =>
                    doc.document_type === 'BANK_REPLY' && doc.lg_instruction_id === instruction.id
                ) : null;

                const isInstructionIssued = instruction && isActionableInstructionType(instruction.instruction_type);
                const hasDeliveryDate = instruction && (instruction.delivery_date !== null && instruction.delivery_date !== "");
                const hasBankReplyDate = instruction && (instruction.bank_reply_date !== null && instruction.bank_reply_date !== "");
                const isReminderInstruction = instruction && instruction.instruction_type === 'LG_REMINDER_TO_BANKS';
                const hasBeenRemindedPreviously = hasReminderBeenSent(instruction?.id);

                const showSendReminderButtonForEndUser = !isCorporateAdminView &&
                                              instruction && Number.isInteger(instruction.id) &&
                                              (instruction.bank_reply_date === null || instruction.bank_reply_date === "") &&
                                              !isReminderInstruction &&
                                              isActionableInstructionType(instruction.instruction_type) &&
                                              isReminderApplicableByTime(instruction) &&
                                              !hasBeenRemindedPreviously &&
                                              !!onSendReminder;

                const showViewIssuedReminderButton = instruction && Number.isInteger(instruction.id) &&
                                                   (hasBeenRemindedPreviously || isReminderInstruction);
                
                const isLatestInstruction = latestCancellableInstruction && instruction?.id === latestCancellableInstruction.id;
                const isCancellableStatus = instruction?.status === 'Instruction Issued' || instruction?.status === 'Reminder Issued';
                const isCancellableType = isActionableInstructionType(instruction?.instruction_type) && !instruction?.instruction_type.includes('REMINDER');

                return (
                    <div key={eventItem.id || `event-${index}`} className="mb-8 relative">
                        <div className="absolute -left-3.5 top-0 flex items-center justify-center w-7 h-7 rounded-full bg-white border-2 border-gray-200 shadow-sm">
                            {getEventIcon(eventItem.action_type)}
                        </div>
                        <div className="bg-white rounded-lg shadow-md p-4 ml-4 border border-gray-200">
                            <div className="flex justify-between items-center mb-2">
                                <h3 className="font-semibold text-lg text-gray-800">{formatActionTypeLabel(eventItem.action_type)}</h3>
                                <span className="text-sm text-gray-500">
                                    {format(new Date(eventItem.timestamp), 'MMM dd, yyyy HH:mm')}
                                </span>
                            </div>
                            <p className="text-gray-700 mb-1">
                                <span className="font-medium">User:</span> {eventItem.user_email || 'System'}
                            </p>
                            <p className="text-gray-600 text-sm">
                                {formatEventDetails(eventItem.action_type, eventItem.details, eventItem.user_email)}
                            </p>

                            {instruction && (
                                <div className="mt-3 flex flex-wrap gap-2">
                                    {!isCorporateAdminView && !isGracePeriod && isLatestInstruction && isCancellableStatus && isCancellableType && (
                                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                            <button
                                                onClick={() => handleCancelInstruction(instruction)}
                                                className="inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-red-100 text-red-700 hover:bg-red-200"
                                                title="Cancel Last Instruction"
                                                disabled={isGracePeriod}
                                            >
                                                <RotateCcw className="h-4 w-4 mr-2" />
                                                Cancel
                                            </button>
                                        </GracePeriodTooltip>
                                    )}

                                    {instruction.generated_content_path && (
                                        <button
                                            onClick={() => onViewInstructionLetter(instruction.id)}
                                            className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                                            title="View Instruction Letter"
                                        >
                                            <FileText className="h-4 w-4 mr-2" />
                                            View Letter
                                        </button>
                                    )}

                                    {isInstructionIssued && !isReminderInstruction && (
                                        <>
                                            {!isCorporateAdminView && !isGracePeriod && !hasDeliveryDate ? (
                                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                    <button
                                                        onClick={() => onRecordDelivery(instruction)}
                                                        className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-blue-600 bg-blue-100 hover:bg-blue-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                                                        title="Record Delivery"
                                                        disabled={isGracePeriod}
                                                    >
                                                        <Truck className="h-4 w-4 mr-2" />
                                                        Record Delivery
                                                    </button>
                                                </GracePeriodTooltip>
                                            ) : hasDeliveryDate ? (
												deliveryDoc ? (
													<button
														onClick={() => onViewInstructionDocument(deliveryDoc.id)}
														className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-gray-700 bg-gray-100 hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500"
														title={`View Delivery Evidence (${format(new Date(instruction.delivery_date), 'MMM dd, yyyy')})`}
													>
														<Download className="h-4 w-4 mr-2" />
														Delivery Evid.
													</button>
												) : (
													<span className="inline-flex items-center px-3 py-1 text-xs font-medium text-gray-500 bg-gray-50 rounded-md">
														Delivered {format(new Date(instruction.delivery_date), 'MMM dd, yyyy')} (No Doc)
													</span>
												)
                                            ) : (
                                                isCorporateAdminView && (
                                                    <span className="inline-flex items-center px-3 py-1 text-xs font-medium text-gray-500 bg-gray-50 rounded-md">
                                                        Delivery Not Recorded
                                                    </span>
                                                )
                                            )}
                                        </>
                                    )}

                                    {isInstructionIssued && !isReminderInstruction && (
                                        <>
                                            {!isCorporateAdminView && !isGracePeriod && !hasBankReplyDate ? (
                                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                    <button
                                                        onClick={() => onRecordBankReply(instruction)}
                                                        className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-teal-600 bg-teal-100 hover:bg-teal-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-teal-500"
                                                        title="Record Bank Reply"
                                                        disabled={isGracePeriod}
                                                    >
                                                        <Building className="h-4 w-4 mr-2" />
                                                        Record Reply
                                                    </button>
                                                </GracePeriodTooltip>
                                            ) : hasBankReplyDate ? (
												bankReplyDoc ? (
													<button
														onClick={() => onViewInstructionDocument(bankReplyDoc.id)}
														className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-gray-700 bg-gray-100 hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500"
														title={`View Bank Reply (${format(new Date(instruction.bank_reply_date), 'MMM dd, yyyy')})`}
													>
														<Download className="h-4 w-4 mr-2" />
														Bank Reply Evid.
													</button>
												) : (
													<span className="inline-flex items-center px-3 py-1 text-xs font-medium text-gray-500 bg-gray-50 rounded-md">
														Replied {format(new Date(instruction.bank_reply_date), 'MMM dd, yyyy')} (No Doc)
													</span>
												)
                                            ) : (
                                                isCorporateAdminView && (
                                                    <span className="inline-flex items-center px-3 py-1 text-xs font-medium text-gray-500 bg-gray-50 rounded-md">
                                                        Bank Reply Not Recorded
                                                    </span>
                                                )
                                            )}

                                            {showSendReminderButtonForEndUser ? (
                                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                    <button
                                                        onClick={() => onSendReminder(instruction)}
                                                        className="inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-orange-100 text-orange-700 hover:bg-orange-200"
                                                        title="Send Reminder to Bank"
                                                        disabled={isGracePeriod}
                                                    >
                                                        <Mail className="h-4 w-4 mr-1" /> Send Reminder
                                                    </button>
                                                </GracePeriodTooltip>
                                            ) : showViewIssuedReminderButton ? (
                                                <button
                                                    onClick={() => onViewInstructionLetter(instruction.id)}
                                                    className="inline-flex items-center px-3 py-1 text-sm font-medium rounded-md shadow-sm bg-yellow-100 text-yellow-800 hover:bg-yellow-200"
                                                    title="View Issued Reminder"
                                                >
                                                    <Eye className="h-4 w-4 mr-1" /> View Issued Reminder
                                                </button>
                                            ) : (
                                                isInstructionIssued && !isReminderInstruction && (
                                                    <span className="inline-flex items-center px-3 py-1 text-xs font-medium text-gray-500 bg-gray-50 rounded-md">
                                                        No Reminder Needed
                                                    </span>
                                                )
                                            )}
                                        </>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                );
            })}
            
            {showCancelModal && selectedInstructionToCancel && (
                <CancelInstructionModal
                    instruction={selectedInstructionToCancel}
                    onClose={() => setShowCancelModal(false)}
                    onSuccess={fetchLifecycleHistoryAndConfig}
                    isGracePeriod={isGracePeriod}
                />
            )}
        </div>
    );
};

export default LGLifecycleHistoryComponent;