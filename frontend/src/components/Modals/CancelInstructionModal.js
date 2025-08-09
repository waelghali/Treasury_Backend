import React, { useState, useEffect } from 'react';
import { X, AlertTriangle, Loader2 } from 'lucide-react';
import { toast } from 'react-toastify';
import moment from 'moment';

import { cancelLGInstruction } from '../../services/apiService';
import { apiRequest } from '../../services/apiService';

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

const CancelInstructionModal = ({ instruction, onClose, onSuccess, isGracePeriod }) => {
    const [reason, setReason] = useState('');
    const [declarationConfirmed, setDeclarationConfirmed] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [cancellationWindowHours, setCancellationWindowHours] = useState(24);
    const [timeRemaining, setTimeRemaining] = useState(null);

    useEffect(() => {
		const fetchCancellationWindow = async () => {
			try {
				const response = await apiRequest('/end-user/customer-configurations/MAX_DAYS_FOR_LAST_INSTRUCTION_CANCELLATION', 'GET');
				setCancellationWindowHours(parseInt(response.effective_value, 10) * 24);
			} catch (error) {
				console.error("Failed to fetch cancellation window config:", error);
				setCancellationWindowHours(24); 
			}
		};

        fetchCancellationWindow();
    }, []);
    
    useEffect(() => {
        if (!instruction || !instruction.created_at) return;

        const interval = setInterval(() => {
            const now = moment();
            const created = moment(instruction.created_at);
            const cutoff = created.add(cancellationWindowHours, 'hours');
            const diff = cutoff.diff(now);

            if (diff > 0) {
                const duration = moment.duration(diff);
                const hours = Math.floor(duration.asHours());
                const minutes = duration.minutes();
                const seconds = duration.seconds();
                setTimeRemaining(`${hours}h ${minutes}m ${seconds}s`);
            } else {
                setTimeRemaining('0h 0m 0s');
                clearInterval(interval);
            }
        }, 1000);

        return () => clearInterval(interval);
    }, [instruction, cancellationWindowHours]);


    const handleCancel = async () => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }

        if (!reason.trim()) {
            toast.error("Please provide a reason for cancellation.");
            return;
        }
        if (!declarationConfirmed) {
            toast.error("You must confirm the declaration to proceed.");
            return;
        }

        setIsLoading(true);
        try {
            const response = await cancelLGInstruction(instruction.id, reason, declarationConfirmed);
            toast.success(response.message);
            onSuccess(response.lg_record, response.instruction.id);
        } catch (error) {
            console.error("Failed to cancel instruction:", error);
            toast.error(`Failed to cancel instruction: ${error.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsLoading(false);
        }
    };

    const isButtonDisabled = isLoading || !reason.trim() || !declarationConfirmed || timeRemaining === '0h 0m 0s' || isGracePeriod; // NEW: Added isGracePeriod

    const formatActionTypeLabel = (actionType) => {
        if (!actionType) return '';
        return actionType.replace(/_/g, ' ').split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ');
    };

    return (
        <div className="fixed inset-0 z-50 overflow-y-auto">
            <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
                <div className="fixed inset-0 transition-opacity" aria-hidden="true">
                    <div className="absolute inset-0 bg-gray-500 opacity-75"></div>
                </div>

                <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

                <div className="inline-block align-bottom bg-white rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
                    <div className="absolute top-0 right-0 pt-4 pr-4">
                        <button
                            type="button"
                            className="bg-white rounded-md text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                            onClick={onClose}
                        >
                            <span className="sr-only">Close</span>
                            <X className="h-6 w-6" />
                        </button>
                    </div>

                    <div className="sm:flex sm:items-start">
                        <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-red-100 sm:mx-0 sm:h-10 sm:w-10">
                            <AlertTriangle className="h-6 w-6 text-red-600" />
                        </div>
                        <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
                            <h3 className="text-lg leading-6 font-medium text-gray-900">
                                Cancel Last Instruction
                            </h3>
                            <div className="mt-2 text-sm text-gray-500">
                                <p>You are about to cancel the following instruction. This action will roll back the LG to its previous state.</p>
                                <div className="mt-4 p-4 border rounded-md bg-gray-50">
                                    <p><strong>Instruction Type:</strong> {formatActionTypeLabel(instruction.instruction_type)}</p>
                                    <p><strong>Instruction Serial:</strong> {instruction.serial_number}</p>
                                    <p><strong>LG Number:</strong> {instruction.lg_record?.lg_number || 'N/A'}</p>
                                    <p><strong>Issued On:</strong> {moment(instruction.created_at).format('DD-MMM-YYYY HH:mm')}</p>
                                    <p className="mt-2 text-red-600 font-bold">
                                        Time left to cancel: {timeRemaining || 'Loading...'}
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className={`mt-5 ${isGracePeriod ? 'opacity-50' : ''}`}>
                        <label htmlFor="reason" className="block text-sm font-medium text-gray-700">Reason for Cancellation</label>
                        <div className="mt-1">
                            <textarea
                                id="reason"
                                name="reason"
                                rows="3"
                                className="shadow-sm focus:ring-indigo-500 focus:border-indigo-500 mt-1 block w-full sm:text-sm border-gray-300 rounded-md"
                                placeholder="e.g., Incorrect amount was entered"
                                value={reason}
                                onChange={(e) => setReason(e.target.value)}
                                disabled={isGracePeriod} // NEW: Disable textarea
                            ></textarea>
                        </div>
                    </div>

                    <div className={`mt-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
                        <div className="flex items-start">
                            <div className="flex items-center h-5">
                                <input
                                    id="declaration"
                                    name="declaration"
                                    type="checkbox"
                                    className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
                                    checked={declarationConfirmed}
                                    onChange={(e) => setDeclarationConfirmed(e.target.checked)}
                                    disabled={isGracePeriod} // NEW: Disable checkbox
                                />
                            </div>
                            <div className="ml-3 text-sm">
                                <label htmlFor="declaration" className="font-medium text-gray-700">
                                    I confirm that this instruction is to be considered null and void,
                                    that it has not been delivered to the bank, or if delivered, all necessary
                                    steps have been taken to stop its use, including shredding physical copies
                                    and ensuring it will not be used in any way. I take full responsibility for this action.
                                </label>
                            </div>
                        </div>
                    </div>

                    <div className="mt-5 sm:mt-4 sm:flex sm:flex-row-reverse">
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                type="button"
                                className={`w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 text-base font-medium text-white sm:ml-3 sm:w-auto sm:text-sm
                                    ${isButtonDisabled ? 'bg-red-300 cursor-not-allowed' : 'bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500'}`}
                                onClick={handleCancel}
                                disabled={isButtonDisabled}
                            >
                                {isLoading ? (
                                    <Loader2 className="animate-spin h-5 w-5 text-white" />
                                ) : (
                                    'Confirm Cancellation'
                                )}
                            </button>
                        </GracePeriodTooltip>
                        <button
                            type="button"
                            className="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 sm:mt-0 sm:w-auto sm:text-sm"
                            onClick={onClose}
                            disabled={isLoading}
                        >
                            Back
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default CancelInstructionModal;