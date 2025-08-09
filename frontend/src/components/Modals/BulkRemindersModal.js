// frontend/src/components/Modals/BulkRemindersModal.js
import React, { useState } from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Mail, AlertCircle, Loader2 } from 'lucide-react';
import { apiRequest, API_BASE_URL, getAuthToken } from '../../services/apiService';
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

const buttonBaseClassNames = "inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

const BulkRemindersModal = ({ onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const [isProcessing, setIsProcessing] = useState(false);
    const [processError, setProcessError] = useState('');

    const handleGenerateBulkReminders = async () => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        setIsProcessing(true);
        setProcessError('');
        try {
            const authToken = getAuthToken();
            if (!authToken) {
                setProcessError("Authentication token missing. Please log in again.");
                toast.error("Authentication required to generate reminders.");
                setIsProcessing(false);
                return;
            }

            let url = `${API_BASE_URL}/end-user/lg-records/instructions/generate-all-bank-reminders-pdf`;
            url += `?token=${authToken}`;

            const newWindow = window.open(url, '_blank', 'noopener,noreferrer');
            
            if (newWindow) {
                toast.info("Generating and opening consolidated reminder PDF in a new tab. Please ensure pop-ups are allowed.");
                setTimeout(() => {
                    onSuccess();
                }, 1000);
            } else {
                setProcessError("Failed to open new tab. Please ensure your browser allows pop-ups.");
                toast.error("Failed to open new tab. Check pop-up blocker.");
            }
        } catch (error) {
            console.error("Failed to generate bulk reminders:", error);
            setProcessError(`Failed to generate bulk reminders: ${error.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to generate bulk reminders: ${error.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsProcessing(false);
        }
    };

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
                            leaveFrom="opacity-100"
                            leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                        >
                            <DialogPanel className="relative transform overflow-hidden rounded-lg bg-white px-4 pb-4 pt-5 text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-lg sm:p-6">
                                <div className="absolute right-0 top-0 hidden pr-4 pt-4 sm:block">
                                    <button
                                        type="button"
                                        className="rounded-md bg-white text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
                                        onClick={onClose}
                                    >
                                        <span className="sr-only">Close</span>
                                        <X className="h-6 w-6" aria-hidden="true" />
                                    </button>
                                </div>
                                <div className="sm:flex sm:items-start">
                                    <div className="mt-3 text-center sm:ml-4 sm:mt-0 sm:text-left w-full">
                                        <DialogTitle as="h3" className="text-xl font-semibold leading-6 text-gray-900 border-b pb-3 mb-4">
                                            Generate Bulk Bank Reminders
                                        </DialogTitle>
                                        <div className="mt-2 text-gray-700 space-y-4">
                                            <p>
                                                This action will automatically identify all eligible LG instructions for a bank reminder
                                                and generate a consolidated PDF document containing all such reminders.
                                                The PDF will open in a new tab for printing.
                                            </p>
                                            <p className="text-sm text-gray-600">
                                                Eligible instructions are those where a bank reply has not been recorded, and they fall
                                                within configured time thresholds (e.g., days since issuance/delivery).
                                            </p>

                                            {processError && (
                                                <div className="text-red-600 text-sm mt-2">
                                                    <AlertCircle className="inline h-4 w-4 mr-1" />
                                                    {processError}
                                                </div>
                                            )}

                                            <div className="mt-5 sm:mt-6 sm:grid sm:grid-flow-row-dense sm:grid-cols-2 sm:gap-3">
                                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                    <button
                                                        type="button"
                                                        className={`${buttonBaseClassNames} sm:col-start-2 bg-blue-600 text-white hover:bg-blue-700 ${isProcessing || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                        onClick={handleGenerateBulkReminders}
                                                        disabled={isProcessing || isGracePeriod}
                                                    >
                                                        {isProcessing ? 'Generating...' : <Mail className="h-5 w-5 mr-2" />}
                                                        {isProcessing ? 'Generating...' : 'Generate & Print Reminders'}
                                                    </button>
                                                </GracePeriodTooltip>
                                                <button
                                                    type="button"
                                                    className={`${buttonBaseClassNames} sm:col-start-1 bg-gray-200 text-gray-700 hover:bg-gray-300`}
                                                    onClick={onClose}
                                                    disabled={isProcessing}
                                                >
                                                    Cancel
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </DialogPanel>
                        </TransitionChild>
                    </div>
                </div>
            </Dialog>
        </Transition>
    );
};

export default BulkRemindersModal;