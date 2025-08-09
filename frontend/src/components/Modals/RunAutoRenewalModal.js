// frontend/src/components/Modals/RunAutoRenewalModal.js
import React, { useState } from 'react';
import { apiRequest, API_BASE_URL, getAuthToken } from '../../services/apiService';
import { Loader2, XCircle, CheckCircle, Download, Repeat } from 'lucide-react';
import { toast } from 'react-toastify';
import moment from 'moment';

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

const RunAutoRenewalModal = ({ onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const [isProcessing, setIsProcessing] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState('');

    const handleRunRenewal = async () => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }
        setIsProcessing(true);
        setError('');
        setResult(null);

        try {
            const response = await apiRequest('/end-user/lg-records/run-auto-renewal', 'POST', {});
            setResult(response);
            toast.success(response.message);
            onSuccess();

            if (response.combined_pdf_base64) {
                try {
                    const byteCharacters = atob(response.combined_pdf_base64);
                    const byteNumbers = new Array(byteCharacters.length);
                    for (let i = 0; i < byteCharacters.length; i++) {
                        byteNumbers[i] = byteCharacters.charCodeAt(i);
                    }
                    const byteArray = new Uint8Array(byteNumbers);
                    const blob = new Blob([byteArray], { type: 'application/pdf' });

                    const fileURL = URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    link.href = fileURL;
                    link.target = '_blank';
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);

                    setTimeout(() => URL.revokeObjectURL(fileURL), 100); 
                    toast.info("Combined PDF opened in a new tab for viewing/printing.");
                } catch (blobError) {
                    console.error("Error creating Blob or opening PDF:", blobError);
                    toast.error("Failed to open PDF automatically. Please close and retry.");
                }
            }
        } catch (err) {
            console.error("Failed to run auto-renewal:", err);
            setError(`Failed to run auto-renewal: ${err.message || 'An unexpected error occurred.'}`);
            toast.error(`Failed to run auto-renewal: ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsProcessing(false);
        }
    };

    const handleDownloadPdf = () => {
        if (result && result.combined_pdf_base64) {
            const link = document.createElement('a');
            link.href = `data:application/pdf;base64,${result.combined_pdf_base64}`;
            link.download = `bulk_lg_renewal_${moment().format('YYYYMMDD_HHmmss')}.pdf`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            toast.info("Combined PDF download initiated.");
        } else {
            toast.warn("No PDF available for download.");
        }
    };

    return (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50 flex justify-center items-center">
            <div className="bg-white p-6 rounded-lg shadow-xl w-11/12 md:w-2/3 lg:w-1/2">
                <h2 className="text-2xl font-bold text-gray-800 mb-4 border-b pb-2">Run Auto/Forced LG Renewal</h2>
                
                {error && (
                    <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                        <XCircle className="h-5 w-5 mr-2" />
                        <span className="block sm:inline">{error}</span>
                    </div>
                )}

                {result ? (
                    <div className="text-center py-4">
                        <CheckCircle className="h-16 w-16 text-green-500 mx-auto mb-4" />
                        <p className="text-xl text-gray-700 font-semibold mb-2">{result.message}</p>
                        <p className="text-gray-600">Successfully renewed: <span className="font-bold">{result.renewed_count} LGs</span>.</p>
                        <button
                            onClick={onClose}
                            className="mt-6 ml-4 inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
                        >
                            Close
                        </button>
                    </div>
                ) : (
                    <>
                        <p className="text-gray-700 mb-4">
                            This process will identify all eligible Letters of Guarantee for auto-renewal
                            (based on `auto_renewal` status and `AUTO_RENEWAL_DAYS_BEFORE_EXPIRY` setting)
                            and for forced renewal (based on `FORCED_RENEW_DAYS_BEFORE_EXPIRY` setting)
                            for your customer.
                        </p>
                        <p className="text-gray-700 mb-6">
                            For each eligible LG, the system will automatically extend its expiry date,
                            generate an individual instruction letter, and send an individual email notification.
                            A single, combined PDF containing all generated instruction letters will be provided for download.
                        </p>

                        <div className="flex justify-end space-x-4">
                            <button
                                type="button"
                                onClick={onClose}
                                className={`inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                disabled={isGracePeriod}
                            >
                                Cancel
                            </button>
                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                <button
                                    type="button"
                                    onClick={handleRunRenewal}
                                    disabled={isProcessing || isGracePeriod}
                                    className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-orange-600 hover:bg-orange-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-orange-500 ${isProcessing || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                >
                                    {isProcessing ? (
                                        <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                                    ) : (
                                        <Repeat className="h-5 w-5 mr-2" />
                                    )}
                                    {isProcessing ? 'Processing...' : 'Confirm & Run Renewal'}
                                </button>
                            </GracePeriodTooltip>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
};

export default RunAutoRenewalModal;