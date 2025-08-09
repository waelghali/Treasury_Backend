// frontend/src/pages/EndUser/LGDetailsPage.js
// This component is now designed to be reusable by Corporate Admin for read-only view.

import { toast } from 'react-toastify';
import React, { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { apiRequest, API_BASE_URL, getAuthToken } from '../../services/apiService';
import { Loader2, AlertCircle, ArrowLeft, Users, CalendarPlus, FileMinus, FileText, CheckCircle, MinusCircle, Mail, Truck, Building, Repeat, Eye } from 'lucide-react';
import moment from 'moment';
import LGLifecycleHistoryComponent from '../../components/LGLifecycleHistoryComponent';
import ChangeLGOwnerModal from '../../components/Modals/ChangeLGOwnerModal';
import ExtendLGModal from '../../components/Modals/ExtendLGModal';
import ReleaseLGModal from '../../components/Modals/ReleaseLGModal';
import LiquidateLGModal from '../../components/Modals/LiquidateLGModal';
import DecreaseAmountModal from '../../components/Modals/DecreaseAmountModal';
import RecordDeliveryModal from '../../components/Modals/RecordDeliveryModal';
import RecordBankReplyModal from '../../components/Modals/RecordBankReplyModal';
import { Switch } from '@headlessui/react';

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

// Helper to get RGB from Tailwind color class (approximate for glow)
const getRgbFromTailwind = (colorClass) => {
    switch (colorClass) {
        case 'bg-green-500': return '59, 201, 107';
        case 'bg-orange-500': return '255, 165, 0';
        case 'bg-red-500': return '239, 68, 68';
        case 'bg-gray-500': return '107, 114, 128';
        case 'bg-purple-500': return '168, 85, 247';
        default: return '255, 255, 255';
    }
};

// Reusable styling for action bar buttons (lighter colors)
const actionBarButtonClassNames = (baseColor) => `
    inline-flex items-center justify-center px-4 py-2 text-sm font-medium rounded-md shadow-sm
    focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200 w-full
    bg-${baseColor}-100 text-${baseColor}-700 hover:bg-${baseColor}-200
`;

// Add isCorporateAdminView and isGracePeriod props
function LGDetailsPage({ isCorporateAdminView = false, isGracePeriod }) {
    const { id } = useParams();
    const navigateToLgList = isCorporateAdminView ? '/corporate-admin/lg-records' : '/end-user/lg-records';
    const navigate = useNavigate();
    const [lgRecord, setLgRecord] = useState(null);
    const [isInitialLoading, setIsInitialLoading] = useState(true);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [error, setError] = useState('');
    const [activeTab, setActiveTab] = useState('details');
    // States for all modals
    const [showChangeOwnerModal, setShowChangeOwnerModal] = useState(false);
    const [showExtendModal, setShowExtendModal] = useState(false);
    const [showReleaseModal, setShowReleaseModal] = useState(false);
    const [showLiquidateModal, setShowLiquidateModal] = useState(false);
    const [showDecreaseAmountModal, setShowDecreaseAmountModal] = useState(false);
    const [showRecordDeliveryModal, setShowRecordDeliveryModal] = useState(false);
    const [showRecordBankReplyModal, setShowRecordBankReplyModal] = useState(false);
    const [selectedInstructionForAction, setSelectedInstructionForAction] = useState(null);

    const fetchLgRecord = useCallback(async (isBackgroundRefresh = false) => {
        if (!isBackgroundRefresh) {
            setIsInitialLoading(true);
        } else {
            setIsRefreshing(true);
        }
        setError('');
        try {
            const response = await apiRequest(`/end-user/lg-records/${id}`, 'GET');
            setLgRecord(response);
        } catch (err) {
            console.error("Failed to fetch LG record details:", err);
            setError(`Failed to load LG record details. ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            if (!isBackgroundRefresh) {
                setIsInitialLoading(false);
            }
            setIsRefreshing(false);
        }
    }, [id]);

    useEffect(() => {
        if (id) {
            fetchLgRecord(false);
        }
    }, [id, fetchLgRecord]);

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

    const handleViewLetter = useCallback(async (lgRecordToView, instructionIdFromModal = null) => {
        let instructionToUse = null;

        if (instructionIdFromModal) {
            instructionToUse = { id: instructionIdFromModal };
        } else {
            instructionToUse = getLatestInstruction(lgRecordToView);
        }

        if (instructionToUse && instructionToUse.id) {
            try {
                if (!isCorporateAdminView) {
                    await apiRequest(
                        `/end-user/lg-records/instructions/${instructionToUse.id}/mark-as-accessed-for-print`,
                        'POST'
                    );
                }

                const authToken = getAuthToken();
                let letterUrl = `${API_BASE_URL}/end-user/lg-records/instructions/${instructionToUse.id}/view-letter`;
                if (authToken) {
                    letterUrl += `?token=${authToken}&print=true`;
                }
                window.open(letterUrl, '_blank');
                toast.info("Opening letter in new tab. Please check for pop-up blockers.");
            } catch (error) {
                console.error("Failed to mark instruction as accessed for print or open letter:", error);
                setError("Could not open letter. Please try again later.");
                toast.error(`Could not open letter. ${error.message || 'An unexpected error occurred.'}`);
            }
        } else {
            setError('No generated letter found for this LG, or instruction ID is missing.');
            toast.error('No generated letter found for this LG, or instruction ID is missing.');
        }
    }, [getLatestInstruction, isCorporateAdminView]);

    const handleActionSuccess = useCallback((updatedRecordFromBackend, latestInstructionId = null) => {
        setShowChangeOwnerModal(false);
        setShowExtendModal(false);
        setShowReleaseModal(false);
        setShowLiquidateModal(false);
        setShowDecreaseAmountModal(false);
        setShowRecordDeliveryModal(false);
        setShowRecordBankReplyModal(false);
        setSelectedInstructionForAction(null);

        setLgRecord(updatedRecordFromBackend);

        if (latestInstructionId) {
            setTimeout(() => {
                handleViewLetter(updatedRecordFromBackend, latestInstructionId);
            }, 50);
        } else {
            fetchLgRecord(true);
        }
        toast.success("LG action completed successfully!");
    }, [fetchLgRecord, handleViewLetter]);

    const handleExtend = () => {
        if (!isCorporateAdminView && lgRecord && !isGracePeriod) { // NEW: Add isGracePeriod check
            setShowExtendModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleRelease = () => {
        if (!isCorporateAdminView && lgRecord && !isGracePeriod) { // NEW: Add isGracePeriod check
            setShowReleaseModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleLiquidate = () => {
        if (!isCorporateAdminView && lgRecord && !isGracePeriod) { // NEW: Add isGracePeriod check
            setShowLiquidateModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleDecreaseAmount = () => {
        if (!isCorporateAdminView && lgRecord && !isGracePeriod) { // NEW: Add isGracePeriod check
            setShowDecreaseAmountModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleChangeOwner = () => {
        if (!isCorporateAdminView && lgRecord && !isGracePeriod) { // NEW: Add isGracePeriod check
            setShowChangeOwnerModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleToggleAutoRenewal = useCallback(async (newStatus) => {
        if (isCorporateAdminView || !lgRecord || isGracePeriod) { // NEW: Add isGracePeriod check
            if (isGracePeriod) toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }

        const originalLgRecord = { ...lgRecord };

        setLgRecord(prevRecord => ({
            ...prevRecord,
            auto_renewal: newStatus,
        }));

        try {
            const payload = {
                auto_renewal: newStatus,
                reason: `Auto-renewal toggled to ${newStatus ? 'ON' : 'OFF'} from details page.`
            };
            const response = await apiRequest(`/end-user/lg-records/${lgRecord.id}/toggle-auto-renewal`, 'POST', payload);
            toast.success(response.message);
            setLgRecord(prevRecord => ({
                ...prevRecord,
                ...response.lg_record
            }));
        } catch (err) {
            console.error("Failed to toggle auto-renewal:", err);
            setLgRecord(originalLgRecord);
            toast.error(`Failed to toggle auto-renewal. ${err.message || 'An unexpected error occurred.'}`);
        }
    }, [lgRecord, isCorporateAdminView, isGracePeriod]); // NEW: Add isGracePeriod to dependencies

    const handleRecordDelivery = (instruction) => {
        if (!isCorporateAdminView && !isGracePeriod) { // NEW: Add isGracePeriod check
            setSelectedInstructionForAction(instruction);
            setShowRecordDeliveryModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleRecordBankReply = (instruction) => {
        if (!isCorporateAdminView && !isGracePeriod) { // NEW: Add isGracePeriod check
            setSelectedInstructionForAction(instruction);
            setShowRecordBankReplyModal(true);
        } else if (isGracePeriod) {
            toast.warn("This action is disabled during your subscription's grace period.");
        }
    };

    const handleSendReminderToBank = async (instruction) => {
        if (isCorporateAdminView || isGracePeriod) { // NEW: Add isGracePeriod check
            if (isGracePeriod) toast.warn("This action is disabled during your subscription's grace period.");
            return;
        }

        setIsRefreshing(true);
        try {
            const response = await apiRequest(`/end-user/lg-records/instructions/${instruction.id}/send-reminder-to-bank`, 'POST');
            toast.success(response.message);

            const authToken = getAuthToken();
            if (authToken && response.new_instruction_id) {
                const letterUrl = `${API_BASE_URL}/end-user/lg-records/instructions/${response.new_instruction_id}/view-letter?token=${authToken}&print=true`;
                window.open(letterUrl, '_blank');
            }
            fetchLgRecord(true);
        } catch (error) {
            console.error("Failed to send reminder:", error);
            toast.error(`Failed to send reminder: ${error.message || 'An unexpected error occurred.'}`);
        } finally {
            setIsRefreshing(false);
        }
    };

	const handleViewInstructionDocument = useCallback(async (documentId) => {
		if (!documentId) {
			toast.error("Document ID not found.");
			return;
		}

		try {
			const response = await apiRequest(`/end-user/lg-documents/${documentId}/view`, 'GET');
			const signedUrl = response.signed_url;

			if (signedUrl) {
				window.open(signedUrl, '_blank');
				toast.info("Opening document securely in a new tab.");
			} else {
				toast.error("Failed to retrieve secure document link.");
			}
		} catch (error) {
			console.error("Failed to view document securely:", error);
			toast.error(`Could not open document: ${error.message || 'An unexpected error occurred.'}`);
		}
	}, []);

    const handleViewInstructionLetterFromHistory = async (instructionId) => {
        try {
            if (!isCorporateAdminView) {
                await apiRequest(
                    `/end-user/lg-records/instructions/${instructionId}/mark-as-accessed-for-print`,
                    'POST'
                );
            }

            const authToken = getAuthToken();
            let letterUrl = `${API_BASE_URL}/end-user/lg-records/instructions/${instructionId}/view-letter`;
            if (authToken) {
                letterUrl += `?token=${authToken}&print=true`;
            }
            window.open(letterUrl, '_blank');

        } catch (error) {
            console.error("Failed to mark instruction as accessed for print or open letter:", error);
            toast.error("Could not open letter. Please try again later.");
        }
    };


    const formatAmount = (amount, currencyCode) => {
        if (amount === null || currencyCode === null || currencyCode === undefined || isNaN(parseFloat(amount))) {
            return 'N/A';
        }
        try {
            const num = parseFloat(amount);
            const formattedNumber = num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return `${currencyCode} ${formattedNumber}`;
        } catch (e) {
            console.error("Error formatting currency:", e);
            return `${currencyCode} ${parseFloat(amount).toFixed(2)}`;
        }
    };

    const formatDate = (dateString) => {
        if (!dateString) return 'N/A';
        return moment(dateString).format('DD-MMM-YYYY');
    };

    const getExpiryStatusBarProps = (lgRecord) => {
        const defaultProps = {
            barColorClass: 'bg-gray-600',
            fillColorClass: 'bg-gray-300',
            labelText: 'Status Unavailable',
            percentage: 100,
            indicatorColor: 'text-white',
            glowShadow: 'none'
        };

        if (!lgRecord || !lgRecord.lg_status) {
            return defaultProps;
        }

        const statusName = lgRecord.lg_status.name;
        const expiryDate = moment(lgRecord.expiry_date);
        const today = moment();

        if (statusName === 'Valid') {
            const remainingDays = expiryDate.diff(today, 'days');

            const CRITICAL_DAYS = 30;
            const MODERATE_DAYS = 60;

            let fillColor = '';
            let label = `${remainingDays} days remaining`;
            let calculatedPercentage = 0;
            let glowRgb = '';

            if (remainingDays <= 0) {
                fillColor = 'bg-red-500';
                label = 'Expired';
                calculatedPercentage = 100;
                glowRgb = getRgbFromTailwind(fillColor);
            } else if (remainingDays <= CRITICAL_DAYS) {
                fillColor = 'bg-red-500';
                calculatedPercentage = ((CRITICAL_DAYS - remainingDays) / CRITICAL_DAYS) * 100;
                calculatedPercentage = Math.min(100, Math.max(0, calculatedPercentage));
                glowRgb = getRgbFromTailwind(fillColor);
            } else if (remainingDays <= MODERATE_DAYS) {
                fillColor = 'bg-orange-500';
                calculatedPercentage = ((MODERATE_DAYS - remainingDays) / (MODERATE_DAYS - CRITICAL_DAYS)) * 100;
                calculatedPercentage = Math.min(100, Math.max(0, calculatedPercentage));
                glowRgb = getRgbFromTailwind(fillColor);
            } else {
                fillColor = 'bg-green-500';
                calculatedPercentage = 0;
                glowRgb = getRgbFromTailwind(fillColor);
            }

            const glowShadow = `0 0 8px rgba(${glowRgb}, 0.7), 0 0 15px rgba(${glowRgb}, 0.4)`;

            return {
                barColorClass: 'bg-gray-800',
                fillColorClass: fillColor,
                labelText: label,
                percentage: calculatedPercentage,
                indicatorColor: 'text-white',
                glowShadow: glowShadow
            };
        } else {
            let barFillColor = '';
            let barGlowRgb = '';
            if (statusName === 'Expired') {
                barFillColor = 'bg-red-500';
            } else if (statusName === 'Released') {
                barFillColor = 'bg-gray-500';
            } else if (statusName === 'Liquidated') {
                barFillColor = 'bg-purple-500';
            } else {
                barFillColor = 'bg-gray-500';
            }
            barGlowRgb = getRgbFromTailwind(barFillColor);
            const glowShadow = `0 0 8px rgba(${barGlowRgb}, 0.7), 0 0 15px rgba(${barGlowRgb}, 0.4)`;

            return {
                barColorClass: 'bg-gray-800',
                fillColorClass: barFillColor,
                labelText: statusName,
                percentage: 100,
                indicatorColor: 'text-white',
                glowShadow: glowShadow
            };
        }
    };

    const statusBarProps = lgRecord ? getExpiryStatusBarProps(lgRecord) : null;

    if (isInitialLoading) {
        return (
            <div className="flex justify-center items-center h-screen bg-gray-100">
                <Loader2 className="animate-spin h-10 w-10 text-blue-600" />
                <p className="ml-3 text-gray-700">Loading LG record details...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative m-6" role="alert">
                <AlertCircle className="h-5 w-5 mr-2 inline-block" />
                <span className="block sm:inline">{error}</span>
            </div>
        );
    }

    if (!lgRecord) {
        return (
            <div className="bg-blue-100 border border-blue-400 text-blue-700 px-4 py-3 rounded-md relative m-6" role="alert">
                <span className="block sm:inline">LG Record not found.</span>
            </div>
        );
    }

    const isLgValid = lgRecord.lg_status?.name === 'Valid';
    const isLgValidOrActive = ['Valid', 'Active'].includes(lgRecord.lg_status?.name);


    return (
        <div className="container mx-auto p-6 bg-white rounded-lg shadow-xl my-8 relative">
            {isRefreshing && (
                <div className="absolute inset-x-0 top-0 flex items-center justify-center py-1 bg-blue-100 text-blue-700 text-sm font-medium z-10 rounded-t-lg">
                    <Loader2 className="animate-spin h-4 w-4 mr-2" /> Refreshing data...
                </div>
            )}
            {/* Action Buttons: Full width, spaced evenly */}
            {!isCorporateAdminView && ( // Hide action bar if Corporate Admin
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-2 w-full p-3 bg-gray-50 rounded-lg shadow-inner mb-6">
                    {isLgValid && ( // Only show Extend for 'Valid' LGs
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={handleExtend}
                                className={`${actionBarButtonClassNames('indigo')} ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Extend LG"
                                disabled={isGracePeriod}
                            >
                                <CalendarPlus className="h-5 w-5 mr-2" />
                                Extend
                            </button>
                        </GracePeriodTooltip>
                    )}
                    {isLgValidOrActive && ( // Show Release for 'Valid' or 'Active' LGs
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={handleRelease}
                                className={`${actionBarButtonClassNames('green')} ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Release LG"
                                disabled={isGracePeriod}
                            >
                                <CheckCircle className="h-5 w-5 mr-2" />
                                Release
                            </button>
                        </GracePeriodTooltip>
                    )}
                    {isLgValidOrActive && ( // Show Liquidate for 'Valid' or 'Active' LGs
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={handleLiquidate}
                                className={`${actionBarButtonClassNames('red')} ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Liquidate LG"
                                disabled={isGracePeriod}
                            >
                                <FileMinus className="h-5 w-5 mr-2" />
                                Liquidate
                            </button>
                        </GracePeriodTooltip>
                    )}
                    {isLgValidOrActive && ( // Show Decrease Amount for 'Valid' or 'Active' LGs
                        <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                            <button
                                onClick={handleDecreaseAmount}
                                className={`${actionBarButtonClassNames('orange')} ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                title="Decrease LG Amount"
                                disabled={isGracePeriod}
                            >
                                <MinusCircle className="h-5 w-5 mr-2" />
                                Decrease Amt
                            </button>
                        </GracePeriodTooltip>
                    )}
                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                        <button // Change Owner can always be done (if not Corporate Admin view)
                            onClick={handleChangeOwner}
                            className={`${actionBarButtonClassNames('purple')} ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                            title="Change Internal Owner"
                            disabled={isGracePeriod}
                        >
                            <Users className="h-5 w-5 mr-2" />
                            Change Owner
                        </button>
                    </GracePeriodTooltip>
                </div>
            )}

            <div className="flex items-center justify-between mb-6">
                <button
                    onClick={() => navigate(navigateToLgList)}
                    className="flex items-center text-gray-600 hover:text-gray-900 transition-colors duration-200 mr-4"
                >
                    <ArrowLeft className="h-5 w-5 mr-1" />
                    <span className="text-sm font-medium">Back to LG Records</span>
                </button>
                <h1 className="text-3xl font-bold text-gray-800 flex-grow text-center">LG Record: {lgRecord.lg_number}</h1>
                <div className="flex-shrink-0 w-auto">
                </div>
            </div>

            {statusBarProps && (
                <div className={`w-full h-6 rounded-full ${statusBarProps.barColorClass} relative overflow-hidden mb-6 shadow-inner`}>
                    <div
                        className={`h-full rounded-full ${statusBarProps.fillColorClass} transition-all duration-500 ease-in-out`}
                        style={{
                            width: `${statusBarProps.percentage}%`,
                            boxShadow: statusBarProps.glowShadow,
                        }}
                    >
                        <div className="absolute inset-0 rounded-full opacity-70" style={{
                            background: `radial-gradient(circle at right, rgba(${getRgbFromTailwind(statusBarProps.fillColorClass)}, 0.9) 0%, transparent 60%)`
                        }}></div>
                    </div>
                    <div className="absolute inset-0 flex items-center justify-center">
                        <span className={`font-semibold text-sm md:text-base ${statusBarProps.indicatorColor} text-shadow-sm`}>
                            {statusBarProps.labelText}
                        </span>
                    </div>
                </div>
            )}

            <div className="border-b border-gray-200 mb-6">
                <nav className="-mb-px flex space-x-8" aria-label="Tabs">
                    <button
                        onClick={() => setActiveTab('details')}
                        className={`${
                            activeTab === 'details'
                                ? 'border-indigo-500 text-indigo-600'
                                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                        } whitespace-nowrap py-3 px-1 border-b-2 font-medium text-base transition-colors duration-200`}
                    >
                        Details
                    </button>
                    <button
                        onClick={() => setActiveTab('documents')}
                        className={`${
                            activeTab === 'documents'
                                ? 'border-indigo-500 text-indigo-600'
                                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                        } whitespace-nowrap py-3 px-1 border-b-2 font-medium text-base transition-colors duration-200`}
                    >
                        Documents
                    </button>
                    <button
                        onClick={() => setActiveTab('history')}
                        className={`${
                            activeTab === 'history'
                                ? 'border-indigo-500 text-indigo-600'
                                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                        } whitespace-nowrap py-3 px-1 border-b-2 font-medium text-base transition-colors duration-200`}
                    >
                        Lifecycle History
                    </button>
                </nav>
            </div>

            <div>
                {activeTab === 'details' && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-4 text-gray-700">
                        <div className="col-span-2">
                            <h2 className="text-xl font-semibold text-gray-800 mb-4 border-b pb-2">LG Core Information</h2>
                        </div>
                        <p><strong>LG Number:</strong> {lgRecord.lg_number}</p>
                        <p><strong>Beneficiary:</strong> {lgRecord.beneficiary_corporate?.entity_name || 'N/A'}</p>
                        <p><strong>Amount:</strong> {formatAmount(lgRecord.lg_amount, lgRecord.lg_currency?.iso_code)}</p>
                        <p><strong>Issuance Date:</strong> {formatDate(lgRecord.issuance_date)}</p>
                        <p><strong>Expiry Date:</strong> {formatDate(lgRecord.expiry_date)}</p>
                        <p><strong>LG Type:</strong> {lgRecord.lg_type?.name || 'N/A'}</p>
                        <p><strong>Status:</strong> <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            lgRecord.lg_status?.name === 'Valid' ? 'bg-green-100 text-green-800' :
                            lgRecord.lg_status?.name === 'Expired' ? 'bg-red-100 text-red-800' :
                            'bg-gray-100 text-gray-800'
                        }`}>{lgRecord.lg_status?.name || 'N/A'}</span></p>
                        <p><strong>Operational Status:</strong> {lgRecord.lg_operational_status?.name || 'N/A'}</p>
                        <p><strong>Purpose:</strong> {lgRecord.description_purpose || 'N/A'}</p>
                        {!isCorporateAdminView && (
                            <div className="flex items-center">
                                <strong>Auto Renewal:</strong>
                                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                    <Switch
                                        checked={lgRecord.auto_renewal}
                                        onChange={handleToggleAutoRenewal}
                                        disabled={isGracePeriod} // NEW: Disable the switch
                                        className={`${
                                            lgRecord.auto_renewal ? 'bg-blue-600' : 'bg-gray-200'
                                        } relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ml-3 ${isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                    >
                                        <span className="sr-only">Enable auto-renewal</span>
                                        <span
                                            className={`${
                                                lgRecord.auto_renewal ? 'translate-x-6' : 'translate-x-1'
                                            } inline-block h-4 w-4 transform rounded-full bg-white transition-transform`}
                                        />
                                    </Switch>
                                </GracePeriodTooltip>
                            </div>
                        )}
                        <p><strong>Period (Months):</strong> {lgRecord.lg_period_months}</p>

                        <div className="col-span-2 mt-6">
                            <h2 className="text-xl font-semibold text-gray-800 mb-4 border-b pb-2">Bank & Rule Information</h2>
                        </div>
                        <p><strong>Issuing Bank:</strong> {lgRecord.issuing_bank?.name || 'N/A'}</p>
                        <p><strong>Bank Address:</strong> {lgRecord.issuing_bank_address || 'N/A'}</p>
                        <p><strong>Bank Phone:</strong> {lgRecord.issuing_bank_phone || 'N/A'}</p>
                        <p><strong>Issuing Method:</strong> {lgRecord.issuing_method?.name || 'N/A'}</p>
                        <p><strong>Applicable Rule:</strong> {lgRecord.applicable_rule?.name || 'N/A'}</p>
                        <p><strong>Rules Text:</strong> {lgRecord.applicable_rules_text || 'N/A'}</p>
                        <p><strong>Other Conditions:</strong> {lgRecord.other_conditions || 'N/A'}</p>

                        <div className="col-span-2 mt-6">
                            <h2 className="text-xl font-semibold text-gray-800 mb-4 border-b pb-2">Internal & Category Details</h2>
                        </div>
                        <p><strong>Internal Owner:</strong> {lgRecord.internal_owner_contact?.email || 'N/A'}</p>
                        <p><strong>Owner Phone:</strong> {lgRecord.internal_owner_contact?.phone_number || 'N/A'}</p>
                        <p><strong>Owner Manager:</strong> {lgRecord.internal_owner_contact?.manager_email || 'N/A'}</p>
                        <p><strong>LG Category:</strong> {lgRecord.lg_category?.category_name || 'N/A'}</p>
                        {lgRecord.lg_category?.extra_field_name && (
                             <p><strong>{lgRecord.lg_category.extra_field_name}:</strong> {lgRecord.additional_field_values?.[lgRecord.lg_category.extra_field_name] || 'N/A'}</p>
                        )}
                        <p><strong>Internal Project ID:</strong> {lgRecord.internal_contract_project_id || 'N/A'}</p>
                        <p><strong>Notes:</strong> {lgRecord.notes || 'N/A'}</p>
                    </div>
                )}
				{activeTab === 'documents' && (
					<div className="p-4 bg-gray-50 rounded-lg">
						<h2 className="text-xl font-semibold text-gray-800 mb-4">Associated Documents</h2>
						{lgRecord.documents && lgRecord.documents.length > 0 ? (
							<ul className="list-disc list-inside space-y-2">
								{lgRecord.documents.map(doc => (
									<li key={doc.id} className="text-gray-700 flex items-center justify-between">
										<span>
											{doc.file_name} ({doc.document_type})
										</span>
										<button
											onClick={() => handleViewInstructionDocument(doc.id)}
											className="ml-4 px-3 py-1 text-sm font-medium rounded-md shadow-sm text-blue-600 bg-blue-100 hover:bg-blue-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 flex items-center"
											title="View Document Securely"
										>
											<Eye className="h-4 w-4 mr-1" /> View
										</button>
									</li>
								))}
							</ul>
						) : (
							<p className="text-gray-500">No documents associated with this LG record.</p>
						)}
					</div>
				)}
                {activeTab === 'history' && (
                    <LGLifecycleHistoryComponent
                        lgRecordId={lgRecord.id}
                        lgInstructions={lgRecord.instructions || []}
                        lgDocuments={lgRecord.documents || []}
                        onRecordDelivery={isCorporateAdminView || isGracePeriod ? null : handleRecordDelivery} // NEW: Pass null if in grace period
                        onRecordBankReply={isCorporateAdminView || isGracePeriod ? null : handleRecordBankReply} // NEW: Pass null if in grace period
                        onSendReminder={isCorporateAdminView || isGracePeriod ? null : handleSendReminderToBank} // NEW: Pass null if in grace period
                        onViewInstructionLetter={handleViewInstructionLetterFromHistory}
                        onViewInstructionDocument={handleViewInstructionDocument}
                    />
                )}
            </div>

            {!isCorporateAdminView && (
                <>
                    {showChangeOwnerModal && lgRecord && (
                        <ChangeLGOwnerModal
                            lgRecord={lgRecord}
                            onClose={() => setShowChangeOwnerModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showExtendModal && lgRecord && (
                        <ExtendLGModal
                            lgRecord={lgRecord}
                            onClose={() => setShowExtendModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showReleaseModal && lgRecord && (
                        <ReleaseLGModal
                            lgRecord={lgRecord}
                            onClose={() => setShowReleaseModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showLiquidateModal && lgRecord && (
                        <LiquidateLGModal
                            lgRecord={lgRecord}
                            onClose={() => setShowLiquidateModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showDecreaseAmountModal && lgRecord && (
                        <DecreaseAmountModal
                            lgRecord={lgRecord}
                            onClose={() => setShowDecreaseAmountModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showRecordDeliveryModal && selectedInstructionForAction && (
                        <RecordDeliveryModal
                            instruction={selectedInstructionForAction}
                            onClose={() => setShowRecordDeliveryModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                    {showRecordBankReplyModal && selectedInstructionForAction && (
                        <RecordBankReplyModal
                            instruction={selectedInstructionForAction}
                            onClose={() => setShowRecordBankReplyModal(false)}
                            onSuccess={handleActionSuccess}
                            isGracePeriod={isGracePeriod} // NEW: Pass prop
                        />
                    )}
                </>
            )}
        </div>
    );
}

export default LGDetailsPage;