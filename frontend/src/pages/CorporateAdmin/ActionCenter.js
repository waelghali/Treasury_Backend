// src/pages/CorporateAdmin/ActionCenter.js
import React, { useState, useEffect } from 'react';
import { apiRequest } from 'services/apiService.js';
import { Loader2, AlertCircle, RefreshCcw, FileText, Hourglass, BarChart } from 'lucide-react';
import moment from 'moment';

const ActionCenter = ({ onLogout }) => {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [lgRenewalList, setLgRenewalList] = useState([]);
    const [undeliveredInstructions, setUndeliveredInstructions] = useState([]);
    const [awaitingReplyInstructions, setAwaitingReplyInstructions] = useState([]);
    const [pendingPrintRequests, setPendingPrintRequests] = useState([]);

    const fetchData = async () => {
        setLoading(true);
        setError(null);
        try {
            const [
                renewalResponse,
                undeliveredResponse,
                awaitingReplyResponse,
                pendingPrintResponse
            ] = await Promise.all([
                apiRequest('/corporate-admin/action-center/lg-for-renewal', 'GET'),
                apiRequest('/corporate-admin/action-center/instructions/undelivered', 'GET'),
                apiRequest('/corporate-admin/action-center/instructions/awaiting-reply', 'GET'),
                apiRequest('/corporate-admin/action-center/requests/pending-print', 'GET')
            ]);

            setLgRenewalList(renewalResponse);
            setUndeliveredInstructions(undeliveredResponse);
            setAwaitingReplyInstructions(awaitingReplyResponse);
            setPendingPrintRequests(pendingPrintResponse);

        } catch (err) {
            console.error('Failed to fetch action center data:', err);
            setError(`Failed to load action items. ${err.message || 'An unexpected error occurred.'}`);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);

    const renderList = (items, type) => {
        if (!items || items.length === 0) {
            return <p className="text-gray-500">No pending items found.</p>;
        }

        return (
            <ul className="divide-y divide-gray-200">
                {items.map((item) => (
                    <li key={item.id} className="py-4">
                        <div className="flex justify-between items-center">
                            <div className="flex-1 min-w-0">
                                <p className="text-sm font-semibold text-gray-900 truncate">{item.lg_record?.lg_number || item.lg_number || 'N/A'}</p>
                                <p className="mt-1 text-xs text-gray-500">
                                    {type === 'renewal' && `Expires on: ${moment(item.expiry_date).format('YYYY-MM-DD')}`}
                                    {type === 'undelivered' && `Issued: ${moment(item.created_at).format('YYYY-MM-DD')}`}
                                    {type === 'awaiting-reply' && `Delivered: ${moment(item.delivery_date).format('YYYY-MM-DD')}`}
                                    {type === 'pending-print' && `Approved: ${moment(item.approved_at).format('YYYY-MM-DD')}`}
                                </p>
                            </div>
                            <div className="ml-4 flex-shrink-0">
                                <span className="inline-flex items-center rounded-full bg-blue-100 px-2.5 py-0.5 text-xs font-medium text-blue-800">
                                    {item.lg_record?.lg_type?.name || item.lg_type?.name || 'N/A'}
                                </span>
                            </div>
                        </div>
                    </li>
                ))}
            </ul>
        );
    };

    if (loading) {
        return (
            <div className="text-center py-8">
                <Loader2 className="animate-spin h-8 w-8 text-blue-600 mx-auto" />
                <p className="text-gray-600 mt-2">Loading your action items...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4 flex items-center" role="alert">
                <AlertCircle className="h-5 w-5 mr-2" />
                <span className="block sm:inline">{error}</span>
            </div>
        );
    }

    return (
        <div className="space-y-8">
            <div className="flex justify-end">
                <button
                    onClick={fetchData}
                    className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
                >
                    <RefreshCcw className="h-5 w-5 mr-2" />
                    Refresh
                </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-6">
                <div className="bg-white p-6 rounded-lg shadow-md">
                    <h3 className="text-xl font-semibold mb-4 text-gray-800 flex items-center">
                        <BarChart className="h-6 w-6 mr-3 text-blue-500" /> LGs for Renewal
                    </h3>
                    {renderList(lgRenewalList, 'renewal')}
                </div>
                <div className="bg-white p-6 rounded-lg shadow-md">
                    <h3 className="text-xl font-semibold mb-4 text-gray-800 flex items-center">
                        <FileText className="h-6 w-6 mr-3 text-blue-500" /> Undelivered Instructions
                    </h3>
                    {renderList(undeliveredInstructions, 'undelivered')}
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-6">
                <div className="bg-white p-6 rounded-lg shadow-md">
                    <h3 className="text-xl font-semibold mb-4 text-gray-800 flex items-center">
                        <Hourglass className="h-6 w-6 mr-3 text-blue-500" /> Instructions Awaiting Reply
                    </h3>
                    {renderList(awaitingReplyInstructions, 'awaiting-reply')}
                </div>
                <div className="bg-white p-6 rounded-lg shadow-md">
                    <h3 className="text-xl font-semibold mb-4 text-gray-800 flex items-center">
                        <FileText className="h-6 w-6 mr-3 text-blue-500" /> Pending Print Tasks
                    </h3>
                    {renderList(pendingPrintRequests, 'pending-print')}
                </div>
            </div>
        </div>
    );
};

export default ActionCenter;