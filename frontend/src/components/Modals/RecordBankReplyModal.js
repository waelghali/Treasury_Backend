// frontend/src/components/Modals/RecordBankReplyModal.js
import React, { useState } from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Save, Building, AlertCircle } from 'lucide-react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import * as Yup from 'yup';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
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
                        <polygon className="fill-current" points="0,0 127.5,127.5 255,0"/>
                    </svg>
                </div>
            </div>
        );
    }
    return children;
};

const buttonBaseClassNames = "inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

const RecordBankReplyModal = ({ instruction, onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const [selectedReplyDate, setSelectedReplyDate] = useState(new Date());
    const [replyFile, setReplyFile] = useState(null);

    const initialValues = {
        bankReplyDate: new Date(),
        replyDetails: '',
    };

    const BankReplySchema = Yup.object().shape({
        bankReplyDate: Yup.date()
            .required('Bank reply date is required')
            .max(new Date(), 'Bank reply date cannot be in the future'),
        replyDetails: Yup.string().nullable().max(500, 'Reply details cannot exceed 500 characters'),
    });

    const handleSubmit = async (values, { setSubmitting, setErrors }) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            setSubmitting(false);
            return;
        }

        try {
            const formData = new FormData();
            formData.append('bank_reply_date', values.bankReplyDate.toISOString().split('T')[0]);
            formData.append('reply_details', values.replyDetails || '');
            if (replyFile) {
                const documentMetadata = {
                    document_type: "BANK_REPLY",
                    file_name: replyFile.name,
                    mime_type: replyFile.type,
                    lg_instruction_id: instruction.id,
                };
                formData.append('bank_reply_document_file', replyFile);
                formData.append('bank_reply_document_metadata', JSON.stringify(documentMetadata));
            }

            const response = await apiRequest(
                `/end-user/lg-records/instructions/${instruction.id}/record-bank-reply`,
                'POST',
                formData,
                'multipart/form-data'
            );

            toast.success(`Bank reply recorded successfully for Instruction ${instruction.serial_number}!`);
            onSuccess();
        } catch (error) {
            console.error("Failed to record bank reply:", error);
            toast.error(`Failed to record bank reply: ${error.message || 'An unexpected error occurred.'}`);
            setErrors({ general: error.message || 'An unexpected error occurred.' });
        } finally {
            setSubmitting(false);
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
                                            Record Bank Reply for Instruction: {instruction.serial_number}
                                        </DialogTitle>
                                        <div className="mt-2">
                                            <p className="text-gray-600 mb-4">
                                                Record the bank's response to this instruction.
                                            </p>
                                            <Formik
                                                initialValues={initialValues}
                                                validationSchema={BankReplySchema}
                                                onSubmit={handleSubmit}
                                            >
                                                {({ isSubmitting, errors, touched, setFieldValue }) => (
                                                    <Form className={`space-y-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
                                                        <div className="bg-blue-50 border border-blue-200 text-blue-800 p-3 rounded-md text-sm">
                                                            <strong>LG:</strong> {instruction.lg_record?.lg_number || 'N/A'} | <strong>Type:</strong> {instruction.instruction_type} | <strong>Issued:</strong> {new Date(instruction.instruction_date).toLocaleDateString()}
                                                            {instruction.delivery_date && <span> | <strong>Delivered:</strong> {new Date(instruction.delivery_date).toLocaleDateString()}</span>}
                                                        </div>

                                                        <div>
                                                            <label htmlFor="bankReplyDate" className="block text-sm font-medium text-gray-700">
                                                                Bank Reply Date
                                                            </label>
                                                            <DatePicker
                                                                selected={selectedReplyDate}
                                                                onChange={(date) => {
                                                                    setSelectedReplyDate(date);
                                                                    setFieldValue('bankReplyDate', date);
                                                                }}
                                                                dateFormat="yyyy-MM-dd"
                                                                className={`mt-1 block w-full ${errors.bankReplyDate && touched.bankReplyDate ? 'border-red-500' : 'border-gray-300'}`}
                                                                maxDate={new Date()}
                                                                popperPlacement="auto"
                                                                disabled={isGracePeriod} // NEW: Disable DatePicker
                                                            />
                                                            <ErrorMessage name="bankReplyDate" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>

                                                        <div>
                                                            <label htmlFor="replyDetails" className="block text-sm font-medium text-gray-700">
                                                                Reply Details (Optional)
                                                            </label>
                                                            <Field
                                                                as="textarea"
                                                                id="replyDetails"
                                                                name="replyDetails"
                                                                rows="3"
                                                                className="mt-1 block w-full border-gray-300"
                                                                disabled={isGracePeriod} // NEW: Disable textarea
                                                            />
                                                            <ErrorMessage name="replyDetails" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>

                                                        <div>
                                                            <label htmlFor="replyFile" className="block text-sm font-medium text-gray-700">
                                                                Bank Reply Document (Optional)
                                                            </label>
                                                            <input
                                                                id="replyFile"
                                                                name="replyFile"
                                                                type="file"
                                                                accept="image/*,application/pdf"
                                                                onChange={(event) => setReplyFile(event.currentTarget.files[0])}
                                                                className="mt-1 block w-full text-sm text-gray-500
                                                                           file:mr-4 file:py-2 file:px-4
                                                                           file:rounded-md file:border-0
                                                                           file:text-sm file:font-semibold
                                                                           file:bg-blue-50 file:text-blue-700
                                                                           hover:file:bg-blue-100"
                                                                disabled={isGracePeriod} // NEW: Disable file input
                                                            />
                                                            <p className="mt-1 text-xs text-gray-500">Supported formats: JPG, PNG, PDF. (Max 5MB)</p>
                                                        </div>

                                                        {errors.general && (
                                                            <div className="text-red-600 text-sm mt-2">
                                                                <AlertCircle className="inline h-4 w-4 mr-1" />
                                                                {errors.general}
                                                            </div>
                                                        )}

                                                        <div className="mt-5 sm:mt-6 sm:grid sm:grid-flow-row-dense sm:grid-cols-2 sm:gap-3">
                                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                                <button
                                                                    type="submit"
                                                                    className={`${buttonBaseClassNames} sm:col-start-2 bg-blue-600 text-white hover:bg-blue-700 ${isSubmitting || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                                    disabled={isSubmitting || isGracePeriod}
                                                                >
                                                                    {isSubmitting ? 'Processing...' : <Building className="h-5 w-5 mr-2" />}
                                                                    {isSubmitting ? 'Processing...' : 'Record Reply'}
                                                                </button>
                                                            </GracePeriodTooltip>
                                                            <button
                                                                type="button"
                                                                className={`${buttonBaseClassNames} sm:col-start-1 bg-gray-200 text-gray-700 hover:bg-gray-300`}
                                                                onClick={onClose}
                                                                disabled={isSubmitting}
                                                            >
                                                                Cancel
                                                            </button>
                                                        </div>
                                                    </Form>
                                                )}
                                            </Formik>
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

export default RecordBankReplyModal;