// frontend/src/components/Modals/RecordDeliveryModal.js
import React, { useState } from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Save, Truck, AlertCircle, Upload } from 'lucide-react';
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

const RecordDeliveryModal = ({ instruction, onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const [selectedDeliveryDate, setSelectedDeliveryDate] = useState(new Date());
    const [deliveryFile, setDeliveryFile] = useState(null);

    const initialValues = {
        deliveryDate: new Date(),
    };

    const DeliverySchema = Yup.object().shape({
        deliveryDate: Yup.date()
            .required('Delivery date is required')
            .max(new Date(), 'Delivery date cannot be in the future'),
    });

    const handleSubmit = async (values, { setSubmitting, setErrors }) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            setSubmitting(false);
            return;
        }

        try {
            const formData = new FormData();
            formData.append('delivery_date', values.deliveryDate.toISOString().split('T')[0]);

            if (deliveryFile) {
                const documentMetadata = {
                    document_type: "DELIVERY_PROOF",
                    file_name: deliveryFile.name,
                    mime_type: deliveryFile.type,
                    lg_instruction_id: instruction.id,
                };
                formData.append('delivery_document_file', deliveryFile);
                formData.append('delivery_document_metadata', JSON.stringify(documentMetadata));
            }

            const response = await apiRequest(
                `/end-user/lg-records/instructions/${instruction.id}/record-delivery`,
                'POST',
                formData,
                'multipart/form-data'
            );

            toast.success(`Delivery recorded successfully for Instruction ${instruction.serial_number}!`);
            onSuccess();
        } catch (error) {
            console.error("Failed to record delivery:", error);
            toast.error(`Failed to record delivery: ${error.message || 'An unexpected error occurred.'}`);
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
                                            Record Delivery for Instruction: {instruction.serial_number}
                                        </DialogTitle>
                                        <div className="mt-2">
                                            <p className="text-gray-600 mb-4">
                                                Confirm when this instruction was delivered to the bank and attach any supporting evidence.
                                            </p>
                                            <Formik
                                                initialValues={initialValues}
                                                validationSchema={DeliverySchema}
                                                onSubmit={handleSubmit}
                                            >
                                                {({ isSubmitting, errors, touched, setFieldValue }) => (
                                                    <Form className={`space-y-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
                                                        <div className="bg-blue-50 border border-blue-200 text-blue-800 p-3 rounded-md text-sm">
                                                            <strong>LG:</strong> {instruction.lg_record?.lg_number || 'N/A'} | <strong>Type:</strong> {instruction.instruction_type} | <strong>Issued:</strong> {new Date(instruction.instruction_date).toLocaleDateString()}
                                                        </div>

                                                        <div>
                                                            <label htmlFor="deliveryDate" className="block text-sm font-medium text-gray-700">
                                                                Delivery Date
                                                            </label>
                                                            <DatePicker
                                                                selected={selectedDeliveryDate}
                                                                onChange={(date) => {
                                                                    setSelectedDeliveryDate(date);
                                                                    setFieldValue('deliveryDate', date);
                                                                }}
                                                                dateFormat="yyyy-MM-dd"
                                                                className={`mt-1 block w-full ${errors.deliveryDate && touched.deliveryDate ? 'border-red-500' : 'border-gray-300'}`}
                                                                maxDate={new Date()}
                                                                popperPlacement="auto"
                                                                disabled={isGracePeriod} // NEW: Disable DatePicker
                                                            />
                                                            <ErrorMessage name="deliveryDate" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>

                                                        <div>
                                                            <label htmlFor="deliveryFile" className="block text-sm font-medium text-gray-700">
                                                                Delivery Document (Optional)
                                                            </label>
                                                            <input
                                                                id="deliveryFile"
                                                                name="deliveryFile"
                                                                type="file"
                                                                accept="image/*,application/pdf"
                                                                onChange={(event) => setDeliveryFile(event.currentTarget.files[0])}
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
                                                                    {isSubmitting ? 'Processing...' : <Truck className="h-5 w-5 mr-2" />}
                                                                    {isSubmitting ? 'Processing...' : 'Record Delivery'}
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

export default RecordDeliveryModal;