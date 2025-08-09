// frontend/src/components/Modals/InternalOwnerFormModal.js
import React from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Save, UserPlus, Edit3 } from 'lucide-react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import * as Yup from 'yup';
import { apiRequest } from '../../services/apiService';
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

const buttonBaseClassNames = "inline-flex items-center px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200";

const InternalOwnerFormModal = ({ owner, onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const isEditMode = !!owner;
    const modalTitle = isEditMode ? 'Edit Internal Owner Details' : 'Add New Internal Owner';
    const submitButtonText = isEditMode ? 'Save Changes' : 'Add Owner';
    const submitButtonIcon = isEditMode ? <Edit3 className="h-5 w-5 mr-2" /> : <UserPlus className="h-5 w-5 mr-2" />;

    const InternalOwnerSchema = Yup.object().shape({
        email: Yup.string()
            .email('Invalid email address')
            .required('Email is required'),
        phoneNumber: Yup.string()
            .matches(/^[0-9\-\(\)\s\+]+$/, 'Invalid phone number format')
            .required('Phone Number is required'),
        internalId: Yup.string().nullable(),
        managerEmail: Yup.string()
            .email('Invalid manager email address')
            .required('Manager Email is required'),
    });

    const initialValues = {
        email: owner?.email || '',
        phoneNumber: owner?.phone_number || '',
        internalId: owner?.internal_id || '',
        managerEmail: owner?.manager_email || '',
    };

    const handleSubmit = async (values, { setSubmitting, setErrors }) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            setSubmitting(false);
            return;
        }

        try {
            const payload = {
                email: values.email,
                phone_number: values.phoneNumber,
                internal_id: values.internalId || null,
                manager_email: values.managerEmail,
            };

            let response;
            if (isEditMode) {
                response = await apiRequest(`/end-user/internal-owner-contacts/${owner.id}`, 'PUT', payload);
            } else {
                toast.error("Adding new internal owners directly is not currently supported via this form. Owners are typically added when changing an LG's owner.");
                setSubmitting(false);
                return;
            }

            if (response.approval_request_id) {
                toast.info(`Internal Owner Change Request submitted for approval. Request ID: ${response.approval_request_id}`);
            } else {
                toast.success(`Internal Owner Contact ${isEditMode ? 'updated' : 'added'} successfully!`);
            }
            onSuccess();
        } catch (error) {
            console.error('Operation failed:', error);
            toast.error(`Failed to ${isEditMode ? 'update' : 'add'} internal owner: ${error.message || 'An unexpected error occurred.'}`);
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
                            leaveFrom="opacity-100 translate-y-0 sm:scale-100"
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
                                            {modalTitle}
                                        </DialogTitle>
                                        <div className="mt-2">
                                            <Formik
                                                initialValues={initialValues}
                                                validationSchema={InternalOwnerSchema}
                                                onSubmit={handleSubmit}
                                            >
                                                {({ isSubmitting, errors, touched }) => (
                                                    <Form className={`space-y-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
                                                        <div>
                                                            <label htmlFor="email" className="block text-sm font-medium text-gray-700">
                                                                Email
                                                            </label>
                                                            <Field
                                                                type="email"
                                                                id="email"
                                                                name="email"
                                                                className={`mt-1 block w-full ${errors.email && touched.email ? 'border-red-500' : 'border-gray-300'}`}
                                                                disabled={isEditMode || isGracePeriod} // NEW: Disable input
                                                            />
                                                            <ErrorMessage name="email" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>
                                                        <div>
                                                            <label htmlFor="phoneNumber" className="block text-sm font-medium text-gray-700">
                                                                Phone Number
                                                            </label>
                                                            <Field
                                                                type="text"
                                                                id="phoneNumber"
                                                                name="phoneNumber"
                                                                className={`mt-1 block w-full ${errors.phoneNumber && touched.phoneNumber ? 'border-red-500' : 'border-gray-300'}`}
                                                                disabled={isGracePeriod} // NEW: Disable input
                                                            />
                                                            <ErrorMessage name="phoneNumber" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>
                                                        <div>
                                                            <label htmlFor="internalId" className="block text-sm font-medium text-gray-700">
                                                                Internal ID (Optional)
                                                            </label>
                                                            <Field
                                                                type="text"
                                                                id="internalId"
                                                                name="internalId"
                                                                className="mt-1 block w-full border-gray-300"
                                                                disabled={isGracePeriod} // NEW: Disable input
                                                            />
                                                        </div>
                                                        <div>
                                                            <label htmlFor="managerEmail" className="block text-sm font-medium text-gray-700">
                                                                Manager Email
                                                            </label>
                                                            <Field
                                                                type="email"
                                                                id="managerEmail"
                                                                name="managerEmail"
                                                                className={`mt-1 block w-full ${errors.managerEmail && touched.managerEmail ? 'border-red-500' : 'border-gray-300'}`}
                                                                disabled={isGracePeriod} // NEW: Disable input
                                                            />
                                                            <ErrorMessage name="managerEmail" component="div" className="text-red-600 text-xs mt-1" />
                                                        </div>

                                                        {errors.general && (
                                                            <div className="text-red-600 text-sm mt-2">
                                                                {errors.general}
                                                            </div>
                                                        )}

                                                        <div className="mt-5 sm:mt-6 sm:grid sm:grid-flow-row-dense sm:grid-cols-2 sm:gap-3">
                                                            <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                                                                <button
                                                                    type="submit"
                                                                    className={`${buttonBaseClassNames} sm:col-start-2 bg-blue-600 text-white hover:bg-blue-700 ${isSubmitting || isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                                    disabled={isSubmitting || isGracePeriod} // NEW: Disable submit button
                                                                >
                                                                    {isSubmitting ? 'Processing...' : submitButtonIcon}
                                                                    {isSubmitting ? 'Processing...' : submitButtonText}
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

export default InternalOwnerFormModal;