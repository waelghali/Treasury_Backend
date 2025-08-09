// frontend/src/components/Modals/ChangeLGOwnerModal.js
import React, { useState, useEffect } from 'react';
import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from '@headlessui/react';
import { X, Save, UserPlus, Users, Loader2, AlertCircle } from 'lucide-react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import * as Yup from 'yup';
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

const ChangeLGOwnerModal = ({ lgRecord, onClose, onSuccess, isGracePeriod }) => { // NEW: Accept isGracePeriod prop
    const [existingOwners, setExistingOwners] = useState([]);
    const [isLoadingOwners, setIsLoadingOwners] = useState(true);
    const [isCreatingNewOwner, setIsCreatingNewOwner] = useState(false);

    const initialValues = {
        newOwnerId: '',
        newOwnerEmail: '',
        newOwnerPhoneNumber: '',
        newOwnerInternalId: '',
        newOwnerManagerEmail: '',
        reason: '',
    };

    const ChangeOwnerSchema = Yup.object().shape({
        newOwnerId: Yup.number().when('isCreatingNewOwner', {
            is: false,
            then: (schema) => schema.required('Please select an existing owner'),
            otherwise: (schema) => schema.nullable(),
        }),
        newOwnerEmail: Yup.string().when('isCreatingNewOwner', {
            is: true,
            then: (schema) => schema.email('Invalid email').required('Email is required for new owner'),
            otherwise: (schema) => schema.nullable(),
        }),
        newOwnerPhoneNumber: Yup.string().when('isCreatingNewOwner', {
            is: true,
            then: (schema) => schema.matches(/^[0-9\-()\s+]+$/, 'Invalid phone number format').required('Phone is required for new owner'),
            otherwise: (schema) => schema.nullable(),
        }),
        newOwnerInternalId: Yup.string().nullable(),
        newOwnerManagerEmail: Yup.string().when('isCreatingNewOwner', {
            is: true,
            then: (schema) => schema.email('Invalid manager email').required('Manager email is required for new owner'),
            otherwise: (schema) => schema.nullable(),
        }),
        reason: Yup.string().required('Reason for owner change is required'),
    });

    useEffect(() => {
        const fetchExistingOwners = async () => {
            setIsLoadingOwners(true);
            try {
                const response = await apiRequest('/end-user/internal-owner-contacts/', 'GET');
                setExistingOwners(response);
            } catch (err) {
                console.error("Failed to fetch existing internal owners:", err);
                toast.error("Failed to load existing owners for selection.");
            } finally {
                setIsLoadingOwners(false);
            }
        };
        fetchExistingOwners();
    }, []);

    const handleSubmit = async (values, { setSubmitting, setErrors }) => {
        if (isGracePeriod) { // NEW: Grace period check
            toast.warn("This action is disabled during your subscription's grace period.");
            setSubmitting(false);
            return;
        }

        try {
            let newOwnerPayload = null;
            let newInternalOwnerContactId = values.newOwnerId || null;

            if (isCreatingNewOwner) {
                newOwnerPayload = {
                    email: values.newOwnerEmail,
                    phone_number: values.newOwnerPhoneNumber,
                    internal_id: values.newOwnerInternalId || null,
                    manager_email: values.newOwnerManagerEmail,
                };
                newInternalOwnerContactId = null;
            }

            const payload = {
                change_scope: "single_lg",
                lg_record_id: lgRecord.id,
                new_internal_owner_contact_id: newInternalOwnerContactId,
                new_internal_owner_contact_details: newOwnerPayload,
                reason: values.reason,
            };

            const response = await apiRequest('/end-user/lg-records/change-owner', 'POST', payload);

            if (response.approval_request_id) {
                toast.info(`LG Owner change request submitted for approval. Request ID: ${response.approval_request_id}.`);
            } else {
                toast.success(`LG Owner for ${lgRecord.lg_number} changed successfully!`);
            }
            onSuccess();
        } catch (error) {
            console.error("Failed to change LG owner:", error);
            toast.error(`Failed to change LG owner: ${error.message || 'An unexpected error occurred.'}`);
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
                            <DialogPanel className="relative transform overflow-hidden rounded-lg bg-white px-4 pb-4 pt-5 text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-xl sm:p-6">
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
                                        <DialogTitle as="h3" className="text-md font-semibold leading-6 text-gray-900 border-b pb-3 mb-4">
                                            Change Internal Owner for LG: {lgRecord.lg_number}
                                        </DialogTitle>
                                        <div className="mt-2">
                                            <Formik
                                                initialValues={initialValues}
                                                validationSchema={ChangeOwnerSchema}
                                                onSubmit={handleSubmit}
                                            >
                                                {({ isSubmitting, errors, touched, values, setFieldValue }) => (
                                                    <Form className={`space-y-4 ${isGracePeriod ? 'opacity-50' : ''}`}>
                                                        <div className="p-4 border border-blue-200 rounded-md bg-blue-50">
                                                            <h4 className="font-small text-md text-blue-800 mb-2">Current Owner Details:</h4>
                                                            <p><strong>Email:</strong> {lgRecord.internal_owner_contact?.email || 'N/A'}</p>
                                                            <p><strong>Phone:</strong> {lgRecord.internal_owner_contact?.phone_number || 'N/A'}</p>
                                                            <p><strong>Manager:</strong> {lgRecord.internal_owner_contact?.manager_email || 'N/A'}</p>
                                                        </div>

                                                        <div className="flex items-center space-x-2 mt-4 mb-4">
                                                            <input
                                                                id="isCreatingNewOwner"
                                                                name="isCreatingNewOwner"
                                                                type="checkbox"
                                                                checked={isCreatingNewOwner}
                                                                onChange={() => {
                                                                    setIsCreatingNewOwner(!isCreatingNewOwner);
                                                                    setFieldValue('newOwnerId', '');
                                                                    setFieldValue('newOwnerEmail', '');
                                                                    setFieldValue('newOwnerPhoneNumber', '');
                                                                    setFieldValue('newOwnerInternalId', '');
                                                                    setFieldValue('newOwnerManagerEmail', '');
                                                                }}
                                                                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                                                                disabled={isGracePeriod} // NEW: Disable checkbox
                                                            />
                                                            <label htmlFor="isCreatingNewOwner" className="text-sm font-medium text-gray-700">
                                                                Create a New Internal Owner Contact
                                                            </label>
                                                        </div>

                                                        {isCreatingNewOwner ? (
                                                            <div className="space-y-3 border p-3 rounded-md shadow-sm bg-gray-50">
                                                                <h4 className="font-medium text-md text-gray-700 mb-2">New Owner Contact Details:</h4>
                                                                <div>
                                                                    <label htmlFor="newOwnerEmail" className="block text-sm font-medium text-gray-700">
                                                                        Email
                                                                    </label>
                                                                    <Field
                                                                        type="email"
                                                                        id="newOwnerEmail"
                                                                        name="newOwnerEmail"
                                                                        className={`mt-1 block w-full ${errors.newOwnerEmail && touched.newOwnerEmail ? 'border-red-500' : 'border-gray-300'}`}
                                                                        disabled={isGracePeriod} // NEW: Disable input
                                                                    />
                                                                    <ErrorMessage name="newOwnerEmail" component="div" className="text-red-600 text-xs mt-1" />
                                                                </div>
                                                                <div>
                                                                    <label htmlFor="newOwnerPhoneNumber" className="block text-sm font-medium text-gray-700">
                                                                        Phone Number
                                                                    </label>
                                                                    <Field
                                                                        type="text"
                                                                        id="newOwnerPhoneNumber"
                                                                        name="newOwnerPhoneNumber"
                                                                        className={`mt-1 block w-full ${errors.newOwnerPhoneNumber && touched.newOwnerPhoneNumber ? 'border-red-500' : 'border-gray-300'}`}
                                                                        disabled={isGracePeriod} // NEW: Disable input
                                                                    />
                                                                    <ErrorMessage name="newOwnerPhoneNumber" component="div" className="text-red-600 text-xs mt-1" />
                                                                </div>
                                                                <div>
                                                                    <label htmlFor="newOwnerInternalId" className="block text-sm font-medium text-gray-700">
                                                                        Internal ID (Optional)
                                                                    </label>
                                                                    <Field
                                                                        type="text"
                                                                        id="newOwnerInternalId"
                                                                        name="newOwnerInternalId"
                                                                        className="mt-1 block w-full border-gray-300"
                                                                        disabled={isGracePeriod} // NEW: Disable input
                                                                    />
                                                                </div>
                                                                <div>
                                                                    <label htmlFor="newOwnerManagerEmail" className="block text-sm font-medium text-gray-700">
                                                                        Manager Email
                                                                    </label>
                                                                    <Field
                                                                        type="email"
                                                                        id="newOwnerManagerEmail"
                                                                        name="newOwnerManagerEmail"
                                                                        className={`mt-1 block w-full ${errors.newOwnerManagerEmail && touched.newOwnerManagerEmail ? 'border-red-500' : 'border-gray-300'}`}
                                                                        disabled={isGracePeriod} // NEW: Disable input
                                                                    />
                                                                    <ErrorMessage name="newOwnerManagerEmail" component="div" className="text-red-600 text-xs mt-1" />
                                                                </div>
                                                            </div>
                                                        ) : (
                                                            <div>
                                                                <label htmlFor="newOwnerId" className="block text-sm font-medium text-gray-700">
                                                                    Select Existing Owner
                                                                </label>
                                                                {isLoadingOwners ? (
                                                                    <p className="text-gray-500 flex items-center mt-2"><Loader2 className="animate-spin h-4 w-4 mr-2" /> Loading owners...</p>
                                                                ) : (
                                                                    <Field
                                                                        as="select"
                                                                        id="newOwnerId"
                                                                        name="newOwnerId"
                                                                        className={`mt-1 block w-full ${errors.newOwnerId && touched.newOwnerId ? 'border-red-500' : 'border-gray-300'}`}
                                                                        disabled={isGracePeriod} // NEW: Disable select input
                                                                    >
                                                                        <option value="">-- Select an owner --</option>
                                                                        {existingOwners.map(ownerOption => (
                                                                            <option key={ownerOption.id} value={ownerOption.id}>
                                                                                {ownerOption.email} ({ownerOption.phone_number})
                                                                            </option>
                                                                        ))}
                                                                    </Field>
                                                                )}
                                                                <ErrorMessage name="newOwnerId" component="div" className="text-red-600 text-xs mt-1" />
                                                            </div>
                                                        )}

                                                        <div>
                                                            <label htmlFor="reason" className="block text-sm font-medium text-gray-700">
                                                                Reason for Change
                                                            </label>
                                                            <Field
                                                                as="textarea"
                                                                id="reason"
                                                                name="reason"
                                                                rows="2"
                                                                className={`mt-1 block w-full ${errors.reason && touched.reason ? 'border-red-500' : 'border-gray-300'}`}
                                                                disabled={isGracePeriod} // NEW: Disable textarea
                                                            />
                                                            <ErrorMessage name="reason" component="div" className="text-red-600 text-xs mt-1" />
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
                                                                    {isSubmitting ? <Loader2 className="h-5 w-5 mr-2 animate-spin" /> : <Users className="h-5 w-5 mr-2" />}
                                                                    {isSubmitting ? 'Processing...' : 'Submit Change Request'}
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

export default ChangeLGOwnerModal;