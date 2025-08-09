import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { toast } from 'react-toastify';
import { 
  getSystemNotificationById, 
  createSystemNotification, 
  updateSystemNotification, 
  apiRequest,
  getAllUsersForSystemOwner,
} from '../../../services/apiService';
import { ArrowLeft, Loader2 } from 'lucide-react';
import { format, parseISO } from 'date-fns';
import clsx from 'clsx'; // For conditional class names

// Reusable styling classes, matching your sample files
const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200";
const labelClassNames = "block text-sm font-medium text-gray-700";

// Inlined UI Components
const Button = ({ children, className, variant = 'default', size = 'default', ...props }) => {
  const baseClass = "inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-blue-500 disabled:pointer-events-none disabled:opacity-50";
  const sizeClass = size === 'sm' ? "h-8 px-3 text-xs" : size === 'lg' ? "h-10 px-8" : "h-9 px-4 py-2";
  
  const variantClass = clsx({
    "bg-blue-600 text-white shadow hover:bg-blue-700": variant === 'default',
    "bg-red-600 text-white shadow-sm hover:bg-red-700": variant === 'destructive',
    "border border-gray-300 bg-white shadow-sm hover:bg-gray-100": variant === 'outline',
    "bg-gray-200 text-gray-900 shadow-sm hover:bg-gray-300": variant === 'secondary',
    "hover:bg-gray-100": variant === 'ghost',
    "text-blue-600 underline-offset-4 hover:underline": variant === 'link',
  });

  return (
    <button className={clsx(baseClass, sizeClass, variantClass, className)} {...props}>
      {children}
    </button>
  );
};

const Card = ({ children, className, ...props }) => (
  <div className={clsx("bg-white p-6 rounded-lg shadow-md", className)} {...props}>
    {children}
  </div>
);

const CardHeader = ({ children, ...props }) => (
  <div className="border-b border-gray-200 pb-4 mb-4" {...props}>
    {children}
  </div>
);

const CardTitle = ({ children, ...props }) => (
  <h3 className="text-lg font-medium text-gray-800" {...props}>
    {children}
  </h3>
);

const CardContent = ({ children, ...props }) => (
  <div className="space-y-4" {...props}>
    {children}
  </div>
);

const Input = ({ className, ...props }) => (
  <input className={clsx(inputClassNames, className)} {...props} />
);

const Label = ({ htmlFor, children, className, ...props }) => (
  <label htmlFor={htmlFor} className={clsx(labelClassNames, className)} {...props}>
    {children}
  </label>
);

const ToggleSwitch = ({ id, name, checked, onChange, label, className }) => (
  <label htmlFor={id} className={clsx("relative inline-flex items-center cursor-pointer mb-2", className)}>
    <input
      type="checkbox"
      name={name}
      id={id}
      checked={checked}
      onChange={onChange}
      className="sr-only peer"
    />
    <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
    <span className="ml-3 text-sm font-medium text-gray-900">{label}</span>
  </label>
);

const Textarea = ({ className, ...props }) => (
  <textarea className={clsx(inputClassNames, className)} {...props} />
);

const Select = ({ children, className, ...props }) => (
  <select className={clsx(inputClassNames, className)} {...props}>
    {children}
  </select>
);

function SystemNotificationForm() {
    const navigate = useNavigate();
    const { id } = useParams();
    const isEditMode = Boolean(id);

    const [formData, setFormData] = useState({
        content: '',
        link: '',
        start_date: '',
        end_date: '',
        is_active: true,
        animation_type: 'fade', // NEW
        display_frequency: 'once-per-login', // NEW
        max_display_count: 3, // NEW
        target_customer_ids: [],
        target_user_ids: [], // NEW
        target_roles: [], // NEW
    });
    
    const [customers, setCustomers] = useState([]);
    const [users, setUsers] = useState([]); // NEW
    const [isLoadingCustomers, setIsLoadingCustomers] = useState(false);
    const [isLoadingUsers, setIsLoadingUsers] = useState(false); // NEW
    const [isLoading, setIsLoading] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);

    // Hardcoded roles for now, or fetch from a future API endpoint
    const availableRoles = [
      { id: 'SYSTEM_OWNER', name: 'System Owner' },
      { id: 'CORPORATE_ADMIN', name: 'Corporate Admin' },
      { id: 'END_USER', name: 'End User' },
    ];
    
    // Fetch dependencies for selects
    useEffect(() => {
        const fetchCustomers = async () => {
            setIsLoadingCustomers(true);
            try {
                const data = await apiRequest('/system-owner/customers/');
                setCustomers(data);
            } catch (err) {
                console.error("Failed to fetch customers:", err);
                toast.error("Failed to load customer list.");
            } finally {
                setIsLoadingCustomers(false);
            }
        };

        const fetchUsers = async () => { // NEW
          setIsLoadingUsers(true);
          try {
              // This is a placeholder call, assuming an endpoint exists.
              // We'll update apiService.js to mock/implement this.
              const data = await getAllUsersForSystemOwner();
              setUsers(data);
          } catch (err) {
              console.error("Failed to fetch users:", err);
              toast.error("Failed to load user list.");
          } finally {
              setIsLoadingUsers(false);
          }
        };

        fetchCustomers();
        fetchUsers();
    }, []);

    useEffect(() => {
        if (isEditMode) {
            fetchNotification();
        }
    }, [isEditMode]);

    const fetchNotification = async () => {
        setIsLoading(true);
        try {
            const data = await getSystemNotificationById(id);
            setFormData({
                ...data,
                start_date: format(parseISO(data.start_date), "yyyy-MM-dd'T'HH:mm"),
                end_date: format(parseISO(data.end_date), "yyyy-MM-dd'T'HH:mm"),
                target_customer_ids: data.target_customer_ids || [],
                target_user_ids: data.target_user_ids || [], // NEW
                target_roles: data.target_roles || [], // NEW
            });
        } catch (err) {
            toast.error('Failed to fetch notification details.');
            navigate('/system-owner/system-notifications');
        } finally {
            setIsLoading(false);
        }
    };

    const handleChange = (e) => {
        const { id, value, type, checked } = e.target;
        setFormData((prev) => ({ 
            ...prev, 
            [id]: type === 'checkbox' ? checked : value 
        }));
    };

    const handleMultiSelectChange = (e, fieldName) => {
        const selectedValues = Array.from(e.target.options)
            .filter(option => option.selected)
            .map(option => option.value);

        // Check if the "all" option is selected
        const isAllSelected = selectedValues.includes("");

        if (isAllSelected) {
            // If "All" is selected, the state should be an empty array
            setFormData(prev => ({ ...prev, [fieldName]: [] }));
        } else {
            // Otherwise, set the state to the selected values, converting to number
            const value = selectedValues.map(v => 
                (fieldName === 'target_user_ids' || fieldName === 'target_customer_ids') ? Number(v) : v
            );
            setFormData(prev => ({ ...prev, [fieldName]: value }));
        }
    };
    
    const handleSubmit = async (e) => {
        e.preventDefault();
        setIsSubmitting(true);
        
        const payload = {
            ...formData,
            link: formData.link || null,
            max_display_count: formData.display_frequency === 'repeat-x-times' ? formData.max_display_count : null, // NEW logic
        };
        
        try {
            if (isEditMode) {
                await updateSystemNotification(id, payload);
                toast.success('Notification updated successfully.');
            } else {
                await createSystemNotification(payload);
                toast.success('Notification created successfully.');
            }
            navigate('/system-owner/system-notifications');
        } catch (err) {
            toast.error(`Failed to ${isEditMode ? 'update' : 'create'} notification.`);
            console.error(err);
        } finally {
            setIsSubmitting(false);
        }
    };

    if (isLoading || isLoadingCustomers || isLoadingUsers) {
        return (
            <div className="flex justify-center items-center h-64">
                <Loader2 className="h-8 w-8 animate-spin" />
            </div>
        );
    }
    
    // Determine the value for the multi-selects based on the state.
    // If the state array is empty, we must pass `[""]` to select the "All" option.
    const getMultiSelectValue = (fieldName) => {
      const fieldData = formData[fieldName];
      return (fieldData.length === 0 && fieldData !== null) ? [""] : fieldData.map(String);
    };

    return (
        <div>
            <h2 className="text-2xl font-semibold text-gray-800 mb-6">
                {isEditMode ? 'Edit Notification' : 'Create New Notification'}
            </h2>
            <Card>
                <CardHeader>
                    <CardTitle>Notification Details</CardTitle>
                </CardHeader>
                <CardContent>
                    <form onSubmit={handleSubmit} className="space-y-6">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="mb-2">
                                <Label htmlFor="content">Content <span className="text-red-500">*</span></Label>
                                <Textarea
                                    id="content"
                                    placeholder="Enter the notification message"
                                    value={formData.content}
                                    onChange={handleChange}
                                    required
                                />
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="link">Link (Optional)</Label>
                                <Input
                                    id="link"
                                    type="url"
                                    placeholder="https://example.com/link"
                                    value={formData.link}
                                    onChange={handleChange}
                                />
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="start_date">Start Date <span className="text-red-500">*</span></Label>
                                <Input
                                    id="start_date"
                                    type="datetime-local"
                                    value={formData.start_date}
                                    onChange={handleChange}
                                    required
                                />
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="end_date">End Date <span className="text-red-500">*</span></Label>
                                <Input
                                    id="end_date"
                                    type="datetime-local"
                                    value={formData.end_date}
                                    onChange={handleChange}
                                    required
                                />
                            </div>
                            <div className="flex items-center space-x-2 mt-2">
                                <ToggleSwitch
                                    id="is_active"
                                    name="is_active"
                                    checked={formData.is_active}
                                    onChange={handleChange}
                                    label="Active"
                                />
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="animation_type">Animation Type</Label>
                                <Select id="animation_type" value={formData.animation_type} onChange={handleChange}>
                                    <option value="fade">Fade</option>
                                    <option value="slide-left">Slide Left</option>
                                    <option value="scroll-left">Scroll Left</option>
                                </Select>
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="display_frequency">Display Frequency</Label>
                                <Select id="display_frequency" value={formData.display_frequency} onChange={handleChange}>
                                    <option value="once">Once</option>
                                    <option value="once-per-login">Once Per Login</option>
                                    <option value="repeat-x-times">Repeat Up to X Times</option>
                                </Select>
                            </div>
                            {formData.display_frequency === 'repeat-x-times' && (
                                <div className="mb-2">
                                    <Label htmlFor="max_display_count">Max Display Count</Label>
                                    <Input
                                        id="max_display_count"
                                        type="number"
                                        min="1"
                                        value={formData.max_display_count}
                                        onChange={handleChange}
                                    />
                                </div>
                            )}
                            <div className="mb-2">
                                <Label htmlFor="target_customer_ids">Target Customer(s) (Optional)</Label>
                                <select
                                    id="target_customer_ids"
                                    multiple
                                    value={getMultiSelectValue('target_customer_ids')}
                                    onChange={(e) => handleMultiSelectChange(e, 'target_customer_ids')}
                                    className={clsx(inputClassNames, "h-32")}
                                    disabled={isLoadingCustomers}
                                >
                                    <option value="">All Customers</option>
                                    {isLoadingCustomers ? (
                                        <option disabled>Loading...</option>
                                    ) : (
                                        customers.map((customer) => (
                                            <option key={customer.id} value={customer.id}>
                                                {customer.name}
                                            </option>
                                        ))
                                    )}
                                </select>
                            </div>
                             <div className="mb-2">
                                <Label htmlFor="target_user_ids">Target User(s) (Optional)</Label>
                                <select
                                    id="target_user_ids"
                                    multiple
                                    value={getMultiSelectValue('target_user_ids')}
                                    onChange={(e) => handleMultiSelectChange(e, 'target_user_ids')}
                                    className={clsx(inputClassNames, "h-32")}
                                    disabled={isLoadingUsers}
                                >
                                    <option value="">All Users</option>
                                    {isLoadingUsers ? (
                                        <option disabled>Loading...</option>
                                    ) : (
                                        users.map((user) => (
                                            <option key={user.id} value={user.id}>
                                                {user.email}
                                            </option>
                                        ))
                                    )}
                                </select>
                            </div>
                            <div className="mb-2">
                                <Label htmlFor="target_roles">Target Role(s) (Optional)</Label>
                                <select
                                    id="target_roles"
                                    multiple
                                    value={getMultiSelectValue('target_roles')}
                                    onChange={(e) => handleMultiSelectChange(e, 'target_roles')}
                                    className={clsx(inputClassNames, "h-32")}
                                >
                                    <option value="">All Roles</option>
                                    {availableRoles.map((role) => (
                                        <option key={role.id} value={role.id}>
                                            {role.name}
                                        </option>
                                    ))}
                                </select>
                            </div>
                        </div>
                        
                        <div className="flex justify-end space-x-3 mt-6">
                            <Button 
                              type="button"
                              onClick={() => navigate('/system-owner/system-notifications')}
                              variant="outline"
                            >
                                Cancel
                            </Button>
                            <Button type="submit" disabled={isSubmitting}>
                                {isSubmitting && <Loader2 className="animate-spin mr-2 h-4 w-4" />}
                                {isEditMode ? 'Update Notification' : 'Create Notification'}
                            </Button>
                        </div>
                    </form>
                </CardContent>
            </Card>
        </div>
    );
}

export default SystemNotificationForm;