import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'react-toastify';
import { 
  getSystemNotifications, 
  deleteSystemNotification, 
  restoreSystemNotification 
} from '../../../services/apiService';
import { format } from 'date-fns';
import { PlusCircle, Edit, Trash, RotateCcw, Loader2 } from 'lucide-react';
import clsx from 'clsx';

// Inlined UI Components (Updated Button)
const Button = ({ children, className, variant = 'default', size = 'default', ...props }) => {
  const baseClass = "inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-blue-500 disabled:pointer-events-none disabled:opacity-50";
  const sizeClass = size === 'sm' ? "h-8 px-3 text-xs" : size === 'lg' ? "h-10 px-8" : "h-9 px-4 py-2";

  const variantClass = clsx({
    "bg-blue-600 text-white shadow hover:bg-blue-700": variant === 'default',
    "bg-red-600 text-white shadow-sm hover:bg-red-700": variant === 'destructive',
    "border border-gray-300 bg-white shadow-sm hover:bg-gray-100": variant === 'outline',
    "bg-gray-200 text-gray-900 shadow-sm hover:bg-gray-300": variant === 'secondary',
    // New icon variants for text color and hover background
    "p-1 rounded-md text-gray-600 hover:bg-gray-100": variant === 'icon-default',
    "p-1 rounded-md text-blue-600 hover:bg-blue-100": variant === 'icon-blue',
    "p-1 rounded-md text-red-600 hover:bg-red-100": variant === 'icon-red',
    "p-1 rounded-md text-green-600 hover:bg-green-100": variant === 'icon-green',
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

const Table = ({ children, className, ...props }) => (
  <div className="overflow-x-auto bg-white rounded-lg shadow-md">
    <table className={clsx("min-w-full divide-y divide-gray-200", className)} {...props}>
      {children}
    </table>
  </div>
);

const TableHeader = ({ children, className, ...props }) => (
  <thead className={clsx("bg-gray-50", className)} {...props}>
    {children}
  </thead>
);

const TableBody = ({ children, className, ...props }) => (
  <tbody className={clsx("bg-white divide-y divide-gray-200", className)} {...props}>
    {children}
  </tbody>
);

const TableRow = ({ children, className, isDeleted, ...props }) => (
  <tr 
    className={clsx(
      "cursor-pointer hover:bg-gray-50",
      isDeleted && "opacity-60",
      className
    )} 
    {...props}>
    {children}
  </tr>
);

const TableHead = ({ children, className, ...props }) => (
  <th scope="col" className={clsx("px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider", className)} {...props}>
    {children}
  </th>
);

const TableCell = ({ children, className, ...props }) => (
  <td className={clsx("px-6 py-4 whitespace-nowrap text-sm text-gray-500", className)} {...props}>
    {children}
  </td>
);

const Badge = ({ children, className, variant = 'default', ...props }) => {
  const variantClass = clsx({
    "bg-blue-600 text-white": variant === 'default',
    "bg-red-600 text-white": variant === 'destructive',
    "bg-gray-200 text-gray-900": variant === 'secondary',
    "border border-gray-300 text-gray-700": variant === 'outline',
  });
  return (
    <span className={clsx("ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full", variantClass, className)} {...props}>
      {children}
    </span>
  );
};

// No changes needed for AlertDialog logic here, it remains self-contained.
const AlertDialog = ({ title, description, onConfirm, onCancel, ...props }) => {
  return (
    <div className="fixed inset-0 z-50 overflow-y-auto bg-gray-600 bg-opacity-75 flex items-center justify-center transition-opacity">
      <div className="bg-white p-6 rounded-lg shadow-xl w-full max-w-sm mx-auto">
        <h3 className="text-lg font-semibold text-gray-800">{title}</h3>
        <p className="mt-2 text-sm text-gray-500">{description}</p>
        <div className="mt-4 flex justify-end space-x-2">
          <Button variant="outline" onClick={onCancel}>Cancel</Button>
          <Button variant="destructive" onClick={onConfirm}>Continue</Button>
        </div>
      </div>
    </div>
  );
};


function SystemNotificationList({ onLogout }) {
  const navigate = useNavigate();
  const [notifications, setNotifications] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showRestoreConfirm, setShowRestoreConfirm] = useState(false);
  const [selectedNotification, setSelectedNotification] = useState(null);

  useEffect(() => {
    fetchNotifications();
  }, []);

  const fetchNotifications = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await getSystemNotifications();
      setNotifications(data);
    } catch (err) {
      setError('Failed to fetch system notifications.');
      toast.error('Failed to fetch notifications.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleEdit = (id) => {
    navigate(`/system-owner/system-notifications/edit/${id}`);
  };

  const handleConfirmDelete = (notification) => {
    setSelectedNotification(notification);
    setShowDeleteConfirm(true);
  };

  const handleDelete = async () => {
    if (!selectedNotification) return;
    try {
      await deleteSystemNotification(selectedNotification.id);
      toast.success('Notification deleted successfully.');
      fetchNotifications();
    } catch (err) {
      toast.error('Failed to delete notification.');
    } finally {
      setShowDeleteConfirm(false);
      setSelectedNotification(null);
    }
  };

  const handleConfirmRestore = (notification) => {
    setSelectedNotification(notification);
    setShowRestoreConfirm(true);
  };

  const handleRestore = async () => {
    if (!selectedNotification) return;
    try {
      await restoreSystemNotification(selectedNotification.id);
      toast.success('Notification restored successfully.');
      fetchNotifications();
    } catch (err) {
      toast.error('Failed to restore notification.');
    } finally {
      setShowRestoreConfirm(false);
      setSelectedNotification(null);
    }
  };

  const getStatusBadge = (notification) => {
    const now = new Date();
    const startDate = new Date(notification.start_date);
    const endDate = new Date(notification.end_date);
    
    if (notification.is_deleted) {
      return <Badge variant="destructive">Deleted</Badge>;
    }
    if (!notification.is_active) {
      return <Badge variant="secondary">Inactive</Badge>;
    }
    if (now < startDate) {
      return <Badge variant="default">Scheduled</Badge>;
    }
    if (now > endDate) {
      return <Badge variant="secondary">Expired</Badge>;
    }
    return <Badge className="bg-green-500 text-white">Active</Badge>;
  };
  
  const getTargetText = (notification) => {
    const targetCustomers = notification.target_customer_ids?.length || 0;
    const targetUsers = notification.target_user_ids?.length || 0;
    const targetRoles = notification.target_roles?.length || 0;

    if (targetCustomers > 0 || targetUsers > 0 || targetRoles > 0) {
      let parts = [];
      if (targetCustomers > 0) parts.push(`Customers (${targetCustomers})`);
      if (targetUsers > 0) parts.push(`Users (${targetUsers})`);
      if (targetRoles > 0) parts.push(`Roles (${targetRoles})`);
      return parts.join(', ');
    }
    return 'All Users';
  };

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-semibold text-gray-800">System Notifications</h2>
        <Button onClick={() => navigate('/system-owner/system-notifications/new')}>
          <PlusCircle className="h-5 w-5 mr-2" /> Create Notification
        </Button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}

      {isLoading ? (
        <div className="text-center py-8">
          <Loader2 className="h-8 w-8 text-blue-600 animate-spin mx-auto" />
          <p className="text-gray-600 mt-2">Loading notifications...</p>
        </div>
      ) : notifications.length === 0 ? (
        <div className="bg-white p-6 rounded-lg shadow-md text-center">
          <p className="text-gray-500">No notifications found.</p>
        </div>
      ) : (
        <Table>
          <TableHeader>
            <tr>
              <TableHead className="w-1/3">Content</TableHead>
              <TableHead>Target</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Start Date</TableHead>
              <TableHead>End Date</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </tr>
          </TableHeader>
          <TableBody>
            {notifications.map((notification) => (
              <TableRow key={notification.id} isDeleted={notification.is_deleted}>
                <TableCell className="font-medium text-gray-900 w-1/3 max-w-sm truncate">{notification.content}</TableCell>
                <TableCell>
                  {getTargetText(notification)}
                </TableCell>
                <TableCell>{getStatusBadge(notification)}</TableCell>
                <TableCell>{format(new Date(notification.start_date), 'MMM dd, yyyy HH:mm')}</TableCell>
                <TableCell>{format(new Date(notification.end_date), 'MMM dd, yyyy HH:mm')}</TableCell>
                <TableCell className="text-right text-sm font-medium">
                  <div className="flex justify-end space-x-2">
                    {!notification.is_deleted && (
                      <>
                        <Button
                          variant="icon-blue"
                          size="sm"
                          onClick={(e) => { e.stopPropagation(); handleEdit(notification.id); }}
                          title="Edit"
                        >
                          <Edit className="h-5 w-5" />
                        </Button>
                        <Button
                          variant="icon-red"
                          size="sm"
                          onClick={(e) => { e.stopPropagation(); handleConfirmDelete(notification); }}
                          title="Delete"
                        >
                          <Trash className="h-5 w-5" />
                        </Button>
                      </>
                    )}
                    {notification.is_deleted && (
                      <Button
                        variant="icon-green"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); handleConfirmRestore(notification); }}
                        title="Restore"
                      >
                        <RotateCcw className="h-5 w-5" />
                      </Button>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      {showDeleteConfirm && selectedNotification && (
        <AlertDialog
          title="Are you sure you want to delete this notification?"
          description="This action will soft-delete the notification. It can be restored later."
          onConfirm={handleDelete}
          onCancel={() => setShowDeleteConfirm(false)}
        />
      )}

      {showRestoreConfirm && selectedNotification && (
        <AlertDialog
          title="Restore this notification?"
          description="This action will restore the deleted notification. It will be active again if it's within the date range."
          onConfirm={handleRestore}
          onCancel={() => setShowRestoreConfirm(false)}
        />
      )}
    </div>
  );
}

export default SystemNotificationList;