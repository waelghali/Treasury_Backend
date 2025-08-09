import React from 'react';
import { useNavigate } from 'react-router-dom';

function RenewalPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100 p-4">
      <div className="max-w-lg w-full space-y-8 p-10 bg-white rounded-xl shadow-lg text-center">
        <div className="space-y-2">
          <h1 className="text-4xl font-extrabold text-red-600">Account Expired</h1>
          <p className="mt-2 text-lg text-gray-600">Your subscription has expired and access to the platform has been disabled.</p>
          <p className="text-gray-500">To regain access and continue using the service, please contact your System Owner or our sales team to renew your subscription.</p>
        </div>
        <div className="mt-6">
          <h2 className="text-xl font-bold text-gray-700">Contact Information</h2>
          <p className="mt-2 text-gray-500">Email: sales@treasury-platform.com</p>
          <p className="text-gray-500">Phone: +1 (555) 123-4567</p>
        </div>
        <div className="mt-8">
          <button
            onClick={() => navigate('/login', { replace: true })}
            className="w-full inline-flex justify-center py-3 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
          >
            Return to Login
          </button>
        </div>
      </div>
    </div>
  );
}

export default RenewalPage;