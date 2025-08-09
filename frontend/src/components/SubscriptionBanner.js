import React from 'react';
import { AlertTriangle, Clock } from 'lucide-react';
import { Link } from 'react-router-dom';
import { differenceInDays } from 'date-fns';

function SubscriptionBanner({ subscriptionEndDate }) {
  if (!subscriptionEndDate) {
    return null;
  }

  const endDate = new Date(subscriptionEndDate);
  const today = new Date();
  const daysRemaining = differenceInDays(endDate, today);

  if (daysRemaining < 0) {
    return null; // Should be handled by route guard, but a safe check
  }

  return (
    <div className="bg-yellow-50 border-l-4 border-yellow-400 p-4 rounded-md shadow-sm mb-6 flex justify-between items-center animate-fade-in-down">
      <div className="flex items-center">
        <AlertTriangle className="h-6 w-6 text-yellow-500 flex-shrink-0" />
        <div className="ml-3 text-sm text-yellow-800">
          <p className="font-semibold">Your subscription has expired.</p>
          <p>You have **{daysRemaining} days** remaining in your read-only grace period. Please renew to regain full access.</p>
        </div>
      </div>
      <Link
        to="/renewal"
        className="ml-4 flex-shrink-0 bg-yellow-400 hover:bg-yellow-500 text-white font-medium py-2 px-4 rounded-md text-sm transition-colors duration-200"
      >
        Renew Now
      </Link>
    </div>
  );
}

export default SubscriptionBanner;