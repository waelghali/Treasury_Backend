// frontend/src/components/NotificationBanner.js
import React, { useState, useEffect } from 'react';
import { Info } from 'lucide-react';

function NotificationBanner({ notifications }) {
  const [activeNotifications, setActiveNotifications] = useState([]);

  useEffect(() => {
    if (notifications) {
      setActiveNotifications(notifications);
    }
  }, [notifications]);

  if (activeNotifications.length === 0) {
    return null;
  }

  const animationClasses = {
    'fade': 'animate-fade-in',
    'slide-in-top': 'animate-slide-in-top',
    'scroll-left': 'animate-scroll-left',
  };

  return (
    <div className="space-y-4 mb-6">
      {activeNotifications.map((notification) => (
        <div
          key={notification.id}
          className={`bg-blue-50 border-l-4 border-blue-400 text-blue-800 p-4 rounded-md shadow-sm flex items-start ${animationClasses[notification.animation_type] || animationClasses.fade}`}
          role="alert"
        >
          <div className="flex-shrink-0 mt-0.5">
            <Info className="h-5 w-5 text-blue-500" />
          </div>
          <div className="ml-3 flex-1">
            <p className="font-medium text-sm">System Announcement</p>
            <p className="text-sm mt-1">{notification.content}</p>
            {notification.link && (
              <a
                href={notification.link}
                className="text-sm font-semibold text-blue-600 hover:text-blue-700 underline mt-1 block"
                target="_blank"
                rel="noopener noreferrer"
              >
                Learn More
              </a>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

export default NotificationBanner;