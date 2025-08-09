// frontend/src/components/LGActionsMenu.js
import React from 'react';
import { Menu, MenuButton, MenuItem, MenuItems, Transition } from '@headlessui/react';
import { EllipsisVertical, CalendarPlus, Users, CheckCircle, FileMinus, MinusCircle, Eye } from 'lucide-react';
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

function classNames(...classes) {
  return classes.filter(Boolean).join(' ');
}

const LGActionsMenu = ({ lgRecord, onExtend, onChangeOwner, onRelease, onLiquidate, onDecreaseAmount, onViewDetails, isGracePeriod }) => {
  const isLgValid = lgRecord.lg_status?.name === 'Valid';
  const isLgValidOrActive = ['Valid', 'Active'].includes(lgRecord.lg_status?.name);

  // Define a wrapper for actions that checks for grace period
  const handleAction = (action) => {
    if (isGracePeriod) {
      toast.warn("This action is disabled during your subscription's grace period.");
      return;
    }
    action();
  };

  return (
    <Menu as="div" className="relative inline-block text-left">
      <div>
        <MenuButton className="inline-flex justify-center w-full rounded-md p-1 text-sm font-medium text-gray-700 hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-blue-500">
          <EllipsisVertical className="h-5 w-5" aria-hidden="true" />
        </MenuButton>
      </div>

      <Transition
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <MenuItems className="origin-top-right absolute right-0 mt-2 w-56 rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 focus:outline-none z-10">
          <div className="py-1">
            <MenuItem>
              {({ active }) => (
                <button
                  onClick={() => handleAction(() => onViewDetails(lgRecord.id))}
                  className={classNames(
                    active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                    'group flex items-center w-full px-4 py-2 text-sm'
                  )}
                >
                  <Eye className="mr-3 h-5 w-5 text-gray-400 group-hover:text-gray-500" aria-hidden="true" />
                  View Details
                </button>
              )}
            </MenuItem>

            {isLgValid && (
              <MenuItem disabled={isGracePeriod}>
                {({ active }) => (
                  <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                    <button
                      onClick={() => handleAction(() => onExtend(lgRecord))}
                      className={classNames(
                        active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                        'group flex items-center w-full px-4 py-2 text-sm',
                        isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''
                      )}
                      disabled={isGracePeriod}
                    >
                      <CalendarPlus className="mr-3 h-5 w-5 text-indigo-400 group-hover:text-indigo-500" aria-hidden="true" />
                      Extend LG
                    </button>
                  </GracePeriodTooltip>
                )}
              </MenuItem>
            )}
            <MenuItem disabled={isGracePeriod}>
              {({ active }) => (
                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                  <button
                    onClick={() => handleAction(() => onDecreaseAmount(lgRecord))}
                    className={classNames(
                      active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                      'group flex items-center w-full px-4 py-2 text-sm',
                      isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''
                    )}
                    disabled={isGracePeriod}
                  >
                    <MinusCircle className="mr-3 h-5 w-5 text-orange-400 group-hover:text-orange-500" aria-hidden="true" />
                    Decrease Amount
                  </button>
                </GracePeriodTooltip>
              )}
            </MenuItem>

            {isLgValidOrActive && (
              <>
                <MenuItem disabled={isGracePeriod}>
                  {({ active }) => (
                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                      <button
                        onClick={() => handleAction(() => onRelease(lgRecord))}
                        className={classNames(
                          active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                          'group flex items-center w-full px-4 py-2 text-sm',
                          isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''
                        )}
                        disabled={isGracePeriod}
                      >
                        <CheckCircle className="mr-3 h-5 w-5 text-green-400 group-hover:text-green-500" aria-hidden="true" />
                        Release LG
                      </button>
                    </GracePeriodTooltip>
                  )}
                </MenuItem>
                <MenuItem disabled={isGracePeriod}>
                  {({ active }) => (
                    <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                      <button
                        onClick={() => handleAction(() => onLiquidate(lgRecord))}
                        className={classNames(
                          active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                          'group flex items-center w-full px-4 py-2 text-sm',
                          isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''
                        )}
                        disabled={isGracePeriod}
                      >
                        <FileMinus className="mr-3 h-5 w-5 text-red-400 group-hover:text-red-500" aria-hidden="true" />
                        Liquidate LG
                      </button>
                    </GracePeriodTooltip>
                  )}
                </MenuItem>
              </>
            )}
            <MenuItem disabled={isGracePeriod}>
              {({ active }) => (
                <GracePeriodTooltip isGracePeriod={isGracePeriod}>
                  <button
                    onClick={() => handleAction(() => onChangeOwner(lgRecord))}
                    className={classNames(
                      active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
                      'group flex items-center w-full px-4 py-2 text-sm',
                      isGracePeriod ? 'opacity-50 cursor-not-allowed' : ''
                    )}
                    disabled={isGracePeriod}
                  >
                    <Users className="mr-3 h-5 w-5 text-purple-400 group-hover:text-purple-500" aria-hidden="true" />
                    Change Owner
                  </button>
                </GracePeriodTooltip>
              )}
            </MenuItem>
          </div>
        </MenuItems>
      </Transition>
    </Menu>
  );
};

export default LGActionsMenu;