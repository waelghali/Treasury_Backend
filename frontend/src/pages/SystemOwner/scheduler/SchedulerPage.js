import React, { useState, useEffect, useRef } from 'react';
import { apiRequest } from '../../../services/apiService';
import { Play, Pause, Repeat, FastForward, Clock, Calendar, X } from 'lucide-react';
import { toast } from 'react-toastify';
import moment from 'moment-timezone';

const jobIcons = {
  'undelivered_report_daily_job': <Repeat className="h-5 w-5 text-purple-500" />,
  'print_reminders_daily_job': <Clock className="h-5 w-5 text-blue-500" />,
  'renewal_reminders_daily_job': <Repeat className="h-5 w-5 text-green-500" />,
};

// Common input field styling classes
const inputClassNames = "mb-2 mt-1 block w-full text-base px-3 py-2 rounded-md border border-gray-300 bg-white shadow-sm focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-all duration-200";
const labelClassNames = "block text-sm font-medium text-gray-700";

function SchedulerPage({ onLogout }) {
  const [jobs, setJobs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [showRescheduleForm, setShowRescheduleForm] = useState(null);
  const modalRef = useRef(null);

  const fetchJobs = async () => {
    setIsLoading(true);
    try {
      const response = await apiRequest('/system-owner/scheduler/jobs', 'GET');
      setJobs(response.jobs);
      setError('');
    } catch (err) {
      setError('Failed to fetch scheduled jobs. Please check API status.');
      toast.error('Failed to fetch jobs.');
      console.error(err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, []);

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key === 'Escape') {
        setShowRescheduleForm(null);
      }
    };
    if (showRescheduleForm) {
      document.addEventListener('keydown', handleKeyDown);
    } else {
      document.removeEventListener('keydown', handleKeyDown);
    }
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [showRescheduleForm]);

  const handleAction = async (jobId, endpoint, payload = {}) => {
    try {
      const response = await apiRequest(`/system-owner/scheduler/${endpoint}/${jobId}`, 'POST', payload);
      toast.success(response.message);
      fetchJobs();
    } catch (err) {
      setError(`Error performing action: ${err.message}`);
      toast.error(`Action failed: ${err.message}`);
      console.error(err);
    }
  };

  const handleRescheduleSubmit = async (e, jobId) => {
    e.preventDefault();
    const form = e.target;
    const triggerType = form.elements['trigger_type'].value;
    const payload = { trigger_type: triggerType };

    if (triggerType === 'cron') {
      const hour = parseInt(form.elements['hour'].value, 10);
      const minute = parseInt(form.elements['minute'].value, 10);
      payload.hour = hour;
      payload.minute = minute;
      payload.timezone = "Africa/Cairo";
    } else if (triggerType === 'date') {
      const runDate = moment(form.elements['run_date'].value).tz("Africa/Cairo").toISOString();
      payload.run_date = runDate;
    }

    try {
      const response = await apiRequest(`/system-owner/scheduler/reschedule_job/${jobId}`, 'POST', payload);
      toast.success(response.message);
      setShowRescheduleForm(null);
      fetchJobs();
    } catch (err) {
      setError(`Error rescheduling job: ${err.message}`);
      toast.error(`Rescheduling failed: ${err.message}`);
      console.error(err);
    }
  };
const RescheduleForm = ({ job }) => (
  <div
    className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50 flex justify-center items-center"
    onClick={() => setShowRescheduleForm(null)}
  >
    <div
      className="relative p-6 border w-full max-w-md shadow-md rounded-lg bg-white mx-2 text-left"
      ref={modalRef}
      onClick={e => e.stopPropagation()}
    >
      <div className="flex justify-between items-start mb-4">
        <h5 className="text-md font-medium text-gray-800">Reschedule: {job.name}</h5>
        <button onClick={() => setShowRescheduleForm(null)} className="text-gray-500 hover:text-gray-800">
          <X className="h-5 w-5" />
        </button>
      </div>
      <form onSubmit={(e) => handleRescheduleSubmit(e, job.id)} className="space-y-4">
        <div className="mb-2">
          <label htmlFor="trigger_type" className={labelClassNames}>Trigger Type</label>
          <select
            name="trigger_type"
            id="trigger_type"
            className={inputClassNames}
          >
            <option value="cron">Cron (Recurring)</option>
            <option value="date">Date (One-time)</option>
          </select>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div className="mb-2">
            <label htmlFor="hour" className={labelClassNames}>Hour (0-23)</label>
            <input
              type="number"
              name="hour"
              id="hour"
              defaultValue={2}
              min="0"
              max="23"
              required
              className={inputClassNames}
            />
          </div>
          <div className="mb-2">
            <label htmlFor="minute" className={labelClassNames}>Minute (0-59)</label>
            <input
              type="number"
              name="minute"
              id="minute"
              defaultValue={0}
              min="0"
              max="59"
              required
              className={inputClassNames}
            />
          </div>
        </div>
        <div className="mb-2">
          <label htmlFor="run_date" className={labelClassNames}>Specific Date/Time (for 'date' trigger)</label>
          <input
            type="datetime-local"
            name="run_date"
            id="run_date"
            className={inputClassNames}
          />
        </div>
		          <button
            type="submit"
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200"
          >
            Reschedule Job
          </button>
      </form>
    </div>
  </div>
);

  if (isLoading) {
    return (
      <div className="text-center py-8">
        <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
        <p className="text-gray-600 mt-2">Loading data...</p>
      </div>
    );
  }

  return (
    <div className="p-6">
      <h2 className="text-2xl font-semibold text-gray-800 mb-6 text-left">Scheduler Management</h2>
      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-md relative mb-4" role="alert">
          <span className="block sm:inline">{error}</span>
        </div>
      )}
      <div className="bg-white p-6 rounded-lg shadow-md">
        <div className="overflow-auto border border-gray-200 rounded-md">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Next Run Time</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Trigger</th>
                <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {jobs.length > 0 ? (
                jobs.map((job, index) => (
                  <tr key={job.id} className={`${index % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-gray-100`}>
                    <td className="px-6 py-4 text-sm font-medium text-gray-900 flex items-center space-x-2 whitespace-normal">
                      {jobIcons[job.id] || <Clock className="h-5 w-5 text-gray-500" />}
                      <span className="break-words">{job.name}</span>
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-500 whitespace-normal">{job.id}</td>
                    <td className="px-6 py-4 text-sm text-gray-500 whitespace-normal">
                      {job.next_run_time ? moment(job.next_run_time).tz("Africa/Cairo").format('YYYY-MM-DD HH:mm:ss z') : 'N/A'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-500 whitespace-normal">{job.trigger}</td>
                    <td className="px-6 py-4 text-right text-sm font-medium">
                      <div className="relative inline-flex items-center space-x-2">
                        <button
                          onClick={() => handleAction(job.id, 'run_job')}
                          className="p-2 rounded-md hover:bg-gray-200"
                          title="Run Now"
                        >
                          <FastForward className="h-5 w-5 text-green-500" />
                        </button>
                        <button
                          onClick={() => handleAction(job.id, 'pause_job')}
                          className="p-2 rounded-md hover:bg-gray-200"
                          title="Pause Job"
                        >
                          <Pause className="h-5 w-5 text-yellow-500" />
                        </button>
                        <button
                          onClick={() => handleAction(job.id, 'resume_job')}
                          className="p-2 rounded-md hover:bg-gray-200"
                          title="Resume Job"
                        >
                          <Play className="h-5 w-5 text-green-500" />
                        </button>
                        <button
                          onClick={() => setShowRescheduleForm(showRescheduleForm === job.id ? null : job.id)}
                          className="p-2 rounded-md hover:bg-gray-200"
                          title="Reschedule Job"
                        >
                          <Calendar className="h-5 w-5 text-blue-500" />
                        </button>
                        {showRescheduleForm === job.id && <RescheduleForm job={job} />}
                      </div>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan="5" className="px-6 py-4 text-center text-gray-500">No scheduled jobs found.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default SchedulerPage;