// frontend/src/pages/CorporateAdmin/LGDetailsReadOnlyPage.js
import React from 'react';
import LGDetailsPage from '../EndUser/LGDetailsPage'; // Import the reusable component

function LGDetailsReadOnlyPage({ isGracePeriod }) {
    // This component renders the LGDetailsPage but passes the props
    // to indicate it's a Corporate Admin view and if it's a grace period.
    return <LGDetailsPage isCorporateAdminView={true} isGracePeriod={isGracePeriod} />;
}

export default LGDetailsReadOnlyPage;