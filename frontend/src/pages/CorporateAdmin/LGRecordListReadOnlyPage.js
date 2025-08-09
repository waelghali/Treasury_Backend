// frontend/src/pages/CorporateAdmin/LGRecordListReadOnlyPage.js
import React from 'react';
import LGRecordList from '../EndUser/LGRecordList';

function LGRecordListReadOnlyPage({ isGracePeriod }) {
    return <LGRecordList isCorporateAdminView={true} isGracePeriod={isGracePeriod} />;
}

export default LGRecordListReadOnlyPage;