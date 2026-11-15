"""
Generates studio-grade bilingual (English + Arabic) Neural Text-to-Speech MP3 narrations
for all 4 Executive Module Pitches, 4 Platform Governance & Security Pillars, and all 15
Interactive Demo Tours (81 live steps x EN/AR = 162 step files + 16 pitch files = 178 MP3s).

Voice & Phonetic Engineering:
- English: `en-US-AvaMultilingualNeural` at `-2%` natural executive presenter pace.
- Arabic: `ar-JO-SanaNeural` at `-2%` Modern Standard Arabic broadcast pace, with full
  grammatical vocalization on every non-pausal `ة` (construct-state & connected `/t/`) vs
  natural pausal `-ah` (`ه`) at phrase boundaries, explicit pausal sukoon on non-`ة` stops,
  and coarticulated English brand pronunciation for `Grow` (never `جرو`).
"""

import asyncio
import os
import re
import sys
import edge_tts

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUDIO_ROOT = os.path.join(BASE_DIR, "frontend", "public", "demo", "audio")
PITCHES_DIR = os.path.join(AUDIO_ROOT, "pitches")
os.makedirs(PITCHES_DIR, exist_ok=True)

VOICE_EN = "en-US-AvaMultilingualNeural"
RATE_EN = "-2%"

VOICE_AR = "ar-JO-SanaNeural"
RATE_AR = "-2%"

FRAME_BYTES = 144  # 24ms CBR MP3 frame @ 24kHz 48kbps mono

PITCH_NARRATIONS = {
    'pitch_custody': {
        "en": 'Module One, Executive Brief: Letter of Guarantee Custody. Never lose financial security to a missed expiry date or a hidden claim trap. For Chief Financial Officers and Treasurers safeguarding millions in supplier and contractor guarantees, missing just one expiry date in a spreadsheet means losing your financial protection overnight. Grow digitizes incoming guarantees with AI in seconds and highlights conditional claim wording upfront. Safe, protective extensions execute directly without approval bottlenecks. Meanwhile, every release, reduction, amendment, or liquidation enforces strict Maker-Checker control, backed by automated reminders and management escalations whenever an owner action, letter delivery, or bank reply is delayed.',
        "ar": 'مُلَخَّصٌ لِصَانِعِ القَرَارْ، الوَحْدَةُ الأُولَى: حِفْظُ خِطَابَاتِ الضَّمَانِ الوَارِدَة. حِمَايَةٌ كَامِلَةٌ لِلْغِطَاءِ المَالِيِّ لِلشَّرِكَةِ مِنْ ضَيَاعِ مَوَاعِيدِ الانْتِهَاءِ أَوْ شُرُوطِ المُصَادَرَةِ المُقَيِّدَة. بِالنِّسْبَةِ لِلْمُدِيرِينَ المَالِيِّينَ وَمُدِيرِي الخِزَانَةِ المَسْؤُولِينَ عَنْ ضَمَانَاتٍ بِمَلَايِينِ الجُنَيْهَاتْ، فَإِنَّ فَوَاتَ مَوْعِدِ انْتِهَاءِ ضَمَانٍ وَاحِدٍ فِي جَدَاوِلِ الإِكْسِيلْ يَعْنِي ضَيَاعَ حَقِّ الشَّرِكَةِ المَالِيِّ فَوْراً. تَقُومُ وَحْدَةُ الحِفْظِ بِرَقْمَنَةِ الضَّمَانَاتِ الوَارِدَةِ بِالذَّكَاءِ الاصْطِنَاعِيِّ خِلَالَ ثَوَانٍ، وَكَشْفِ شُرُوطِ التَّسْيِيلِ المُقَيِّدَةِ مُبَكِّراً. وَتُنَفِّذُ طَلَبَاتِ التَّمْدِيدِ الحِمَائِيَّةَ مُبَاشَرَةً دُونَ تَعْطِيلْ، مَعَ فَرْضِ رَقَابَةِ المُرَاجِعِ وَالمُعْتَمِدِ الصَّارِمَةِ عَلَى كُلِّ إِفْرَاجٍ أَوْ تَخْفِيضٍ أَوْ تَعْدِيلٍ أَوْ تَسْيِيلْ، وَإِرْسَالِ تَنْبِيهَاتٍ وَتَصْعِيدٍ آلِيٍّ عِنْدَ تَأَخُّرِ مَسْؤُولِ المَشْرُوعِ أَوِ التَّسْلِيمِ أَوْ رَدِّ البَنْكْ.',
    },
    'pitch_issuance': {
        "en": 'Module Two, Executive Brief: Letter of Guarantee Issuance. Lower bank guarantee commissions, free up trapped cash margins, and issue faster. Managing outgoing guarantee requests over email leads to duplicate submissions, routing deals to higher-cost banks, and leaving expired guarantees locking up credit lines for months. Grow standardizes department intake and scores all bank facilities in real time for the lowest commission and cash margin. It enforces multi-step credit approvals with SLA reminders and escalations, automatically fills official bank PDF forms, and reconciles bank position statements to reclaim trapped limits.',
        "ar": 'مُلَخَّصٌ لِصَانِعِ القَرَارْ، الوَحْدَةُ الثَّانِيَة: إِصْدَارُ خِطَابَاتِ الضَّمَانِ الصَّادِرَة. خَفْضُ عُمُولَاتِ الإِصْدَارْ، وَتَحْرِيرُ الغِطَاءِ النَّقْدِيِّ المُحْتَجَزْ، وَتَسْرِيعُ دَوْرَةِ التَّنْفِيذْ. إِدَارَةُ طَلَبَاتِ إِصْدَارِ خِطَابَاتِ الضَّمَانِ عَبْرَ البَرِيدِ الإِلِكْتْرُونِيِّ تُؤَدِّي لِتَكْرَارِ الطَّلَبَاتِ وَتَوْجِيهِهَا لِبُنُوكٍ أَعْلَى تَكْلِفَة، وَبَقَاءِ ضَمَانَاتٍ مُنْتَهِيَةٍ تَحْجِزُ حُدُودَ التَّسْهِيلَاتِ البَنْكِيَّةِ لِشُهُورْ. تُوَحِّدُ هَذِهِ الوَحْدَةُ اسْتِقْبَالَ طَلَبَاتِ الإِدَارَاتْ، وَتُفَاضِلُ لَحْظِيّاً بَيْنَ البُنُوكِ لِاخْتِيَارِ أَقَلِّ عُمُولَةٍ وَغِطَاءٍ نَقْدِيّ. كَمَا تُطَبِّقُ مَصْفُوفَةَ المَوَافَقَاتِ الائْتِمَانِيَّةِ مَعَ تَنْبِيهَاتٍ وَتَصْعِيدٍ لِلتَّأْخِيرْ، وَتَمْلَأُ نَمَاذِجَ البُنُوكِ الرَّسْمِيَّةَ تِلْقَائِيّاً، مَعَ مُطَابَقَةِ كُشُوفِ المَرَاكِزِ لِتَحْرِيرِ الحُدُودِ المُحْتَجَزَة.',
    },
    'pitch_quotations': {
        "en": 'Module Three, Executive Brief: Foreign Exchange and Treasury Bill Quotations. Capture every basis point with auditable multi-bank competition. Shopping Foreign Exchange Spot and Sovereign Treasury Bill rates over phone calls or chat leaves basis points on the table and gives boards and auditors zero proof of best execution. Grow supports pre-broadcast approval and dispatches multi-pair currency baskets and Treasury Bill quote requests simultaneously to relationship banks through secure, zero-password dealer links. It gives dealers a blind live competitive rank to sharpen quotes without revealing rival prices, ranks bids by true all-in net cost after bank fee schedules and settlement-date time value of money, lets treasurers cherry-pick winning banks per currency leg, and seals Best Execution Audit Certificates alongside cryptographically signed Deal Execution Receipts.',
        "ar": 'مُلَخَّصٌ لِصَانِعِ القَرَارْ، الوَحْدَةُ الثَّالِثَة: عُرُوضُ أَسْعَارِ العُمْلَاتِ وَأُذُونِ الخِزَانَة. تَحْقِيقُ أَفْضَلِ سِعْرِ تَنْفِيذٍ صَافٍ بِمُنَافَسَةٍ مَصْرِفِيَّةٍ فَوْرِيَّةٍ وَقَابِلَةٍ لِلْمُرَاجَعَة. التَّفَاوُضُ الهَاتِفِيُّ عَلَى أَسْعَارِ العُمْلَاتِ الفَوْرِيَّةِ وَأُذُونِ الخِزَانَةِ يُفْقِدُ الشَّرِكَةَ نِقَاطاً سِعْرِيَّةً مُؤَثِّرَة، وَلَا يَتْرُكُ أَيَّ إِثْبَاتٍ رَقَابِيٍّ عَلَى عَدَالَةِ التَّنْفِيذْ. تَدْعَمُ هَذِهِ الوَحْدَةُ اعْتِمَادَ مَا قَبْلَ البَثّ، وَتُرْسِلُ طَلَبَاتِ سَلَّاتِ العُمْلَاتِ الفَوْرِيَّةِ وَأُذُونِ الخِزَانَةِ لِكَافَّةِ البُنُوكِ فِي لَحْظَةٍ وَاحِدَةٍ عَبْرَ رَوَابِطَ مُؤَمَّنَةٍ بِدُونِ كَلِمَةِ مُرُورْ. وَتَعْرِضُ لِلْمُتَدَاوِلِ تَرْتِيبَهُ التَّنَافُسِيَّ الحَيَّ لِتَحْسِينِ السِّعْرِ دُونَ كَشْفِ أَسْعَارِ المُنَافِسِينْ، ثُمَّ تُرَتِّبُ العُرُوضَ بِالصَّافِي الشَّامِلِ بَعْدَ مَصْفُوفَةِ العُمُولَاتِ وَتَكْلِفَةِ تَارِيخِ التَّسْوِيَة، وَتُتِيحُ تَرْسِيَةَ كُلِّ عُمْلَةٍ لِأَفْضَلِ بَنْكْ، مَعَ إِصْدَارِ شَهَادَةِ تَدْقِيقٍ وَإِيصَالَاتِ صَفْقَةٍ رَقْمِيَّةٍ مَعْزُولَة.',
    },
    'pitch_reconciliation': {
        "en": 'Module Four, Executive Brief: Bank Auto-Reconciliation. Turn days of manual statement matching into minutes of exception governance. Finance teams lose days every month matching multi-bank statements, chasing wire fee differences across inter-account transfers, and posting bank charges manually. Grow imports Arabic and English bank statements with zero setup and automatically classifies bank fees, taxes, payroll, interest, and bounced checks. It pairs liquidity sweeps and error reversals automatically, learns recurring counterparty rules from user confirmations while protecting sensitive identities, flags ambiguous entries clearly for human review, and exports accounting entries through customizable ERP and General Ledger profiles.',
        "ar": 'مُلَخَّصٌ لِصَانِعِ القَرَارْ، الوَحْدَةُ الرَّابِعَة: المُطَابَقَةُ البَنْكِيَّةُ الذَّكِيَّة. تَحْوِيلُ أَيَّامِ المُطَابَقَةِ اليَدَوِيَّةِ المُرْهِقَةِ إِلَى دَقَائِقَ مِنَ الرَّقَابَةِ الدَّقِيقَةِ عَلَى الاسْتِثْنَاءَاتْ. تَسْتَنْزِفُ الفِرَقُ المَالِيَّةُ أَيَّاماً كُلَّ شَهْرٍ فِي مُطَابَقَةِ كُشُوفِ الحِسَابَاتِ المُتَعَدِّدَة، وَتَتَبُّعِ فُرُوقِ عُمُولَاتِ التَّحْوِيلَاتِ البَيْنِيَّةِ وَتَسْجِيلِ المَصْرُوفَاتِ يَدَوِيّاً. تَسْتَوْرِدُ هَذِهِ الوَحْدَةُ كُشُوفَ الحِسَابَاتِ العَرَبِيَّةَ وَالإِنْجِلِيزِيَّةَ فَوْراً دُونَ إِعْدَادَاتٍ مُسْبَقَة، وَتُصَنِّفُ العُمُولَاتِ وَالضَّرَائِبَ وَالرَّوَاتِبَ وَالفَوَائِدَ وَالشِّيكَاتِ المُرْتَدَّة، وَتَرْبِطُ التَّحْوِيلَاتِ البَيْنِيَّةَ وَالقُيُودَ العَكْسِيَّةَ تِلْقَائِيّاً. كَمَا تَتَعَلَّمُ القَوَاعِدَ المُتَكَرِّرَةَ مِنْ تَأْكِيدَاتِ المُسْتَخْدِمِ مَعَ حِمَايَةِ سِرِّيَّةِ البَيَانَاتْ، وَتَفْرِزُ المُعَلَّقَاتِ الغَامِضَةَ لِلْمُرَاجَعَةِ البَشَرِيَّةِ قَبْلَ تَصْدِيرِ القُيُودِ بِقَوَالِبَ مُخَصَّصَةٍ لِنِظَامِ إي آر بي.',
    },
    'foundation_governance': {
        "en": 'Platform Governance Foundation: Risk-Smart Maker-Checker Control, Proactive Reminders, and Escalations. Grow enforces strict Maker-Checker matrices, blocking self-approval wherever risk or bank liability is affected. That includes LG releases, reductions, liquidations, new credit issuances, and pre-broadcast quote approvals. At the same time, safe, company-favorable Custody LG extensions execute directly without delay. Automated multi-stage timers send reminders for expiries, unprinted letters, and overdue bank replies, escalating delayed actions to line managers and Corporate Admins before rights lapse.',
        "ar": 'أَسَاسِيَّاتُ الحَوْكَمَة: حَوْكَمَةٌ ذَكِيَّةٌ لِلْمُرَاجِعِ وَالمُعْتَمِدْ، وَتَنْبِيهَاتٌ اسْتِبَاقِيَّةٌ وَتَصْعِيدٌ تِلْقَائِيٌّ لِلتَّأْخِيرْ. تُطَبِّقُ مِنَصَّةُ Grow رَقَابَةَ المُرَاجِعِ وَالمُعْتَمِدِ الصَّارِمَةَ مَعَ مَنْعِ الاعْتِمَادِ الذَّاتِيِّ عَلَى الإِجْرَاءَاتِ المُؤَثِّرَةِ عَلَى المَخَاطِرِ أَوِ الالْتِزَامَاتْ، مِثْلِ الإِفْرَاجِ أَوِ التَّخْفِيضِ أَوِ التَّسْيِيلِ أَوِ اعْتِمَادِ طَلَبَاتِ الإِصْدَارِ وَعُرُوضِ الأَسْعَارْ. بَيْنَمَا يُنَفَّذُ تَمْدِيدُ الضَّمَانَاتِ الوَارِدَةِ مُبَاشَرَةً دُونَ انْتِظَارِ اعْتِمَادْ، لِتَفَادِي أَيِّ تَأْخِيرٍ فِي حِمَايَةِ حَقِّ الشَّرِكَة. كَمَا تُرْسِلُ المِنَصَّةُ تَنْبِيهَاتٍ اسْتِبَاقِيَّةً لِلِاسْتِحْقَاقَاتِ وَالطِّبَاعَةِ وَرُدُودِ البُنُوكْ، مَعَ تَصْعِيدٍ آلِيٍّ لِلْمُدِيرِينَ وَمَسْؤُولِي النِّظَامِ عِنْدَ التَّأَخُّرْ.',
    },
    'foundation_security': {
        "en": 'Platform Security Foundation: Per-Company Encryption, Isolated Vaults, and Privacy-Shielded AI. Sensitive financial rates and bank statement fields are sealed with dedicated per-company AES-256 encryption keys. Guarantee scans are encrypted inside tenant-isolated cloud storage, with optional dedicated buckets per enterprise customer. Document AI and rule learning operate under strict privacy guardrails and sensitive data tokenization, while every user action is recorded in immutable audit logs.',
        "ar": 'أَسَاسِيَّاتُ الأَمَانْ: تَشْفِيرٌ مُسْتَقِلٌّ لِكُلِّ شَرِكَة، وَعَزْلٌ تَامٌّ لِلْمُسْتَنَدَاتْ، وَذَكَاءٌ اصْطِنَاعِيٌّ يَحْمِي السِّرِّيَّة. تُحْمَى الأَسْعَارُ المَالِيَّةُ وَبَيَانَاتُ كُشُوفِ الحِسَابَاتِ الحَسَّاسَةُ بِمَفَاتِيحِ تَشْفِيرٍ مُسْتَقِلَّةٍ لِكُلِّ شَرِكَة، وَتُحْفَظُ صُوَرُ الضَّمَانَاتِ مُشَفَّرَةً فِي خَزَائِنَ سَحَابِيَّةٍ مَعْزُولَةٍ مَعَ إِمْكَانِيَّةِ تَخْصِيصِ مِسَاحَةِ تَخْزِينٍ مُسْتَقِلَّةٍ لِكُلِّ عَمِيلْ. وَيَعْمَلُ الذَّكَاءُ الاصْطِنَاعِيُّ وَتَعَلُّمُ القَوَاعِدِ مَعَ إِخْفَاءِ البَيَانَاتِ الحَسَّاسَةِ وَحِمَايَةِ السِّرِّيَّة، وَتَوْثِيقِ كُلِّ حَرَكَةٍ فِي سِجِلِّ مُرَاجَعَةٍ غَيْرِ قَابِلٍ لِلتَّعْدِيلْ.',
    },
    'foundation_verification': {
        "en": 'External Verification Foundation: Tamper-Evident Bank Instruction Letters with Instant QR and Security Code Verification. Every outgoing bank letter, whether extending, reducing, releasing, or liquidating a guarantee, carries a digitally signed QR code and a split security code so receiving banks can verify authenticity on the public portal in seconds. Meanwhile, Grow tracks print and delivery escalations, bank receiving stamps, and AI bank-reply verification.',
        "ar": 'مَوْثُوقِيَّةُ المُرَاسَلَاتِ البَنْكِيَّة: خِطَابَاتُ تَعْلِيمَاتٍ مُؤَمَّنَةٌ ضِدَّ التَّزْوِيرِ بِتَحَقُّقٍ فَوْرِيٍّ عَبْرَ رَمْزِ كيو آرْ وَكُودِ تَحَقُّقْ. يَحْمِلُ كُلُّ خِطَابٍ صَادِرٍ لِلْبَنْكْ، سَوَاءٌ لِلتَّمْدِيدِ أَوِ التَّخْفِيضِ أَوِ الإِفْرَاجِ أَوِ التَّسْيِيلْ، رَمْزَ اسْتِجَابَةٍ سَرِيعَةٍ مُوَقَّعاً رَقْمِيّاً وَكُودَ تَحَقُّقٍ يُتِيحُ لِلْبَنْكِ التَّأَكُّدَ الفَوْرِيَّ مِنْ صِحَّةِ الخِطَابِ عَبْرَ بَوَّابَةِ التَّحَقُّقْ، مَعَ تَتَبُّعِ الطِّبَاعَةِ وَتَصْعِيدِ التَّأْخِيرْ، وَتَوْثِيقِ خَتْمِ اسْتِلَامِ البَنْكِ وَالمُطَابَقَةِ الذَّكِيَّةِ لِرُدُودِ البُنُوكْ.',
    },
    'foundation_enterprise': {
        "en": 'Enterprise Scale Foundation: Multi-Subsidiary Visibility, Executive Dashboards, and Bulk Portfolio Migration. Built for holding groups and multi-entity corporates, Grow unifies subsidiary permissions, departments, corporate projects, custom LG categories, and approval groups with group-wide exposure dashboards. It also includes a smart Migration Hub that maps, validates, and imports legacy Excel trackers and PDF archives in hours instead of months.',
        "ar": 'جَاهِزِيَّةُ المَجْمُوعَاتْ: رُؤْيَةٌ مُوَحَّدَةٌ لِشَرِكَاتِ المَجْمُوعَة، وَتَقَارِيرُ تَنْفِيذِيَّةٌ وَتَرْحِيلٌ ذَكِيٌّ لِلْمَحَافِظِ القَائِمَة. صُمِّمَتِ المِنَصَّةُ لِلشَّرِكَاتِ القَابِضَةِ وَالمَجْمُوعَاتِ مُتَعَدِّدَةِ الكِيَانَاتْ، لِتَوْحِيدِ صَلَاحِيَّاتِ الشَّرِكَاتِ التَّابِعَةِ وَالإِدَارَاتِ وَالمَشَارِيعْ، وَتَصْنِيفَاتِ الضَّمَانَاتِ وَمَجْمُوعَاتِ الاعْتِمَادِ مَعَ لَوْحَاتِ تَعَرُّضَاتٍ مُجَمَّعَة، إِلَى جَانِبِ مَرْكَزِ تَرْحِيلٍ ذَكِيٍّ يَفْحَصُ وَيَسْتَوْرِدُ سِجِلَّاتِ الإِكْسِيلْ وَأَرْشِيفَ مَلَفَّاتِ بي دي إفْ التَّارِيخِيَّ خِلَالَ سَاعَاتٍ بَدَلاً مِنْ شُهُورْ.',
    },
}

TOUR_STEP_NARRATIONS = {
    'custody_scan': {
        1: {
            "en": 'Step One: Start from a Single Clean Dropzone with No Manual Typing. When a supplier or contractor delivers a new Letter of Guarantee, your team opens Record New LG. Instead of typing over fifteen fields by hand, they simply upload the PDF or scanned image. Click Upload and Scan on the screen to continue.',
            "ar": 'الخُطْوَةُ الأُولَى: ابْدَأْ مِنْ نَافِذَةِ رَفْعٍ ذَكِيَّةٍ وَاحِدَة، بِدُونِ إِدْخَالٍ يَدَوِيٍّ مُرْهِقْ. عِنْدَ اسْتِلَامِ خِطَابِ ضَمَانٍ جَدِيدٍ مِنْ مُقَاوِلٍ أَوْ مُوَرِّدْ، يَفْتَحُ فَرِيقُ الخِزَانَةِ شَاشَةَ التَّسْجِيلِ لِضَمَانٍ جَدِيدْ. وَبَدَلاً مِنْ كِتَابَةِ عَشَرَاتِ الحُقُولِ يَدَوِيّاً، يَرْفَعُ مَلَفَّ بي دي إفْ أَوِ الصُّورَةَ مُبَاشَرَةً. اضْغَطْ عَلَى زِرِّ الرَّفْعِ وَالمَسْحِ عَلَى الشَّاشَةِ لِلْمُتَابَعَة.',
        },
        2: {
            "en": "Step Two: Attach the Bank Instrument and Launch AI OCR. Here, we attached a four point five million Egyptian Pound Performance Guarantee issued by First National Corporate Bank on behalf of NorthStar Electromechanical. One click on Scan Now sends it through Grow's privacy-shielded AI parser. Click Scan Now to extract the data.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: إِرْفَاقُ أَصْلِ خِطَابِ الضَّمَانِ وَتَشْغِيلُ مُحَرِّكِ الذَّكَاءِ الاصْطِنَاعِيّ. تَمَّ إِرْفَاقُ خِطَابِ ضَمَانِ حُسْنِ تَنْفِيذٍ بِقِيمَةِ أَرْبَعَةِ مَلَايِينَ وَخَمْسِمِائَةِ أَلْفِ جُنَيْهٍ مِصْرِيّ، صَادِرٍ مِنَ البَنْكِ الوَطَنِيِّ لِتَمْوِيلِ الشَّرِكَاتْ. بِضَغْطَةٍ وَاحِدَةٍ عَلَى زِرِّ المَسْحِ الآنْ، يَبْدَأُ المُحَرِّكُ الذَّكِيُّ قِرَاءَةَ المُسْتَنَدِ وَتَحْلِيلَهْ.',
        },
        3: {
            "en": 'Step Three: Every Core Field and Contract Purpose Populated Automatically. In seconds, Grow populates the Beneficiary Entity, Applicant NorthStar Electromechanical, LG Number, Amount, Currency, Issue and Expiry Dates, LG Type, and the full underlying contract purpose. Click the Bank Data section below to review the extracted bank terms.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَعْبِئَةُ كَافَّةِ البَيَانَاتِ الأَسَاسِيَّةِ وَغَرَضِ التَّعَاقُدِ تِلْقَائِيّاً. خِلَالَ ثَوَانٍ، يَمْلَأُ النِّظَامُ اسْمَ الشَّرِكَةِ المُسْتَفِيدَةِ وَالمُقَاوِلْ، وَرَقْمَ الضَّمَانِ وَالمَبْلَغَ وَالعُمْلَة، وَتَوَارِيخَ الإِصْدَارِ وَالانْتِهَاءِ وَنَوْعَ الضَّمَانِ وَالغَرَضَ التَّعَاقُدِيَّ بِالكَامِلْ. اضْغَطْ عَلَى قِسْمِ بَيَانَاتِ البَنْكِ بِالأَسْفَلِ لِلْمُرَاجَعَة.',
        },
        4: {
            "en": 'Step Four: Auto-Matched Bank Details plus Smart Claim Intelligence. Opening Bank Data shows First National Corporate Bank, its address, hotline, and URDG seven fifty-eight rule already matched. Below, the amber Smart Claim box captures the exact mandatory declaration required if you ever liquidate this guarantee. Click Internal Data to assign governance ownership.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُطَابَقَةُ بَيَانَاتِ البَنْكِ وَكَشْفُ الصِّيَاغَاتِ المَشْرُوطَةِ لِلْمُصَادَرَة. عِنْدَ فَتْحِ بَيَانَاتِ البَنْكْ، تَظْهَرُ بَيَانَاتُ البَنْكِ المُصْدِرِ وَعُنْوَانُهُ وَقَوَاعِدُ التَّغْطِيَةِ الدَّوْلِيَّةُ جَاهِزَةً تِلْقَائِيّاً، مَعَ اسْتِخْرَاجِ جُمْلَةِ الإِقْرَارِ الإِلْزَامِيِّ المَطْلُوبَةِ عِنْدَ التَّسْيِيلِ دَاخِلَ بِطَاقَةِ التَّنْبِيهِ الذَّهَبِيَّةِ لِحِمَايَةِ حَقِّ الشَّرِكَة. افْتَحْ قِسْمَ البَيَانَاتِ الدَّاخِلِيَّةِ لِلْمُتَابَعَة.',
        },
        5: {
            "en": 'Step Five: Bind Project Owner, Proactive Reminders, and Escalation Chain. Selecting the project owner automatically links their contact details and line manager. This activates multi-stage expiry reminders and automatic management escalations if renewal decisions are ever delayed. Click Save LG Record to lock the guarantee into custody.',
            "ar": 'الخُطْوَةُ الخَامِسَة: رَبْطُ مَسْؤُولِ المَشْرُوعِ وَتَفْعِيلُ التَّنْبِيهَاتِ المُبَكِّرَةِ وَسِلْسِلَةِ التَّصْعِيدْ. بِمُجَرَّدِ اخْتِيَارِ مَسْؤُولِ المَشْرُوعْ، تُرْبَطُ بَيَانَاتُهُ وَمُدِيرُهُ المُبَاشِرُ تِلْقَائِيّاً، لِتَفْعِيلِ التَّنْبِيهَاتِ الاسْتِبَاقِيَّةِ قَبْلَ الانْتِهَاءْ، وَالتَّصْعِيدِ الآلِيِّ لِلْإِدَارَةِ وَمَسْؤُولِي النِّظَامِ عِنْدَ تَأَخُّرِ إِجْرَاءِ التَّجْدِيدْ. اضْغَطْ زِرَّ الحِفْظِ لِتَسْجِيلِ الضَّمَانِ فِي المَحْفَظَة.',
        },
        6: {
            "en": "Step Six: Active in Custody with Automated Expiry and Bank-Reply Monitoring. The guarantee is now indexed in Manage LG Records with a unique Custody Serial. Grow's daily radar continuously monitors upcoming maturities, print and delivery reminders, and overdue bank replies before rights lapse. Click the View icon on the right of the row to open its full Custody Cockpit.",
            "ar": 'الخُطْوَةُ السَّادِسَة: إِدْرَاجٌ مُشَفَّرٌ فِي المَحْفَظَةِ تَحْتَ مُرَاقَبَةِ الاسْتِحْقَاقِ وَرُدُودِ البُنُوكْ. يَظْهَرُ الضَّمَانُ فَوْراً فِي سِجِلِّ الحِفْظِ بِرَقْمٍ تَسَلْسُلِيٍّ فَرِيدٍ تَحْتَ المُرَاقَبَةِ اليَوْمِيَّةِ الآلِيَّة، لِمَوَاعِيدِ الاسْتِحْقَاقِ وَتَذْكِيرَاتِ الطِّبَاعَةِ وَالتَّسْلِيمِ وَتَأَخُّرِ رُدُودِ البُنُوكِ قَبْلَ سُقُوطِ أَيِّ حَقٍّ مَالِيّ. اضْغَطْ أَيْقُونَةَ العَرْضِ لِفَتْحِ مَلَفِّ الضَّمَانْ.',
        },
        7: {
            "en": 'Step Seven: Full Lifecycle Control, Direct Extensions, Maker-Checker, and QR Letters. Inside the LG Cockpit, treasurers see the live validity countdown, encrypted scan, and one-click lifecycle actions. Safe, protective extensions execute directly without delay, while Release, Decrease, Amendment, and Liquidation enforce strict Maker-Checker approval, QR-verified bank letters, and delivery or reply escalations.',
            "ar": 'الخُطْوَةُ السَّابِعَة: تَحَكُّمٌ كَامِلٌ بِدَوْرَةِ الحَيَاة، تَمْدِيدٌ مُبَاشِرْ، وَاعْتِمَادٌ مُزْدَوَجْ، وَخِطَابَاتٌ مُوَثَّقَة. دَاخِلَ لَوْحَةِ الضَّمَانِ يَظْهَرُ عَدَّادُ الصَّلَاحِيَّةِ وَالمُسْتَنَدُ المُشَفَّرُ وَإِجْرَاءَاتُ دَوْرَةِ الحَيَاة. حَيْثُ يُنَفَّذُ التَّمْدِيدُ مُبَاشَرَةً دُونَ تَأْخِيرٍ لِحِمَايَةِ حَقِّ الشَّرِكَة، بَيْنَمَا يَخْضَعُ الإِفْرَاجُ وَالتَّخْفِيضُ وَالتَّعْدِيلُ وَالتَّسْيِيلُ لِاعْتِمَادِ المُرَاجِعِ وَالمُعْتَمِدْ، وَخِطَابَاتِ كيو آرْ المُوَثَّقَةِ وَتَصْعِيدِ التَّأْخِيرْ.',
        },
    },
    'custody_action_center': {
        1: {
            "en": 'Step One: One Proactive Action Center Replacing Spreadsheets and Missed Deadlines. Instead of static Excel trackers, Grow unifies four live operational queues: Pending Prints, Due Renewals, Awaiting Delivery, and Overdue Bank Replies. Automated multi-stage alerts notify project owners ahead of expiry and escalate delayed decisions to line managers and Corporate Admins. Click Auto Renewal to preview a consolidated renewal run.',
            "ar": 'الخُطْوَةُ الأُولَى: مَرْكَزُ مَهَامَّ اسْتِبَاقِيٌّ مُوَحَّدٌ بَدَلاً مِنْ جَدَاوِلِ الإِكْسِيلْ وَمَخَاطِرِ السَّهْوْ. بَدَلاً مِنَ المُلَاحَقَةِ اليَدَوِيَّة، تَجْمَعُ المِنَصَّةُ أَرْبَعَ قَوَائِمَ تَشْغِيلِيَّةٍ حَيَّة: خِطَابَاتٌ بِانْتِظَارِ الطِّبَاعَة، وَضَمَانَاتٌ مُسْتَحِقَّةُ التَّجْدِيدْ، وَتَعْلِيمَاتٌ بِانْتِظَارِ التَّسْلِيمْ، وَرُدُودُ بُنُوكٍ مُتَأَخِّرَة، مَعَ إِشْعَارَاتٍ مُبَكِّرَةٍ لِمَسْؤُولِي المَشَارِيعِ وَتَصْعِيدٍ تِلْقَائِيٍّ لِلْمُدِيرِينَ عِنْدَ التَّأَخُّرْ. اضْغَطْ زِرَّ التَّجْدِيدِ التِّلْقَائِيِّ لِمُعَايَنَةِ دُفْعَةِ التَّمْدِيدْ.',
        },
        2: {
            "en": 'Step Two: Bulk Auto-Renewal Engine with Mandatory Cutoff Protection. Grow automatically identifies eligible expiring guarantees, distinguishes urgent mandatory renewals near cutoff from proactive renewals, calculates suggested new expiry dates, and compiles a single print-ready PDF batch with full reprint history. Click Confirm and Extend to inspect fast-track single extension.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: مُحَرِّكُ التَّجْدِيدِ الجَمَاعِيِّ مَعَ حِمَايَةِ الضَّمَانَاتِ الحَرِجَة. يَفْرِزُ النِّظَامُ تِلْقَائِيّاً الضَّمَانَاتِ المُقْتَرِبَةَ مِنَ الانْتِهَاءْ، وَيُمَيِّزُ التَّجْدِيدَاتِ الإِلْزَامِيَّةَ العَاجِلَةَ عَنِ التَّجْدِيدَاتِ الاسْتِبَاقِيَّة، مَعَ حِسَابِ تَوَارِيخِ الصَّلَاحِيَّةِ الجَدِيدَةِ وَإِصْدَارِ مَلَفِّ بي دي إفْ مُجَمَّعٍ قَابِلٍ لِإِعَادَةِ الطِّبَاعَةِ فِي أَيِّ وَقْتْ. اضْغَطْ زِرَّ التَّأْكِيدِ وَالتَّمْدِيدِ لِمُتَابَعَةِ التَّمْدِيدِ الفَرْدِيِّ السَّرِيعْ.',
        },
        3: {
            "en": 'Step Three: Fast-Track Protective Extension with Zero Maker-Checker Delay. Because extending a vendor guarantee protects your company without releasing security, Grow executes Custody extensions directly without waiting for Maker-Checker approval and immediately generates a QR-sealed bank extension letter. Click Extend LG to issue the instruction immediately.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَمْدِيدٌ حِمَائِيٌّ فَوْرِيٌّ بِدُونِ انْتِظَارِ دَوْرَةِ اعْتِمَادْ. لِأَنَّ تَمْدِيدَ خِطَابِ الضَّمَانِ الوَارِدِ يَحْمِي حَقَّ الشَّرِكَةِ المَالِيَّ دُونَ التَّنَازُلِ عَنْ أَيِّ غِطَاءْ، تُنَفِّذُ المِنَصَّةُ التَّمْدِيدَ مُبَاشَرَةً دُونَ تَعْطِيلٍ فِي طَوَابِيرِ الاعْتِمَادْ، وَتُصْدِرُ فَوْراً خِطَابَ تَمْدِيدٍ بَنْكِيّاً مَخْتُوماً بِرَمْزِ كيو آرْ. اضْغَطْ زِرَّ تَمْدِيدِ الضَّمَانِ لِإِصْدَارِ الخِطَابِ فَوْراً.',
        },
        4: {
            "en": 'Step Four: Live Radar for Undelivered Letters and Overdue Bank Replies. Issuing a bank letter is only half the job. Action Center tracks every instruction awaiting courier delivery to the branch and counts exact days pending whenever a bank delays its written confirmation. Click Bulk Reminders to chase overdue bank confirmations.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: رَادَارٌ حَيٌّ لِلتَّعْلِيمَاتِ غَيْرِ المُسَلَّمَةِ وَرُدُودِ البُنُوكِ المُتَأَخِّرَة. إِصْدَارُ الخِطَابِ لَيْسَ نِهَايَةَ المَسْؤُولِيَّة؛ فَالنِّظَامُ يَرْصُدُ كُلَّ خِطَابٍ لَمْ يُسَلَّمْ بَعْدُ لِفَرْعِ البَنْكْ، وَيَحْسِبُ أَيَّامَ التَّأْخِيرِ بِدِقَّةٍ عِنْدَ تَأَخُّرِ البَنْكِ فِي إِرْسَالِ إِشْعَارِ التَّأْكِيدِ الرَّسْمِيّ. اضْغَطْ زِرَّ الِاسْتِعْجَالَاتِ الجَمَاعِيَّةِ لِمُتَابَعَةِ البُنُوكِ المُتَأَخِّرَة.',
        },
        5: {
            "en": 'Step Five: One-Click Official Bank Chaser Letters. With a single click, Grow identifies every instruction exceeding your configured bank-reply threshold and generates formal, QR-verified Bank Reminder Letters ready to print and dispatch, ensuring no instruction is ever forgotten at a bank counter.',
            "ar": 'الخُطْوَةُ الخَامِسَة: إِصْدَارُ خِطَابَاتِ اسْتِعْجَالٍ رَسْمِيَّةٍ لِلْبُنُوكِ بِضَغْطَةٍ وَاحِدَة. بِضَغْطَةِ زِرٍّ وَاحِدَة، يَحْصُرُ النِّظَامُ كَافَّةَ التَّعْلِيمَاتِ الَّتِي تَجَاوَزَتْ مُهْلَةَ الرَّدِّ المَسْمُوحَة، وَيُصْدِرُ خِطَابَاتِ اسْتِعْجَالٍ مَصْرِفِيَّةً رَسْمِيَّةً مُوَثَّقَةً بِرَمْزِ كيو آرْ جَاهِزَةً لِلطِّبَاعَةِ وَالإِرْسَالْ، لِضَمَانِ عَدَمِ نِسْيَانِ أَيِّ مُعَامَلَةٍ لَدَى البَنْكْ.',
        },
    },
    'custody_lifecycle': {
        1: {
            "en": "Step One: AI Beneficiary Protection Check and Governed Lifecycle Cockpit. Inside the guarantee cockpit, Grow's AI Beneficiary Protection Check audits incoming Arabic or English guarantee wording to confirm unconditional payment rights and warn you early against hidden claim hurdles. Above, the lifecycle bar governs Amendments, Amount Reductions, Releases, and Liquidations. Click Decrease Amount to initiate a milestone reduction.",
            "ar": 'الخُطْوَةُ الأُولَى: فَحْصُ حِمَايَةِ المُسْتَفِيدِ بِالذَّكَاءِ الاصْطِنَاعِيِّ وَلَوْحَةُ حَوْكَمَةِ الضَّمَانْ. دَاخِلَ مَلَفِّ الضَّمَانْ، يَفْحَصُ الذَّكَاءُ الاصْطِنَاعِيُّ صِيَاغَةَ الضَّمَانِ الوَارِدِ بِالعَرَبِيَّةِ أَوِ الإِنْجِلِيزِيَّة، لِلتَّأَكُّدِ مِنْ وُجُودِ تَعَهُّدِ دَفْعٍ غَيْرِ مَشْرُوطٍ عِنْدَ أَوَّلِ طَلَبْ، وَالتَّحْذِيرِ المُبَكِّرِ مِنْ أَيِّ شُرُوطٍ قَدْ تُعِيقُ المُطَالَبَة. اضْغَطْ زِرَّ تَخْفِيضِ المَبْلَغِ بِالأَعْلَى لِبَدْءِ إِجْرَاءِ تَخْفِيضٍ مَرْحَلِيّ.',
        },
        2: {
            "en": 'Step Two: Risk-Smart Governance and Supporting Evidence Enforcement. Unlike extensions, reducing, releasing, amending, or liquidating a guarantee lowers your financial protection, and on liquidation, Grow even pre-injects the mandatory claim wording automatically. Here, reducing a four point five million pound guarantee by one point five million attaches the signed Milestone Certificate and routes strictly to Maker-Checker approval. Click Submit Decrease Request.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: حَوْكَمَةٌ ذَكِيَّةٌ حَسَبَ المَخَاطِرِ وَإِلْزَامِيَّةُ المُسْتَنَدَاتِ المُؤَيِّدَة. عَلَى عَكْسِ التَّمْدِيدْ، فَإِنَّ التَّخْفِيضَ أَوِ الإِفْرَاجَ أَوِ التَّعْدِيلَ أَوِ التَّسْيِيلَ يُؤَثِّرُ عَلَى الغِطَاءِ المَالِيِّ لِلشَّرِكَة، كَمَا يَقُومُ النِّظَامُ عِنْدَ التَّسْيِيلِ بِإِدْرَاجِ نَصِّ المُطَالَبَةِ الإِلْزَامِيِّ تِلْقَائِيّاً. هُنَا نُرْفِقُ شَهَادَةَ الإِنْجَازِ المَرْحَلِيِّ لِتَخْفِيضِ الضَّمَانِ إِلَى ثَلَاثَةِ مَلَايِينِ جُنَيْهْ، وَإِحَالَةِ الطَّلَبِ لِدَوْرَةِ الاعْتِمَادْ. اضْغَطْ إِرْسَالَ الطَّلَبْ.',
        },
        3: {
            "en": 'Step Three: Segregation of Duties in the Corporate Admin Approval Queue. Self-approval is blocked at the database level. Logging in as Corporate Admin reveals the pending Decrease Amount request in the Approval Center, complete with Maker identity, submission timestamp, and escalation tracking. Click the View icon to inspect the impact comparison.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: فَصْلٌ تَامٌّ فِي الصَّلَاحِيَّاتِ دَاخِلَ طَابُورِ اعْتِمَادَاتِ مَسْؤُولِ الشَّرِكَة. يَمْنَعُ النِّظَامُ الاعْتِمَادَ الذَّاتِيَّ بِنَاءً عَلَى قَوَاعِدِ الحَوْكَمَةِ الصَّارِمَة؛ فَعِنْدَ الدُّخُولِ بِحِسَابِ المُعْتَمِدِ يَظْهَرُ طَلَبُ التَّخْفِيضِ مُعَلَّقاً مَعَ بَيَانَاتِ مُقَدِّمِ الطَّلَبِ وَتَوْقِيتِ التَّقْدِيمْ. اضْغَطْ أَيْقُونَةَ الفَحْصِ لِمُرَاجَعَةِ تَفَاصِيلِ التَّغْيِيرْ.',
        },
        4: {
            "en": "Step Four: Full Request Details and Snapshot Integrity Check Before Approval. The approver sees the exact one point five million pound decrease request, the Maker's contractual reason, one-click access to the attached Milestone Certificate, and a live Snapshot versus Current integrity grid confirming no guarantee fields drifted since submission. Click Approve to authorize the reduction and generate the bank letter.",
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُرَاجَعَةُ تَفَاصِيلِ التَّخْفِيضِ وَسَلَامَةِ البَيَانَاتِ قَبْلَ الاعْتِمَادِ النِّهَائِيّ. يُعَايِنُ المُعْتَمِدُ تَفَاصِيلَ تَخْفِيضِ الضَّمَانِ بِمَبْلَغِ مِلْيُونٍ وَنِصْفِ جُنَيْهٍ مِصْرِيّ، مَعَ مُبَرِّرَاتِ الطَّلَبِ وَرَابِطِ فَتْحِ شَهَادَةِ الإِنْجَازِ المُرْفَقَة، وَجَدْوَلِ مُقَارَنَةِ حَالَةِ السِّجِلِّ لِلتَّأَكُّدِ مِنْ عَدَمِ تَغَيُّرِ أَيِّ بَيَانَاتٍ مُنْذُ التَّقْدِيمْ. اضْغَطْ زِرَّ الاعْتِمَادِ لِإِصْدَارِ خِطَابِ البَنْكِ الرَّسْمِيّ.',
        },
        5: {
            "en": 'Step Five: Tamper-Evident Bank Instruction Letter with QR and Split Security Code. Upon approval, Grow generates the formal corporate instruction letter addressed to First National Corporate Bank, automatically injecting a non-bypassable security footer with a digitally signed QR seal and a four-digit Security Code. Click the QR verification footer to see what the receiving bank officer experiences.',
            "ar": 'الخُطْوَةُ الخَامِسَة: خِطَابُ تَعْلِيمَاتٍ بَنْكِيٌّ مُؤَمَّنٌ ضِدَّ التَّزْوِيرِ بِرَمْزِ كيو آرْ وَكُودِ أَمَانْ. فَوْرَ الاعْتِمَادْ، يُصْدِرُ النِّظَامُ خِطَابَ التَّعْلِيمَاتِ الرَّسْمِيَّ المُوَجَّهَ لِلْبَنْكْ، مَخْتُوماً بِشَرِيطِ أَمَانٍ إِجْبَارِيٍّ يَحْتَوِي عَلَى رَمْزِ كيو آرْ مُوَقَّعٍ رَقْمِيّاً وَكُودِ أَمَانٍ رُبَاعِيّ. اضْغَطْ عَلَى شَرِيطِ التَّحَقُّقِ بِأَسْفَلِ الخِطَابِ لِمُشَاهَدَةِ تَجْرِبَةِ مُوَظَّفِ البَنْكْ.',
        },
        6: {
            "en": "Step Six: Instant Public Bank Authenticity Verification Portal. Scanning the QR code opens Grow's public verification portal. To defeat cut-and-paste tampering, the bank officer enters the last two characters of the LG number from the letter body, plus the four-digit Security Code from the footer, unlocking a green Verified Genuine Corporate Instruction certificate in seconds.",
            "ar": 'الخُطْوَةُ السَّادِسَة: بَوَّابَةُ التَّحَقُّقِ الفَوْرِيِّ لِمُوَظَّفِي البُنُوكِ ضِدَّ أَيِّ تَلَاعُبٍ وَرَقِيّ. عِنْدَ مَسْحِ رَمْزِ كيو آرْ، تَفْتَحُ بَوَّابَةُ التَّحَقُّقِ العَامَّةُ وَتَطْلُبُ إِدْخَالَ آخِرِ حَرْفَيْنِ مِنْ رَقْمِ الضَّمَانِ بِمَتْنِ الخِطَابِ مَعَ كُودِ الأَمَانِ الرُّبَاعِيِّ بِالتَّذْيِيلْ، لِمَنْعِ أَيِّ قَصٍّ أَوْ تَزْوِيرْ، وَإِظْهَارِ شَهَادَةٍ خَضْرَاءَ تُؤَكِّدُ صِحَّةَ التَّعْلِيمَاتِ وَالمَبْلَغِ المُعْتَمَدِ فَوْراً.',
        },
    },
    'custody_delivery_reply': {
        1: {
            "en": 'Step One: Real-Time In-Flight Instruction Tracker Inside Every LG Cockpit. Once an instruction letter is issued, an amber In-Flight banner stays pinned at the top of the guarantee file, clearly showing Awaiting Bank Delivery so printed letters never disappear in transit. Click Record Delivery to upload the bank-stamped receipt.',
            "ar": 'الخُطْوَةُ الأُولَى: شَرِيطُ تَتَبُّعٍ حَيٌّ لِلتَّعْلِيمَاتِ الجَارِيَةِ دَاخِلَ مَلَفِّ الضَّمَانْ. بِمُجَرَّدِ إِصْدَارِ خِطَابِ التَّعْلِيمَاتْ، يَظْهَرُ شَرِيطُ تَنْبِيهٍ حَيٌّ بِأَعْلَى مَلَفِّ الضَّمَانِ يُوَضِّحُ أَنَّ الخِطَابَ بِانْتِظَارِ التَّسْلِيمِ لِلْبَنْكْ، لِضَمَانِ عَدَمِ ضَيَاعِ أَيِّ خِطَابٍ بَعْدَ طِبَاعَتِهْ. اضْغَطْ زِرَّ تَسْجِيلِ التَّسْلِيمِ لِرَفْعِ صُورَةِ الخِطَابِ المَخْتُومْ.',
        },
        2: {
            "en": "Step Two: AI Bank Receiving Stamp Audit and One-Click Date Alignment. When your team uploads the stamped delivery copy, Grow's AI verifies duplicate scan hashes, LG number, bank identity, and the physical teller receiving stamp. Notice how AI spotted that the bank's receiving stamp is dated yesterday, while the user typed today, offering a one-click Align Date button. Click Align Date to confirm accurate delivery.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: فَحْصُ خَتْمِ اسْتِلَامِ البَنْكِ بِالذَّكَاءِ الاصْطِنَاعِيِّ وَمُطَابَقَةُ التَّارِيخِ بِضَغْطَةٍ وَاحِدَة. عِنْدَ رَفْعِ الصُّورَةِ المَخْتُومَة، يَتَحَقَّقُ الذَّكَاءُ الاصْطِنَاعِيُّ مِنْ عَدَمِ تَكْرَارِ المَلَفّ، وَمُطَابَقَةِ رَقْمِ الضَّمَانِ وَاسْمِ البَنْكِ وَوُجُودِ خَتْمِ الوَارِدِ الفِعْلِيّ. لَاحِظْ كَيْفَ اكْتَشَفَ النِّظَامُ أَنَّ تَارِيخَ خَتْمِ الفَرْعِ هُوَ أَمْسِ بَيْنَمَا أَدْخَلَ المُسْتَخْدِمُ تَارِيخَ اليَوْمْ، وَأَتَاحَ زِرَّ مُطَابَقَةِ التَّارِيخِ بِضَغْطَةٍ وَاحِدَة. اضْغَطْ مُطَابَقَةَ التَّارِيخِ لِلتَّأْكِيدْ.',
        },
        3: {
            "en": "Step Three: Status Advances to Delivered, Awaiting Bank Reply. With physical bank delivery verified and sealed, the cockpit banner automatically transitions to Delivered, Awaiting Bank Reply, activating automated follow-up timers until the bank's official advice arrives. Click Record Bank Reply to verify the incoming bank confirmation.",
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَحْدِيثٌ فَوْرِيٌّ لِلْحَالَةِ إِلَى تَمَّ التَّسْلِيمُ وَبِانْتِظَارِ رَدِّ البَنْكْ. بَعْدَ تَوْثِيقِ خَتْمِ الاسْتِلَامِ البَنْكِيّ، يَتَحَوَّلُ شَرِيطُ التَّتَبُّعِ تِلْقَائِيّاً إِلَى تَمَّ التَّسْلِيمُ وَبِانْتِظَارِ رَدِّ البَنْكْ، مَعَ تَفْعِيلِ مُؤَقِّتَاتِ المُتَابَعَةِ الآلِيَّةِ حَتَّى وُصُولِ إِشْعَارِ البَنْكِ الرَّسْمِيّ. اضْغَطْ زِرَّ تَسْجِيلِ رَدِّ البَنْكِ لِرَفْعِ الإِشْعَارِ الوَارِدْ.',
        },
        4: {
            "en": "Step Four: AI Bank Reply Cross-Check and Calendar Convention Intelligence. Uploading First National Corporate Bank's extension advice triggers deep AI cross-verification, matching LG number, branch seal, instruction serial, and amended terms. When the bank adjusts expiry by two days for business-day convention, Grow flags a friendly Calendar Variance, while routing major bank term changes to Corporate Admin approval. Click Inspect Scan to view the split auditor.",
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُطَابَقَةُ رَدِّ البَنْكِ بِالذَّكَاءِ الاصْطِنَاعِيِّ وَفَهْمُ فُرُوقِ التَّقْوِيمِ المَصْرِفِيّ. فَوْرَ رَفْعِ إِشْعَارِ البَنْكِ الرَّسْمِيّ، يُطَابِقُ الذَّكَاءُ الاصْطِنَاعِيُّ رَقْمَ الضَّمَانِ وَخَتْمَ الفَرْعِ وَمَرْجِعَ التَّعْلِيمَاتِ وَشُرُوطَ التَّمْدِيدْ؛ وَعِنْدَ تَعْدِيلِ البَنْكِ لِلتَّارِيخِ بِيَوْمَيْنِ وِفْقَ أَيَّامِ العَمَلِ المَصْرِفِيَّة، يُصَنِّفُهُ النِّظَامُ كَفَرْقِ تَقْوِيمٍ طَبِيعِيّ، بَيْنَمَا يُحِيلُ التَّغْيِيرَاتِ الجَوْهَرِيَّةَ لِاعْتِمَادِ مَسْؤُولِ الشَّرِكَة. اضْغَطْ فَحْصَ المُسْتَنَدْ.',
        },
        5: {
            "en": "Step Five: Side-by-Side Document and AI Audit Inspector. The split-screen inspector places the scanned bank advice right beside every AI-verified check and extracted date, giving treasury teams complete visual confidence in seconds. Click Accept and Align to update the guarantee's official expiry date to the bank's confirmed date.",
            "ar": 'الخُطْوَةُ الخَامِسَة: عَارِضُ الفَحْصِ المُزْدَوَجِ لِلْمُسْتَنَدِ وَنَتَائِجِ الذَّكَاءِ الاصْطِنَاعِيّ. تَعْرِضُ نَافِذَةُ الفَحْصِ المُزْدَوَجَةُ صُورَةَ إِشْعَارِ البَنْكِ جَنْباً إِلَى جَنْبٍ مَعَ كَافَّةِ نِقَاطِ التَّحَقُّقِ وَالبَيَانَاتِ المُسْتَخْرَجَة، لِمَنْحِ فَرِيقِ الخِزَانَةِ ثِقَةً بَصَرِيَّةً وَرَقَابِيَّةً كَامِلَةً خِلَالَ ثَوَانٍ. اضْغَطْ قَبُولَ وَمُطَابَقَةَ التَّارِيخِ لِاعْتِمَادِ تَارِيخِ الانْتِهَاءِ المُؤَكَّدِ مِنَ البَنْكْ.',
        },
        6: {
            "en": 'Step Six: Closed-Loop Lifecycle History and Auditor-Ready Vault. The instruction is now marked Confirmed by Bank, the guarantee validity is updated automatically, and Lifecycle History links the QR Instruction Letter, Stamped Delivery Receipt, and Verified Bank Advice in one encrypted timeline, completing the end-to-end LG Custody journey.',
            "ar": 'الخُطْوَةُ السَّادِسَة: إِغْلَاقُ الدَّوْرَةِ الرَّقَابِيَّةِ وَأَرْشَفَةُ كَامِلِ المُسْتَنَدَاتِ فِي السِّجِلِّ التَّارِيخِيّ. تَتَحَوَّلُ حَالَةُ التَّعْلِيمَاتِ إِلَى مُؤَكَّدٍ مِنَ البَنْكْ، وَيَتِمُّ تَحْدِيثُ صَلَاحِيَّةِ الضَّمَانِ تِلْقَائِيّاً، مَعَ رَبْطِ خِطَابِ التَّعْلِيمَاتِ وَإِيصَالِ التَّسْلِيمِ المَخْتُومِ وَرَدِّ البَنْكِ المُوَثَّقِ فِي سِجِلٍّ زَمَنِيٍّ مُشَفَّرٍ وَجَاهِزٍ لِلْمُرَاجَعَة.',
        },
    },
    'issuance_intake': {
        1: {
            "en": 'Step One: Tokenized Department Portal for Outgoing Guarantees. Unlike Custody, which safeguards incoming supplier guarantees, Outgoing Issuance begins when your own project or tender team needs your bank to issue a corporate guarantee in favor of an external client. Business units submit and track requests through a secure portal link without email chaos. Click New Request to launch the wizard.',
            "ar": 'الخُطْوَةُ الأُولَى: بَوَّابَةٌ رَقْمِيَّةٌ لِلْإِدَارَاتِ لِطَلَبِ خِطَابَاتِ الضَّمَانِ الصَّادِرَة. عَلَى عَكْسِ وَحْدَةِ الحِفْظِ المَسْؤُولَةِ عَنْ ضَمَانَاتِ المُوَرِّدِينَ الوَارِدَة، تَبْدَأُ وَحْدَةُ الإِصْدَارِ عِنْدَمَا تَحْتَاجُ إِدَارَةُ المَشْرُوعَاتِ لِإِصْدَارِ خِطَابِ ضَمَانٍ مِنْ تَسْهِيلَاتِ الشَّرِكَةِ البَنْكِيَّةِ لِصَالِحِ جِهَةٍ عَمِيلَة. تَرْفَعُ الإِدَارَةُ طَلَبَهَا وَتُتَابِعُهُ عَبْرَ بَوَّابَةٍ مُؤَمَّنَةٍ دُونَ فَوْضَى البَرِيدِ الإِلِكْتْرُونِيّ. اضْغَطْ طَلَبٌ جَدِيدٌ لِلْبَدْءْ.',
        },
        2: {
            "en": "Step Two: Structured Four-Step Outgoing Guarantee Wizard. The guided form captures the external beneficiary authority, contract award reference, guarantee type, twelve million Egyptian Pound amount, validity period, and attaches the underlying contract or custom bank wording. Click Next to send the request into Treasury's inbox.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: مُعَالِجٌ مُنَظَّمٌ مِنْ أَرْبَعِ خُطُوَاتٍ لِبَيَانَاتِ المُسْتَفِيدِ وَالعَقْدْ. يُوَجِّهُ المُعَالِجُ طَالِبَ الإِصْدَارِ لِإِدْخَالِ اسْمِ الجِهَةِ المُسْتَفِيدَة، وَرَقْمِ العَقْدِ وَنَوْعِ الضَّمَانِ الصَّادِرِ وَالمَبْلَغِ وَالمُدَّة، مَعَ إِرْفَاقِ خِطَابِ التَّرْسِيَةِ أَوِ الصِّيَاغَةِ الخَاصَّة. اضْغَطِ التَّالِي لِإِرْسَالِ الطَّلَبِ لِصُنْدُوقِ الخِزَانَة.',
        },
        3: {
            "en": "Step Three: AI Contract Award Verification. Inside Treasury's inbox, clicking Verified opens the AI document audit, confirming an eight out of eight match between the uploaded contract award PDF and the requested Advance Payment Guarantee before any bank credit line is touched.",
            "ar": 'الخُطْوَةُ الثَّالِثَة: مُطَابَقَةُ خِطَابِ التَّرْسِيَةِ بِالذَّكَاءِ الاصْطِنَاعِيِّ مَعَ بَيَانَاتِ الطَّلَبْ. دَاخِلَ صُنْدُوقِ الخِزَانَة، يَعْرِضُ الفَحْصُ الذَّكِيُّ تَطَابُقَ ثَمَانِيَةٍ مِنْ ثَمَانِيَةِ بُنُودٍ بَيْنَ خِطَابِ التَّرْسِيَةِ المُرْفَقِ وَمَبْلَغِ وَشُرُوطِ الضَّمَانِ المَطْلُوبْ، قَبْلَ اسْتِهْلَاكِ أَيِّ جُزْءٍ مِنَ التَّسْهِيلَاتِ الائْتِمَانِيَّة.',
        },
        4: {
            "en": 'Step Four: Weighted Duplicate Request Guardrail. Before approval, Grow automatically compares the request against existing outgoing guarantees on the same contract and beneficiary, flagging near-matches immediately so your company never pays double commissions or locks bank limits twice by mistake.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: حَاجِزٌ ذَكِيٌّ لِكَشْفِ التَّكْرَارِ وَمَنْعِ ازْدِوَاجِ العُمُولَاتِ البَنْكِيَّة. قَبْلَ الاعْتِمَادْ، يُقَارِنُ النِّظَامُ الطَّلَبَ تِلْقَائِيّاً بِالضَّمَانَاتِ الصَّادِرَةِ القَائِمَةِ عَلَى نَفْسِ العَقْدِ وَالمُسْتَفِيدْ، لِمَنْعِ تَكْرَارِ الإِصْدَارِ بِالخَطَأِ أَوْ حَجْزِ حُدُودِ التَّسْهِيلِ مَرَّتَيْنْ.',
        },
        5: {
            "en": 'Step Five: Applicant Special-Wording Trap Scanner. When an external beneficiary demands custom guarantee text, Grow scans the draft from your risk perspective as Applicant. Here it flags a high-risk score of seventy-eight out of one hundred for a missing Non-Operative advance-receipt clause and an Extend-or-Pay trigger, offering protective rewording ready to negotiate.',
            "ar": 'الخُطْوَةُ الخَامِسَة: كَشْفُ شُرُوطِ المُسْتَفِيدِ الخَطِرَةِ وَاقْتِرَاحُ صِيَاغَةٍ بَدِيلَةٍ تَحْمِي الشَّرِكَة. عِنْدَ اشْتِرَاطِ العَمِيلِ صِيَاغَةً خَاصَّة، يَفْحَصُهَا الذَّكَاءُ الاصْطِنَاعِيُّ مِنْ مَنْظُورِ حِمَايَةِ شَرِكَتِكَ كَطَالِبِ إِصْدَارْ، كَاشِفاً خُطُورَةَ غِيَابِ شَرْطِ عَدَمِ السَّرَيَانِ لِحِينِ دُخُولِ الدُّفْعَةِ المُقَدَّمَة، مَعَ تَوْلِيدِ نَصٍّ بَدِيلٍ يَحْمِي الشَّرِكَة.',
        },
        6: {
            "en": 'Step Six: Sequential Credit Approval Matrix. Because every outgoing guarantee commits corporate credit facilities and bank fees, Grow routes the request through your sequential approval matrix, blocking self-approval and escalating delayed reviews. Click Approve to proceed to Smart Facility Scoring.',
            "ar": 'الخُطْوَةُ السَّادِسَة: مَصْفُوفَةُ المَوَافَقَاتِ الائْتِمَانِيَّةِ المُتَدَرِّجَة. لِأَنَّ كُلَّ ضَمَانٍ صَادِرٍ جَدِيدٍ يُرَتِّبُ الْتِزَاماً ائْتِمَانِيّاً وَعُمُولَاتٍ بَنْكِيَّةً عَلَى الشَّرِكَة، يُطَبِّقُ النِّظَامُ مَسَارَ اعْتِمَادٍ مُتَدَرِّجٍ حَسَبَ المَبْلَغِ وَالإِدَارَةِ مَعَ مَنْعِ الاعْتِمَادِ الذَّاتِيّ. اضْغَطِ اعْتِمَادْ لِلانْتِقَالِ إِلَى تَقْيِيمِ التَّسْهِيلَاتِ البَنْكِيَّة.',
        },
    },
    'issuance_scoring': {
        1: {
            "en": 'Step One: Live Multi-Bank Credit Umbrella and Sub-Limit Visibility. Grow tracks all corporate guarantee facilities across your relationship banks, monitoring utilized limits, cash margin relief, and foreign-currency buffers in real time.',
            "ar": 'الخُطْوَةُ الأُولَى: رُؤْيَةٌ لَحْظِيَّةٌ لِمَحَافِظِ التَّسْهِيلَاتِ الائْتِمَانِيَّةِ وَالحُدُودِ الفَرْعِيَّةِ بِالبُنُوكْ. تُتَابِعُ المِنَصَّةُ كَافَّةَ عُقُودِ التَّسْهِيلَاتِ البَنْكِيَّةِ لِلشَّرِكَة، مَعَ حِسَابٍ فَوْرِيٍّ لِلْمُسْتَخْدَمِ وَالمُتَاحِ الفِعْلِيِّ بَعْدَ اسْتِبْعَادِ الغِطَاءِ النَّقْدِيّ.',
        },
        2: {
            "en": 'Step Two: AI Credit Facility Agreement Verification. Uploading the signed bank facility agreement verifies total limits, sub-limits, commission rates, zero percent sovereign cash margin, and contractual turnaround time with zero mismatches, preventing costly setup errors.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: مُطَابَقَةُ عَقْدِ التَّسْهِيلِ البَنْكِيِّ المُوَقَّعِ مَعَ إِعْدَادَاتِ النِّظَامِ بِالذَّكَاءِ الاصْطِنَاعِيّ. عِنْدَ رَفْعِ عَقْدِ التَّسْهِيلِ البَنْكِيّ، يُطَابِقُ الذَّكَاءُ الاصْطِنَاعِيُّ الحُدُودَ الكُلِّيَّةَ وَالفَرْعِيَّةَ وَنِسْبَةَ العُمُولَةِ وَالإِعْفَاءَ مِنَ الغِطَاءِ النَّقْدِيِّ لِضَمَانِ دِقَّةِ التَّسْعِيرْ.',
        },
        3: {
            "en": 'Step Three: Six-Factor Smart Bank Scoring from Zero to One Hundred. For our approved twelve million Egyptian Pound guarantee, Grow scores every eligible bank across commission cost, cash margin, actual historical bank turnaround speed, available headroom, currency match, and preservation of scarce project-specific lines.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَرْتِيبٌ فَوْرِيٌّ لِلْبُنُوكِ مِنْ صِفْرٍ إِلَى مِائَةٍ عَبْرَ سِتَّةِ مَعَايِيرَ مَالِيَّةٍ وَتَشْغِيلِيَّة. يُفَاضِلُ المُحَرِّكُ بَيْنَ البُنُوكِ المُؤَهَّلَةِ حَسَبَ أَقَلِّ عُمُولَة، وَأَقَلِّ غِطَاءٍ نَقْدِيّ، وَسُرْعَةِ البَنْكِ الفِعْلِيَّةِ تَارِيخِيّاً، وَالرَّصِيدِ المُتَاحِ وَالحِفَاظِ عَلَى الخُطُوطِ الائْتِمَانِيَّةِ النَّادِرَة.',
        },
        4: {
            "en": 'Step Four: Pipeline Headroom and Optimal Bank Selection. Inside the execution wizard, First National Corporate Bank ranks number one with the lowest commission, zero cash margin, and eighty-one point five million Egyptian Pounds in net available headroom after pending pipeline requests.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُقَارَنَةٌ شَفَّافَةٌ لِلتَّكْلِفَةِ وَالرَّصِيدِ المُتَاحِ وَاخْتِيَارُ البَنْكِ الأَمْثَلْ. يَتَصَدَّرُ البَنْكُ الوَطَنِيُّ لِتَمْوِيلِ الشَّرِكَاتِ المَرْكَزَ الأَوَّلَ بِأَقَلِّ عُمُولَةٍ وَصِفْرِ غِطَاءٍ نَقْدِيٍّ وَرَصِيدٍ مُتَاحٍ يَتَجَاوَزُ وَاحِداً وَثَمَانِينَ مِلْيُونَ جُنَيْهْ.',
        },
        5: {
            "en": 'Step Five: Atomic Facility Reservation Lock. Selecting the bank locks twelve million Egyptian Pounds as Facility Reserved on the credit ledger immediately, ensuring parallel treasury deals never over-commit your bank limit. Click Proceed to Issue to auto-fill the bank form.',
            "ar": 'الخُطْوَةُ الخَامِسَة: القَفْلُ الفَوْرِيُّ لِلرَّصِيدِ الائْتِمَانِيِّ المَحْجُوزْ. بِضَغْطَةٍ وَاحِدَةٍ يَتِمُّ تَجْنِيبُ مَبْلَغِ اثْنَيْ عَشَرَ مِلْيُونَ جُنَيْهٍ لَحْظِيّاً مِنْ سَقْفِ التَّسْهِيلِ لِمَنْعِ أَيِّ تَجَاوُزٍ عِنْدَ تَعَدُّدِ العَمَلِيَّاتْ. اضْغَطِ المُتَابَعَةَ لِلْإِصْدَارِ لِلانْتِقَالِ إِلَى تَعْبِئَةِ اسْتِمَارَةِ البَنْكْ.',
        },
    },
    'issuance_bank_form': {
        1: {
            "en": "Step One: Choose Official Bank Application Form Execution. With the credit facility reserved, treasury can generate an Official Fillable Bank PDF Form or a bilingual Corporate Letter. Here we choose Fill Bank Form to populate First National Corporate Bank's official Trade Finance application with zero manual typing.",
            "ar": 'الخُطْوَةُ الأُولَى: اخْتِيَارُ التَّنْفِيذِ عَبْرَ اسْتِمَارَةِ البَنْكِ الرَّسْمِيَّة. بَعْدَ حَجْزِ الرَّصِيدِ الائْتِمَانِيّ، نَخْتَارُ تَعْبِئَةَ اسْتِمَارَةِ البَنْكِ الرَّسْمِيَّةِ القَابِلَةِ لِلْمَلْءِ تِلْقَائِيّاً دُونَ إِعَادَةِ كِتَابَةِ أَيِّ بَيَانَاتٍ يَدَوِيّاً.',
        },
        2: {
            "en": "Step Two: Sign-Ready Official Bank PDF Form plus Legal Wording Annex. Grow overlays every approved field directly onto the bank's PDF form, formatting amounts, IBANs, and dates, and automatically appends the companion Special-Wording Annex, cutting forty-five minutes of typing down to seconds.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: تَوْلِيدُ اسْتِمَارَةِ البَنْكِ الرَّسْمِيَّةِ مَعَ مُلْحَقِ الصِّيَاغَةِ الخَاصَّةِ جَاهِزَةً لِلتَّوْقِيعْ. يُسْقِطُ النِّظَامُ كَافَّةَ البَيَانَاتِ المُعْتَمَدَةِ بِدِقَّةٍ عَلَى نَمُوذَجِ مَلَفِّ بي دي إفْ الرَّسْمِيِّ لِلْبَنْكْ، مَعَ إِرْفَاقِ مُلْحَقِ الصِّيَاغَةِ الخَاصَّةِ تِلْقَائِيّاً خِلَالَ ثَوَانٍ.',
        },
        3: {
            "en": 'Step Three: Post-Issuance Bank Delivery and Turnaround Tracking. Once issued, the Post-Issuance Timeline records hand or courier delivery of the signed form to the bank and tracks actual bank turnaround hours, feeding live performance back into your bank scoring engine.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَوْثِيقُ تَسْلِيمِ الاسْتِمَارَةِ لِلْبَنْكِ وَمُتَابَعَةُ مُهْلَةِ الإِصْدَارْ. يُتَابِعُ الخَطُّ الزَّمَنِيُّ تَسْلِيمَ الاسْتِمَارَةِ المُوَقَّعَةِ لِفَرْعِ البَنْكِ وَيَحْسِبُ زَمَنَ اسْتِجَابَةِ البَنْكِ الفِعْلِيَّ لِتَحْدِيثِ تَقْيِيمِ سُرْعَةِ البَنْكِ تِلْقَائِيّاً.',
        },
        4: {
            "en": "Step Four: AI Verification of the Bank's Final Issued Guarantee. When the bank releases the final guarantee scan, Grow runs an instant AI field-by-field check, confirming exact alignment on the twelve million amount, currency, beneficiary, and expiry date before you hand it to your client.",
            "ar": 'الخُطْوَةُ الرَّابِعَة: فَحْصُ نُسْخَةِ الضَّمَانِ الصَّادِرِ مِنَ البَنْكِ بِالذَّكَاءِ الاصْطِنَاعِيِّ قَبْلَ تَسْلِيمِهِ لِلْعَمِيلْ. عِنْدَ اسْتِلَامِ صُورَةِ الضَّمَانِ النِّهَائِيِّ مِنَ البَنْكْ، يُطَابِقُهَا الذَّكَاءُ الاصْطِنَاعِيُّ حَقْلاً بِحَقْلٍ مَعَ الطَّلَبِ المُعْتَمَدِ لِضَمَانِ خُلُوِّهَا مِنْ أَيِّ خَطَأٍ مَطْبَعِيّ.',
        },
        5: {
            "en": 'Step Five: Unified Portfolio of Active Outgoing Guarantees. The guarantee is now Active and Verified in your Outgoing Issued Portfolio, linking department intake, credit approval, facility reservation, bank application form, and verified instrument in one audit trail. Click Next Tour for Position Reconciliation.',
            "ar": 'الخُطْوَةُ الخَامِسَة: سِجِلٌّ مُوَحَّدٌ لِمَحْفَظَةِ خِطَابَاتِ الضَّمَانِ الصَّادِرَةِ النَّشِطَة. يَظْهَرُ الضَّمَانُ الآنَ بِحَالَةِ نَشِطٍ وَمُوَثَّقٍ فِي سِجِلِّ الضَّمَانَاتِ الصَّادِرَة، رَابِطاً الطَّلَبَ وَالمَوَافَقَةَ وَحَجْزَ التَّسْهِيلِ وَأَصْلَ الضَّمَانِ فِي مَلَفٍّ وَاحِدْ.',
        },
    },
    'issuance_recon': {
        1: {
            "en": 'Step One: Risk-Smart Outgoing Maintenance Governance. Notice how Grow flips governance intelligently for Outgoing Guarantees: returning and closing a guarantee executes directly without delay to free up your bank credit line and stop commissions immediately, while extensions, amount increases, and activating non-operative guarantees require credit approval.',
            "ar": 'الخُطْوَةُ الأُولَى: حَوْكَمَةٌ ذَكِيَّةٌ مُعَاكِسَةٌ لِتَعْدِيلَاتِ الضَّمَانَاتِ الصَّادِرَة. فِي الضَّمَانَاتِ الصَّادِرَة، يُنَفَّذُ رَدُّ وَإِلْغَاءُ الضَّمَانِ مُبَاشَرَةً دُونَ انْتِظَارٍ لِتَحْرِيرِ سَقْفِ التَّسْهِيلِ وَإِيقَافِ العُمُولَاتِ فَوْراً، بَيْنَمَا يَخْضَعُ التَّمْدِيدُ وَزِيَادَةُ المَبْلَغِ وَتَفْعِيلُ الضَّمَانِ غَيْرِ السَّارِي لِمَصْفُوفَةِ المَوَافَقَاتِ الائْتِمَانِيَّة.',
        },
        2: {
            "en": "Step Two: Upload Monthly Bank Guarantee Position Report. Banks frequently leave expired or returned guarantees open on their books, quietly charging commissions and blocking credit lines. Upload First National Corporate Bank's monthly position statement to audit every active guarantee.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: رَفْعُ كَشْفِ مَرَاكِزِ خِطَابَاتِ الضَّمَانِ الشَّهْرِيِّ الوَارِدِ مِنَ البَنْكْ. كَثِيراً مَا تَبْقَى ضَمَانَاتٌ مُنْتَهِيَةٌ مَفْتُوحَةً فِي دَفَاتِرِ البَنْكِ فَتَحْجِزُ التَّسْهِيلَ وَتَسْتَقْطِعُ عُمُولَاتٍ دَوْرِيَّة. ارْفَعْ كَشْفَ مَرَاكِزِ البَنْكِ الشَّهْرِيَّ لِمُطَابَقَةِ كُلِّ ضَمَانٍ فِي ثَوَانٍ.',
        },
        3: {
            "en": 'Step Three: Three-Way Position Variance Matrix. Grow immediately segments the bank statement into Matched guarantees, System-Only expired guarantees still locking three point five million Pounds of credit headroom, Bank-Only unrecorded exposure, and amount or expiry mismatches.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: مَصْفُوفَةُ فَرْزِ التَّطَابُقِ وَالفُرُوقِ وَالضَّمَانَاتِ المُنْتَهِيَة. يَفْرِزُ النِّظَامُ الكَشْفَ فَوْراً إِلَى ضَمَانَاتٍ مُتَطَابِقَة، وَضَمَانَاتٍ مُنْتَهِيَةٍ مَا زَالَتْ تَحْجِزُ ثَلَاثَةَ مَلَايِينَ وَنِصْفاً مِنْ سَقْفِ التَّسْهِيلْ، وَضَمَانَاتٍ مُسَجَّلَةٍ بِالبَنْكِ فَقَطْ، وَفُرُوقٍ فِي المَبَالِغِ وَالتَّوَارِيخْ.',
        },
        4: {
            "en": 'Step Four: Anti-Bypass Variance Resolution Controls. Grow prevents silent back-door balance edits: every discrepancy must either be formally Disputed with the bank to reclaim limits or routed through governed approval, preserving complete credit audit integrity.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُعَالَجَةٌ رَقَابِيَّةٌ مَحْكُومَةٌ تَمْنَعُ التَّعْدِيلَاتِ غَيْرَ المُعْتَمَدَة. يَمْنَعُ النِّظَامُ أَيَّ تَعْدِيلٍ خَفِيٍّ لِلْأَرْصِدَة؛ حَيْثُ يَلْزَمُ تَسْجِيلُ اعْتِرَاضٍ رَسْمِيٍّ لِمُخَاطَبَةِ البَنْكِ وَاسْتِرْدَادِ السَّقْفْ، أَوْ إِحَالَةُ التَّسْوِيَةِ لِاعْتِمَادِ المَسْؤُولِ المَالِيّ.',
        },
        5: {
            "en": 'Step Five: Sealed Position Reconciliation History. The monthly bank position session is now sealed as Completed, giving Chief Financial Officers and external auditors permanent proof of verified exposure, disputed fees, and reclaimed credit headroom, completing the LG Issuance module.',
            "ar": 'الخُطْوَةُ الخَامِسَة: إِغْلَاقُ وَتَوْثِيقُ جَلْسَةِ مُطَابَقَةِ مَرَاكِزِ البَنْكْ. تُخْتَمُ جَلْسَةُ المُطَابَقَةِ بِحَالَةِ مُكْتَمِلْ، لِتُوَفِّرَ لِلْمُدِيرِ المَالِيِّ وَالمُرَاجِعِ الخَارِجِيِّ سِجِلّاً دَائِماً لِلِالْتِزَامَاتِ الفِعْلِيَّةِ وَالحُدُودِ المُسْتَرَدَّة، لِتَكْتَمِلَ بِذَلِكَ جَوْلَاتُ وَحْدَةِ الإِصْدَارْ.',
        },
    },
    'quotations_rfq': {
        1: {
            "en": 'Step One: Multi-Pair Foreign Exchange Spot Basket Builder. Instead of calling banks one by one while currency markets move, structure up to four currency pairs in a single simultaneous tender. Here we combine buying five hundred thousand US Dollars and two hundred fifty thousand Euros against Egyptian Pounds, guided by live Central Bank fixings.',
            "ar": 'الخُطْوَةُ الأُولَى: بِنَاءُ سَلَّةِ عُمْلَاتٍ فَوْرِيَّةٍ مُتَعَدِّدَةِ الأَزْوَاجِ فِي مُنَاقَصَةٍ مَصْرِفِيَّةٍ وَاحِدَة. بَدَلاً مِنَ الاتِّصَالِ الهَاتِفِيِّ المُتَتَابِعِ بِالبُنُوكِ مَعَ تَحَرُّكِ السُّوقْ، ابْنِ سَلَّةَ شِرَاءٍ حَتَّى أَرْبَعَةِ أَزْوَاجِ عُمْلَاتٍ مَعاً، مِثْلِ شِرَاءِ خَمْسِمِائَةِ أَلْفِ دُولَارْ وَمِائَتَيْنِ وَخَمْسِينَ أَلْفَ يُورُو، مَعَ اسْتِرْشَادٍ لَحْظِيٍّ بِسِعْرِ البَنْكِ المَرْكَزِيّ.',
        },
        2: {
            "en": "Step Two: Per-Bank Tariff Schedule and Settlement Time-Value Normalization. Selecting your relationship banks automatically applies each bank's pre-agreed commission percentage, minimum and maximum fee clamps, and flat transfer charges. It also enables alternative settlement dates normalized by your Time-Value-of-Money rate so different value dates rank fairly.",
            "ar": 'الخُطْوَةُ الثَّانِيَة: مَصْفُوفَةُ عُمُولَاتِ البُنُوكِ وَمُعَادَلَةُ القِيمَةِ الزَّمَنِيَّةِ لِتَوَارِيخِ التَّسْوِيَة. عِنْدَ اخْتِيَارِ البُنُوكِ المُتَنَافِسَة، يَرْبِطُ النِّظَامُ تِلْقَائِيّاً شَرِيحَةَ عُمُولَةِ كُلِّ بَنْكٍ وَالرُّسُومَ الثَّابِتَة، مَعَ مُعَادَلَةِ القِيمَةِ الزَّمَنِيَّةِ لِلنُّقُودِ عِنْدَ اقْتِرَاحِ تَارِيخِ تَسْوِيَةٍ بَدِيلٍ لِضَمَانِ مُقَارَنَةٍ عَادِلَةٍ تَمَاماً.',
        },
        3: {
            "en": 'Step Three: Sovereign Treasury Bills Auction Mode. Deploying surplus cash or raising liquidity? One click switches the workspace to Sovereign Treasury Bills mode, supporting competitive secondary-market purchase and sale tenders across target maturity windows and minimum yield thresholds.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: وَضْعُ مُنَاقَصَاتِ أُذُونِ الخِزَانَةِ فِي السُّوقِ الثَّانَوِيّ. لِاسْتِثْمَارِ فَوَائِضِ السُّيُولَةِ أَوْ تَسْيِيلِ مَحْفَظَةٍ قَائِمَة، يُحَوِّلُ زِرٌّ وَاحِدٌ الشَّاشَةَ إِلَى وَضْعِ أُذُونِ الخِزَانَة، لِطَرْحِ عَطَاءَاتِ الشِّرَاءِ أَوِ البَيْعِ بِأَفْضَلِ عَائِدٍ عَبْرَ نِطَاقَاتِ الاسْتِحْقَاقِ المُسْتَهْدَفَة.',
        },
        4: {
            "en": 'Step Four: Pre-Broadcast Corporate Admin Release Gate. Before binding Execution requests reach bank trading desks, Corporate Admins review the basket, confirm the institutional liability check, and release invitations to every selected bank at the exact same millisecond.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: بَوَّابَةُ اعْتِمَادِ المَسْؤُولِ وَالإِقْرَارِ المُؤَسَّسِيِّ قَبْلَ البَثِّ المُتَزَامِنْ. قَبْلَ وُصُولِ طَلَبَاتِ التَّنْفِيذِ المُلْزِمَةِ لِغُرَفِ تَدَاوُلِ البُنُوكْ، يُرَاجِعُ المُعْتَمِدُ تَفَاصِيلَ السَّلَّةِ وَقَائِمَةَ البُنُوكْ، لِيَتِمَّ بَثُّ الدَّعَوَاتِ لِكَافَّةِ البُنُوكِ فِي نَفْسِ اللَّحْظَةِ الزَّمَنِيَّة.',
        },
        5: {
            "en": 'Step Five: Sealed Blind Live Trading Floor Telemetry. While the countdown window is active, treasury tracks anonymous dealer connections and quote counts in real time, while every bank identity and price remains encrypted until the clock reaches zero. Click Next Tour to step inside the Bank Dealer Desk.',
            "ar": 'الخُطْوَةُ الخَامِسَة: مُرَاقَبَةٌ حَيَّةٌ لِلتَّدَاوُلِ المُغْلَقِ مَعَ تَشْفِيرٍ كَامِلٍ لِلْأَسْعَارِ حَتَّى إِغْلَاقِ النَّافِذَة. أَثْنَاءَ سَرَيَانِ نَافِذَةِ العَطَاءْ، يَرَى فَرِيقُ الخِزَانَةِ عَدَدَ البُنُوكِ المُتَّصِلَةِ وَالعُرُوضِ المُسْتَلَمَةِ لَحْظِيّاً، بَيْنَمَا تَظَلُّ الأَسْعَارُ وَهُوِيَّاتُ البُنُوكِ مُشَفَّرَةً بِالكَامِلِ حَتَّى انْتِهَاءِ العَدِّ التَّنَازُلِيّ.',
        },
    },
    'quotations_dealer': {
        1: {
            "en": "Step One: Zero-Password Domain-Locked Bank Dealer Gate. Now step inside First National Corporate Bank's FX trading room. Clicking the emailed invitation opens a zero-password verification gate locked strictly to authorized bank email domains with a six-digit one-time passcode, requiring zero IT onboarding.",
            "ar": 'الخُطْوَةُ الأُولَى: بَوَّابَةُ دُخُولِ مُتَدَاوِلِ البَنْكِ بِدُونِ كَلِمَةِ مُرُورْ. نَنْتَقِلُ الآنَ لِغُرْفَةِ التَّدَاوُلِ بِالبَنْكْ؛ فَبِمُجَرَّدِ فَتْحِ الرَّابِطِ المُؤَمَّنِ تَظْهَرُ بَوَّابَةُ تَحَقُّقٍ فَوْرِيَّةٌ مُقَيَّدَةٌ بِالبَرِيدِ الرَّسْمِيِّ المُعْتَمَدِ لِمُتَدَاوِلِ البَنْكِ وَرَمْزِ دُخُولٍ لِمَرَّةٍ وَاحِدَة.',
        },
        2: {
            "en": 'Step Two: Single-Trader Concurrency Lock and Multi-Leg Console. Inside the authenticated console, a forty-five-second heartbeat locks active quoting rights to one trader per bank desk while colleagues watch in spectator mode, displaying exact currency notionals, value dates, and Central Bank reference benchmarks.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: مِنَصَّةُ التَّسْعِيرِ مُتَعَدِّدَةِ العُمْلَاتِ مَعَ قَفْلِ مَنْعِ تَضَارُبِ المُتَدَاوِلِينْ. تَمْنَحُ المِنَصَّةُ صَلَاحِيَةَ الإِدْخَالِ لِمُتَدَاوِلٍ نَشِطٍ وَاحِدٍ مِنَ البَنْكِ مَعَ وَضْعِ زُمَلَائِهِ فِي وَضْعِ المُشَاهَدَةِ اللَّحْظِيَّة، وَتَعْرِضُ المَبَالِغَ وَتَوَارِيخَ التَّسْوِيَةِ وَالسِّعْرَ الاسْتِرْشَادِيَّ لِلْبَنْكِ المَرْكَزِيّ.',
        },
        3: {
            "en": 'Step Three: Real-Time Fat-Finger and Cross-Rate Swap Guardrail. When the dealer accidentally enters five hundred twelve point five zero on Dollars alongside fifty-five point six two on Euros, Grow intercepts the submission immediately, catching the displaced decimal and inverted cross-rate before a costly mistake is recorded.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: حَارِسُ الحِمَايَةِ اللَّحْظِيَّةِ مِنْ أَخْطَاءِ الفَاصِلَةِ العَشْرِيَّةِ وَعَكْسِ الأَسْعَارْ. عِنْدَ إِدْخَالِ سِعْرٍ خَاطِئٍ بِفَاصِلَةٍ مُزَاحَة، يَعْتَرِضُ النِّظَامُ الإِرْسَالَ فَوْراً مُنَبِّهاً لِانْعِكَاسِ السِّعْرِ التَّقَاطُعِيِّ مُقَارَنَةً بِمُؤَشِّرِ البَنْكِ المَرْكَزِيِّ لِحِمَايَةِ الطَّرَفَيْنْ.',
        },
        4: {
            "en": 'Step Four: Blind Live Competitive Rank Feedback. With live ranking active, the bank dealer sees their real-time rank standing without ever seeing competitor names or rival prices, creating instant competitive tension to tighten their spread while the window remains open.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: مُؤَشِّرُ التَّرْتِيبِ التَّنَافُسِيِّ الحَيِّ وَالسِّرِّيّ. يَرَى مُتَدَاوِلُ البَنْكِ تَرْتِيبَهُ التَّنَافُسِيَّ الحَالِيَّ بَيْنَ البُنُوكِ المُشَارِكَةِ كَرَقْمِ تَرْتِيبٍ فَقَطْ دُونَ أَيِّ كَشْفٍ لِأَسْمَاءِ أَوْ أَسْعَارِ المُنَافِسِينْ، مِمَّا يَدْفَعُهُ لِتَحْسِينِ سِعْرِهِ فَوْراً لِصَالِحِ الشَّرِكَة.',
        },
        5: {
            "en": 'Step Five: Sharpening the Quote to Capture Rank Number One. Tightening the Dollar quote to fifty-one point two four elevates First National Corporate Bank to Number One Leading Quote in Market, locking in measurable basis-point savings for your treasury team. Click Next Tour for Net Award and Audit Certificates.',
            "ar": 'الخُطْوَةُ الخَامِسَة: تَحْسِينُ السِّعْرِ وَانْتِزَاعُ المَرْكَزِ الأَوَّلِ فِي العَطَاءْ. بِمُجَرَّدِ خَفْضِ السِّعْرِ إِلَى وَاحِدٍ وَخَمْسِينَ جُنَيْهاً وَأَرْبَعَةٍ وَعِشْرِينَ قِرْشاً، يَرْتَقِي البَنْكُ فَوْراً إِلَى صَدَارَةِ المُنَافَسَة، مُحَقِّقاً وَفْراً مَالِيّاً مُبَاشِراً لِخِزَانَةِ الشَّرِكَةِ قَبْلَ إِغْلَاقِ النَّافِذَة.',
        },
    },
    'quotations_award': {
        1: {
            "en": 'Step One: Unsealed All-In Net Execution Standings. The instant the bidding window closes, Grow unseals every encrypted bid and ranks banks by true all-in net cost after commission percentages, fee clamps, flat transfer charges, and settlement-date time value of money.',
            "ar": 'الخُطْوَةُ الأُولَى: إِغْلَاقُ النَّافِذَةِ وَفَكُّ تَشْفِيرِ العُرُوضِ مُرَتَّبَةً بِالصَّافِي الشَّامِلْ. فَوْرَ انْتِهَاءِ العَدِّ التَّنَازُلِيّ، يَفُكُّ النِّظَامُ تَشْفِيرَ العُرُوضِ وَيُرَتِّبُ البُنُوكَ تِلْقَائِيّاً بِالتَّكْلِفَةِ الصَّافِيَةِ الفِعْلِيَّةِ بَعْدَ إِضَافَةِ العُمُولَاتِ وَفُرُوقِ التَّسْوِيَة.',
        },
        2: {
            "en": 'Step Two: Multi-Leg Cherry-Picking Inside the Acceptance Countdown. Within the synchronized acceptance timer, treasury cherry-picks the best bank per currency leg: First National Corporate Bank wins the five hundred thousand Dollar leg, while Mediterranean Commercial Bank wins the two hundred fifty thousand Euro leg.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: التَّرْسِيَةُ المُجَزَّأَةُ لِأَفْضَلِ بَنْكٍ فِي كُلِّ عُمْلَةٍ خِلَالَ مُهْلَةِ القَبُولْ. يُرَشِّحُ النِّظَامُ الفَائِزَ الأَفْضَلَ لِكُلِّ عُمْلَةٍ عَلَى حِدَة؛ فَيَفُوزُ البَنْكُ الوَطَنِيُّ بِشِرَاءِ الدُّولَارْ، بَيْنَمَا يَفُوزُ بَنْكُ البَحْرِ المُتَوَسِّطِ بِشِرَاءِ اليُورُو لِتَعْظِيمِ إِجْمَالِيِّ الوَفْرْ.',
        },
        3: {
            "en": 'Step Three: Four-Pillar Central Bank Rate Triangulation. Every awarded leg records a permanent benchmark check comparing your executed rate against the frozen Central Bank official mid-rate, live interbank spot feeds, historical fixings, and anonymous corporate spread cohorts.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَوْثِيقُ عَدَالَةِ السِّعْرِ مُقَارَنَةً بِمُؤَشِّرِ البَنْكِ المَرْكَزِيِّ لَحْظَةَ التَّنْفِيذْ. يُوَثِّقُ النِّظَامُ لِكُلِّ عُمْلَةٍ مُقَارَنَةً مَرْجِعِيَّةً رُبَاعِيَّةً تَرْبِطُ سِعْرَ التَّنْفِيذِ الفِعْلِيَّ بِالسِّعْرِ الرَّسْمِيِّ المُعْلَنِ مِنَ البَنْكِ المَرْكَزِيِّ وَمُتَوَسِّطَاتِ السُّوقْ.',
        },
        4: {
            "en": 'Step Four: Printable Best Execution Audit Certificate. One click opens an executive Best Execution Audit Certificate ready for print or PDF export, documenting every invited bank, submission timestamp, net price, and quantified treasury savings for boards and statutory auditors.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: شَهَادَةُ التَّدْقِيقِ الرَّسْمِيَّةُ لِإِثْبَاتِ أَفْضَلِ تَنْفِيذٍ سِعْرِيّ. بِضَغْطَةٍ وَاحِدَةٍ تَصْدُرُ شَهَادَةُ تَدْقِيقٍ جَاهِزَةٌ لِلطِّبَاعَةِ وَالتَّصْدِيرْ، مُوَثِّقَةً كَافَّةَ العُرُوضِ وَالتَّوْقِيتَاتِ وَالوَفْرِ المَالِيِّ المُحَقَّقِ لِلْمُرَاجِعِينَ وَمَجْلِسِ الإِدَارَة.',
        },
        5: {
            "en": 'Step Five: Bank-Scoped Cryptographic Deal Execution Receipt. Each winning bank receives a digitally signed Deal Execution Receipt scoped strictly to the currency leg it won, never disclosing which rival bank won the other leg or at what price.',
            "ar": 'الخُطْوَةُ الخَامِسَة: إِيصَالُ تَنْفِيذِ الصَّفْقَةِ المُوَقَّعُ رَقْمِيّاً وَالمَعْزُولُ لِكُلِّ بَنْكٍ فَائِزْ. يَتَسَلَّمُ كُلُّ بَنْكٍ فَائِزٍ إِيصَالَ تَنْفِيذٍ رَقْمِيّاً مُخَصَّصاً حَصْرِيّاً لِلْعُمْلَةِ الَّتِي فَازَ بِهَا، دُونَ أَيِّ إِفْصَاحٍ عَنِ الفَائِزِ بِالعُمْلَاتِ الأُخْرَى أَوْ أَسْعَارِهَا.',
        },
        6: {
            "en": 'Step Six: Quotation History and Quantified Treasury Value Analytics. The history dashboard archives every multi-pair currency basket and Sovereign Treasury Bill tender, proving cumulative treasury savings and bank win rates across every quarter, completing the Quotations module.',
            "ar": 'الخُطْوَةُ السَّادِسَة: السِّجِلُّ التَّارِيخِيُّ لِلصَّفَقَاتِ وَتَحْلِيلَاتُ الوَفْرِ التَّرَاكُمِيّ. تَجْمَعُ لَوْحَةُ السِّجِلِّ التَّارِيخِيِّ كَافَّةَ صَفَقَاتِ العُمْلَاتِ الفَوْرِيَّةِ وَعَطَاءَاتِ أُذُونِ الخِزَانَةِ المُنَفَّذَةِ مَعَ قِيَاسِ الوَفْرِ التَّرَاكُمِيّ، لِتَكْتَمِلَ بِذَلِكَ جَوْلَاتُ وَحْدَةِ التَّسْعِيرْ.',
        },
    },
    'recon_import': {
        1: {
            "en": 'Step One: Zero-Configuration Multi-Bank Statement Import. Select First National Corporate Bank and drop any Excel or CSV account statement. Grow automatically detects Arabic and English column headers, booking and value dates, and debit or credit layouts without custom IT templates.',
            "ar": 'الخُطْوَةُ الأُولَى: اسْتِيرَادٌ ذَكِيٌّ لِكُشُوفِ الحِسَابَاتِ البَنْكِيَّةِ بِدُونِ إِعْدَادَاتٍ مُسْبَقَة. اخْتَرِ البَنْكَ وَارْفَعْ مَلَفَّ كَشْفِ الحِسَابِ مُبَاشَرَةً؛ حَيْثُ يَتَعَرَّفُ النِّظَامُ تِلْقَائِيّاً عَلَى الأَعْمِدَةِ العَرَبِيَّةِ وَالإِنْجِلِيزِيَّةِ وَتَوَارِيخِ القَيْدِ وَالحَقِّ وَالمَبَالِغْ.',
        },
        2: {
            "en": 'Step Two: Mathematical Opening-to-Closing Continuity Audit. Every imported statement passes an instant mathematical check confirming that Opening Balance plus Credits minus Debits equals Closing Balance, catching truncated exports or missing rows before reconciliation begins.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: التَّحَقُّقُ الرِّيَاضِيُّ الآلِيُّ مِنْ تَسَلْسُلِ الأَرْصِدَةِ الِافْتِتَاحِيَّةِ وَالخِتَامِيَّة. يَفْحَصُ النِّظَامُ تَوَازُنَ المُعَادَلَةِ المُحَاسَبِيَّةِ لِكُلِّ كَشْفٍ مَرْفُوعٍ وَتَسَلْسُلَ الأَرْصِدَةِ بَيْنَ الكُشُوفِ المُتَتَالِيَة، لِكَشْفِ أَيِّ سُطُورٍ نَاقِصَةٍ فَوْراً.',
        },
        3: {
            "en": 'Step Three: One-Click Hybrid Rules and AI Classification Pipeline. Inside the statement workspace, clicking Run Rules and AI cascades across built-in banking rail detectors, self-learned company rules, counterparty memory, and privacy-shielded AI.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَشْغِيلُ المُحَرِّكِ الهَجِينِ لِلْقَوَاعِدِ المُحَاسَبِيَّةِ وَالذَّكَاءِ الاصْطِنَاعِيِّ بِضَغْطَةٍ وَاحِدَة. فِي مِسَاحَةِ عَمَلِ الكَشْفْ، يُشَغِّلُ زِرُّ التَّصْنِيفِ أَرْبَعَ طَبَقَاتٍ مُتَتَالِيَةٍ تَشْمَلُ كَوَاشِفَ القَنَوَاتِ المَصْرِفِيَّةِ وَالقَوَاعِدَ المُتَعَلَّمَةَ وَذَاكِرَةَ الأَطْرَافِ وَالذَّكَاءَ الاصْطِنَاعِيّ.',
        },
        4: {
            "en": 'Step Four: Explainable Classification Badges with Full Provenance. There is zero black-box guesswork: every row displays its matched counterparty pill, the exact rule or AI source, target General Ledger account, and calibrated confidence score.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: شَارَاتُ تَصْنِيفٍ مُفَسَّرَةٌ تُوَضِّحُ مَصْدَرَ القَرَارِ وَحِسَابَ الأُسْتَاذِ العَامّ. كُلُّ سَطْرٍ يَعْرِضُ بِوُضُوحٍ اسْمَ الطَّرَفِ المُقَابِلِ وَالقَاعِدَةَ المُطَبَّقَةَ وَرَقْمَ الحِسَابِ المُحَاسَبِيِّ وَدَرَجَةَ الثِّقَة، مَعَ فَرْزٍ أَمِينٍ لِلْحَالَاتِ الغَامِضَة.',
        },
        5: {
            "en": 'Step Five: Slide-Over Transaction Deep-Dive Audit Drawer. Clicking any transaction opens the audit drawer, preserving raw English and Arabic bank narratives, payment references, and complete General Ledger lineage. Click Next Tour for Cross-Bank Sweeps and ERP Matching.',
            "ar": 'الخُطْوَةُ الخَامِسَة: دُرْجُ المُرَاجَعَةِ التَّفْصِيلِيُّ لِلنَّصِّ البَنْكِيِّ الخَامِ وَمَسَارِ التَّوْجِيهْ. يَعْرِضُ دُرْجُ المُرَاجَعَةِ الجَانِبِيُّ النَّصَّ الأَصْلِيَّ الوَارِدَ مِنَ البَنْكِ بِالعَرَبِيَّةِ وَالإِنْجِلِيزِيَّةِ وَالمَرْجِعَ المَصْرِفِيَّ وَكَامِلَ تَفَاصِيلِ التَّوْجِيهِ المُحَاسَبِيّ.',
        },
    },
    'recon_sweeps': {
        1: {
            "en": 'Step One: Automatic Cross-Bank Liquidity Sweep Pairing. In the unified ledger, your company swept five million Egyptian Pounds from First National Corporate Bank to Mediterranean Commercial Bank, with a one hundred seventy-five Pound transfer fee deducted on outflow. Grow pairs both bank statements automatically and isolates the fee.',
            "ar": 'الخُطْوَةُ الأُولَى: الرَّبْطُ التِّلْقَائِيُّ لِلتَّحْوِيلَاتِ البَيْنِيَّةِ بَيْنَ البُنُوكِ حَتَّى مَعَ خَصْمِ عُمُولَةِ التَّحْوِيلْ. فِي دَفْتَرِ الأُسْتَاذِ المُوَحَّدْ، يَرْبِطُ النِّظَامُ تِلْقَائِيّاً سَحْبَ خَمْسَةِ مَلَايِينِ جُنَيْهٍ مِنَ البَنْكِ الأَوَّلِ مَعَ إِيدَاعِهَا بِالبَنْكِ الثَّانِي، وَيَفْصِلُ عُمُولَةَ التَّحْوِيلِ البَالِغَةَ مِائَةً وَخَمْسَةً وَسَبْعِينَ جُنَيْهاً.',
        },
        2: {
            "en": 'Step Two: Side-by-Side Sweep Pair Verification Modal. Clicking the Sweep badge displays both legs side by side across bank accounts, confirming the five million principal transfer and isolating the one hundred seventy-five Pound corridor fee so internal transfers never inflate operating cash flows.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: نَافِذَةُ التَّحَقُّقِ المُزْدَوَجَةُ لِفَحْصِ طَرَفَيِ التَّحْوِيلِ البَيْنِيِّ جَنْباً إِلَى جَنْبْ. تَعْرِضُ النَّافِذَةُ حَرَكَةَ السَّحْبِ مُقَابِلَ حَرَكَةِ الإِيدَاعِ عَبْرَ الحِسَابَيْنِ البَنْكِيَّيْنِ مَعَ إِثْبَاتِ فَرْقِ العُمُولَة، لِمَنْعِ تَضْخِيمِ التَّدَفُّقَاتِ التَّشْغِيلِيَّةِ بِالتَّحْوِيلَاتِ الدَّاخِلِيَّة.',
        },
        3: {
            "en": 'Step Three: Self-Canceling Returned Wire and Reversal Netting. An outward three hundred eighty thousand Pound supplier wire bounced back the next day due to a closed IBAN. Grow nets both rows together automatically so your open supplier bill is never marked paid by mistake.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: تَصْفِيَةُ التَّحْوِيلَاتِ المُرْتَدَّةِ وَالقُيُودِ العَكْسِيَّةِ تِلْقَائِيّاً. عِنْدَ ارْتِدَادِ تَحْوِيلٍ صَادِرٍ لِمُوَرِّدٍ بِسَبَبِ إِغْلَاقِ الحِسَابِ المُسْتَفِيدْ، يَرْبِطُهُمَا النِّظَامُ كَقَيْدٍ عَكْسِيٍّ صِفْرِيٍّ حَتَّى لَا تُقْفَلَ فَاتُورَةُ المُوَرِّدِ بِالخَطَأْ.',
        },
        4: {
            "en": 'Step Four: Dual-Pane Split Match Against Open ERP Invoices. Switching to Split Match places an inward SWIFT collection of one million two hundred forty-nine thousand eight hundred fifty Pounds right beside the open one million two hundred fifty thousand Pound ERP invoice, spotting the one hundred fifty Pound wire fee instantly.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: شَاشَةُ المُطَابَقَةِ المُزْدَوَجَةِ بَيْنَ سُطُورِ البَنْكِ وَفَوَاتِيرِ نِظَامِ إي آر بي المَفْتُوحَة. تَضَعُ الشَّاشَةُ المُزْدَوَجَةُ تَحْصِيلَ السُّوِيفْتِ الصَّافِي مُبَاشَرَةً مُقَابِلَ فَاتُورَةِ المَبِيعَاتِ المَفْتُوحَة، كَاشِفَةً فَرْقَ عُمُولَةِ البَنْكِ المُرَاسِلِ البَالِغَ مِائَةً وَخَمْسِينَ جُنَيْهاً فَوْراً.',
        },
        5: {
            "en": 'Step Five: Confirmed Tolerance Match and Automatic Fee Write-Off. Confirming the match reconciles the full invoice and routes the one hundred fifty Pound deduction directly to Bank Corridor Charges. Click Next Tour for Self-Learning Rules and Privacy Architecture.',
            "ar": 'الخُطْوَةُ الخَامِسَة: إِقْفَالُ الفَاتُورَةِ بِالكَامِلِ وَتَوْجِيهُ فَرْقِ العُمُولَةِ لِلْمَصْرُوفَاتِ البَنْكِيَّةِ تِلْقَائِيّاً. فَوْرَ تَأْكِيدِ المُطَابَقَة، تُقْفَلُ فَاتُورَةُ العَمِيلِ بِكَامِلِ قِيمَتِهَا وَيُوَجَّهُ فَرْقُ الرُّسُومِ آلِيّاً لِحِسَابِ العُمُولَاتِ البَنْكِيَّةِ جَاهِزاً لِلْقَيْدِ المُزْدَوَجْ.',
        },
    },
    'recon_rules': {
        1: {
            "en": 'Step One: One-Click Inline Classification and Counterparty Memory. When a new counterparty narrative appears, classify it once and check Remember for Counterparty to turn a single human review into permanent future automation without writing a single formula.',
            "ar": 'الخُطْوَةُ الأُولَى: التَّصْنِيفُ السَّرِيعُ بِضَغْطَةٍ وَاحِدَةٍ وَتَفْعِيلُ ذَاكِرَةِ الطَّرَفِ المُقَابِلْ. عِنْدَ ظُهُورِ حَرَكَةٍ جَدِيدَة، اخْتَرِ الحِسَابَ المُحَاسَبِيَّ وَفَعِّلْ خِيَارَ تَذَكُّرِ الطَّرَفِ المُقَابِلِ لِيَتَحَوَّلَ قَرَارُ المُحَاسِبِ إِلَى أَتْمَتَةٍ دَائِمَةٍ فِي الكُشُوفِ القَادِمَة.',
        },
        2: {
            "en": 'Step Two: Online-Learned and Self-Healing Precision Rules. Grow tracks empirical precision across confirmations and overrides for every rule. Whenever an accountant overrides an edge case like a fee refund, the rule self-heals automatically by adding negative exclusion words.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: قَوَاعِدُ ذَاتِيَّةُ التَّعَلُّمِ تَقِيسُ دِقَّتَهَا الفِعْلِيَّةَ وَتُصَحِّحُ نَفْسَهَا تِلْقَائِيّاً. يَتَتَبَّعُ النِّظَامُ نِسْبَةَ الدِّقَّةِ الفِعْلِيَّةِ لِكُلِّ قَاعِدَة، وَعِنْدَ تَعْدِيلِ أَيِّ حَالَةٍ اسْتِثْنَائِيَّةٍ كَاسْتِرْدَادِ عُمُولَة، تُطَوِّرُ القَاعِدَةُ نَفْسَهَا تِلْقَائِيّاً لِمَنْعِ تَكْرَارِ الخَطَأْ.',
        },
        3: {
            "en": "Step Three: Per-Company Encrypted Counterparty Registry. Your supplier and customer names are encrypted with your company's dedicated AES-256 key and indexed with blind tokens, matching noisy bank statement aliases while keeping your commercial relationships completely confidential.",
            "ar": 'الخُطْوَةُ الثَّالِثَة: سِجِلُّ المُوَرِّدِينَ وَالعُمَلَاءِ المُشَفَّرُ بِمِفْتَاحٍ مُسْتَقِلٍّ لِكُلِّ شَرِكَة. تُحْفَظُ أَسْمَاءُ الأَطْرَافِ مُشَفَّرَةً دَاخِلَ خَزِينَةِ شَرِكَتِكَ مَعَ مُطَابَقَةٍ ذَكِيَّةٍ لِلِاخْتِصَارَاتِ البَنْكِيَّةِ دُونَ خُرُوجِ أَيِّ بَيَانَاتٍ تِجَارِيَّةٍ خَارِجَ نِطَاقِ الشَّرِكَة.',
        },
        4: {
            "en": 'Step Four: Privacy-Preserving Collaborative Consensus Brain. Before any banking syntax pattern reaches shared intelligence, Grow strips all company names, account numbers, dates, and amounts locally, promoting an anonymized banking pattern only after at least three independent companies confirm it.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: الذَّكَاءُ المَصْرِفِيُّ المُشْتَرَكُ الخَالِي تَمَاماً مِنَ البَيَانَاتِ الشَّخْصِيَّة. يُجَرِّدُ دِرْعُ الخُصُوصِيَّةِ النُّصُوصَ مَحَلِّيّاً مِنْ كَافَّةِ الأَسْمَاءِ وَالحِسَابَاتِ وَالمَبَالِغْ، وَلَا يَعْتَمِدُ أَيَّ صِيغَةٍ مَصْرِفِيَّةٍ عَامَّةٍ إِلَّا بَعْدَ تَوَافُقِ ثَلَاثِ شَرِكَاتٍ مُسْتَقِلَّةٍ عَلَى الأَقَلّ.',
        },
        5: {
            "en": 'Step Five: Bilingual Hierarchical General Ledger Taxonomy. Map English and Arabic treasury categories and subcategories directly to your corporate Chart of Accounts by debit or credit direction. Click Next Tour for Double-Entry Journal Export.',
            "ar": 'الخُطْوَةُ الخَامِسَة: شَجَرَةُ التَّصْنِيفَاتِ المُحَاسَبِيَّةِ ثُنَائِيَّةُ اللُّغَةِ وَرَبْطُهَا بِدَلِيلِ الحِسَابَاتْ. تُخَصِّصُ كُلُّ شَرِكَةٍ تَصْنِيفَاتِهَا الرَّئِيسِيَّةَ وَالفَرْعِيَّةَ بِالعَرَبِيَّةِ وَالإِنْجِلِيزِيَّةِ مَعَ رَبْطِهَا مُبَاشَرَةً بِأَرْقَامِ حِسَابَاتِ الأُسْتَاذِ العَامّ.',
        },
    },
    'recon_export': {
        1: {
            "en": 'Step One: Close Readiness Gate and Classification KPIs. Before generating General Ledger posting files, Accounting Export verifies one hundred percent classification readiness with eight of eight lines classified and zero unresolved exceptions remaining.',
            "ar": 'الخُطْوَةُ الأُولَى: بَوَّابَةُ جَاهِزِيَّةِ الإِقْفَالِ وَالتَّحَقُّقِ مِنِ اكْتِمَالِ التَّصْنِيفِ بِنِسْبَةِ مِائَةٍ بِالمِائَة. تَتَحَقَّقُ شَاشَةُ التَّصْدِيرِ المُحَاسَبِيِّ مِنْ تَصْنِيفِ كَافَّةِ السُّطُورِ وَعَدَمِ وُجُودِ أَيِّ حَرَكَاتٍ غَيْرِ مُصَنَّفَةٍ قَبْلَ إِصْدَارِ مَلَفَّاتِ التَّرْحِيلْ.',
        },
        2: {
            "en": 'Step Two: Customizable ERP Profile and Column Layout Builder. Select saved templates such as SAP Double-Entry Journal or tailor column headers, field ordering, date formats, and split debit and credit columns to fit SAP, Oracle, Dynamics, or Excel with zero middleware coding.',
            "ar": 'الخُطْوَةُ الثَّانِيَة: مُصَمِّمُ قَوَالِبِ التَّصْدِيرِ المُخَصَّصَةِ لِأَنْظِمَةِ إي آر بي المُخْتَلِفَة. اخْتَرْ قَالَبَ التَّصْدِيرِ المَحْفُوظَ أَوْ خَصِّصْ تَرْتِيبَ وَمُسَمَّيَاتِ الأَعْمِدَةِ وَصِيغَةَ التَّارِيخِ وَفَصْلَ المَدِينِ وَالدَّائِنِ دُونَ أَيِّ تَعْقِيدَاتٍ بَرْمَجِيَّة.',
        },
        3: {
            "en": 'Step Three: Automatic Double-Entry General Ledger Leg Expansion. Enabling Expand Double-Entry Journal Legs automatically converts each bank statement line into balanced Cash at Bank, Customer or Vendor Offset, and Bank Fee Variance entries.',
            "ar": 'الخُطْوَةُ الثَّالِثَة: التَّوْلِيدُ التِّلْقَائِيُّ لِأَطْرَافِ قَيْدِ اليَوْمِيَّةِ المُزْدَوَجِ وَفُرُوقِ العُمُولَاتْ. يُحَوِّلُ النِّظَامُ كُلَّ حَرَكَةٍ بَنْكِيَّةٍ تِلْقَائِيّاً إِلَى قَيْدِ يَوْمِيَّةٍ مُتَوَازِنٍ يَشْمَلُ حِسَابَ البَنْكِ وَحِسَابَ العَمِيلِ أَوِ المُوَرِّدِ وَسَطْرَ فَرْقِ عُمُولَةِ التَّحْوِيلْ.',
        },
        4: {
            "en": 'Step Four: Live Balanced Journal Preview Before Download. Preview the exact multi-leg General Ledger rows right on screen, confirming mathematically balanced debits and credits across every collection, vendor settlement, and bank fee voucher.',
            "ar": 'الخُطْوَةُ الرَّابِعَة: المُعَايَنَةُ الحَيَّةُ لِجَدْوَلِ قُيُودِ اليَوْمِيَّةِ المُتَوَازِنَةِ قَبْلَ التَّحْمِيلْ. تَعْرِضُ المُعَايَنَةُ الحَيَّةُ شَكْلَ القُيُودِ النِّهَائِيَّةِ شَامِلَةً أَرْقَامَ الحِسَابَاتِ وَتَوَازُنَ إِجْمَالِيِّ المَدِينِ وَالدَّائِنِ بِدِقَّةٍ تَامَّةٍ قَبْلَ إِصْدَارِ المَلَفّ.',
        },
        5: {
            "en": 'Step Five: Batch Export Lock Against Duplicate ERP Posting. Checking Lock and Mark as Exported stamps every downloaded transaction so no bank line can ever be double-posted into your General Ledger, completing all four modules of the Grow Corporate Treasury Platform.',
            "ar": 'الخُطْوَةُ الخَامِسَة: قَفْلُ الدُّفْعَةِ المُصَدَّرَةِ لِمَنْعِ تَكْرَارِ التَّرْحِيلِ إِلَى الأُسْتَاذِ العَامّ. بِتَفْعِيلِ خِيَارِ القَفْلِ عِنْدَ التَّحْمِيلْ، يَخْتِمُ النِّظَامُ كَافَّةَ الحَرَكَاتِ المُصَدَّرَةِ لِمَنْعِ تَكْرَارِ تَرْحِيلِهَا مُسْتَقْبَلاً، لِتَكْتَمِلَ بِذَلِكَ جَوْلَتُنَا التَّفَاعُلِيَّةُ الشَّامِلَةُ عَبْرَ وَحَدَاتِ مِنَصَّةِ Grow الأَرْبَعْ.',
        },
    },
}


def _validate_ar_script(label: str, txt: str):
    if "جرو" in txt:
        raise ValueError(f"[{label}] Forbidden Arabic transliteration 'جرو' found; use 'Grow'!")
    latin_tokens = [m for m in re.findall(r"[A-Za-z]+", txt) if m != "Grow"]
    if latin_tokens:
        raise ValueError(f"[{label}] Unexpected Latin tokens {latin_tokens} in Arabic script!")
    tokens = txt.split()
    for i, tok in enumerate(tokens):
        core = tok.rstrip(".,،؛:؟!\"")
        is_pausal = (len(core) != len(tok)) or (i == len(tokens) - 1)
        if is_pausal:
            if len(core) >= 2 and core[-2] == "ة" and core[-1] != "\u064b":
                raise ValueError(f"[{label}] Pausal ة must remain bare (or ـةً) on '{tok}'!")
        else:
            if core.endswith("ة"):
                raise ValueError(f"[{label}] Unvoweled non-pausal ة on '{tok}'!")
            if core.endswith("ه"):
                raise ValueError(f"[{label}] Unvoweled non-pausal ه on '{tok}'!")


def validate_scripts():
    """Validate phonetic & grammatical invariants across all English and Arabic TTS scripts."""
    spaced_acronym_re = re.compile(r"\b(?:[A-Z]\.\s+)+[A-Z]\.")
    for key, langs in PITCH_NARRATIONS.items():
        _validate_ar_script(f"PITCH_NARRATIONS[{key}]", langs["ar"])
        spaced_match = spaced_acronym_re.findall(langs["en"])
        if spaced_match:
            raise ValueError(f"Found spaced-period acronym {spaced_match} inside PITCH_NARRATIONS[{key}]['en']!")

    for tour_dir, steps_map in TOUR_STEP_NARRATIONS.items():
        for step, langs in steps_map.items():
            _validate_ar_script(f"TOUR_STEP_NARRATIONS[{tour_dir}][{step}]", langs["ar"])
            spaced_match = spaced_acronym_re.findall(langs["en"])
            if spaced_match:
                raise ValueError(f"Found spaced-period acronym {spaced_match} inside TOUR_STEP_NARRATIONS[{tour_dir}][{step}]['en']!")


def _sec_to_frame_byte(sec: float, total_len: int) -> int:
    frame_idx = max(0, int(round(sec / 0.024)))
    return min(total_len, frame_idx * FRAME_BYTES)


async def _stream_tts_with_words(text: str, voice: str, rate: str = "-2%", pitch: str = "+0Hz"):
    audio = bytearray()
    words = []
    comm = edge_tts.Communicate(text=text, voice=voice, rate=rate, pitch=pitch, boundary="WordBoundary")
    async for msg in comm.stream():
        if msg["type"] == "audio":
            audio.extend(msg["data"])
        elif msg["type"] == "WordBoundary":
            words.append({
                "text": msg["text"],
                "offset_s": msg["offset"] / 10_000_000.0,
                "dur_s": msg["duration"] / 10_000_000.0,
            })
    return bytes(audio), words


async def _collect_tts_retry(text: str, voice: str, rate: str = "-2%", pitch: str = "+0Hz"):
    for attempt in range(1, 6):
        try:
            audio, words = await asyncio.wait_for(
                _stream_tts_with_words(text, voice, rate, pitch), timeout=30.0
            )
            if len(audio) > 0 and len(words) > 0:
                return audio, words
        except Exception as exc:
            print(f"[WARN] WordBoundary TTS retry {attempt} ({voice}): {repr(exc)}", flush=True)
            await asyncio.sleep(1.2 * attempt)
    raise RuntimeError(f"Failed WordBoundary TTS for {voice}: {text[:40]}")


_GROW_CLIP_CACHE = None
_GROW_LOCK = None


async def _get_english_grow_frames():
    global _GROW_CLIP_CACHE, _GROW_LOCK
    if _GROW_LOCK is None:
        _GROW_LOCK = asyncio.Lock()
    async with _GROW_LOCK:
        if _GROW_CLIP_CACHE is not None:
            return _GROW_CLIP_CACHE
        g_audio, g_words = await _collect_tts_retry(
            "Platform Grow today.", VOICE_EN, rate="-6%", pitch="-2Hz"
        )
        silence = g_audio[: FRAME_BYTES * 2]  # 48ms clean silent MP3 frames
        grow_idx = next(i for i, w in enumerate(g_words) if w["text"].lower() == "grow")
        cut_start = _sec_to_frame_byte(g_words[grow_idx]["offset_s"], len(g_audio))
        cut_end = _sec_to_frame_byte(g_words[grow_idx + 1]["offset_s"] - 0.010, len(g_audio))
        _GROW_CLIP_CACHE = silence + g_audio[cut_start:cut_end] + silence
        return _GROW_CLIP_CACHE


def _to_ar_tts_phonetics(text: str) -> str:
    """Render pausal bare ة as explicit pausal ـهْ so neural TTS never applies regional imala (-e/-yi)."""
    return re.sub(r"ة(?=[.,،؛:؟!\"]|\s*$)", "هْ", text)


async def synth_ar_with_grow(text: str) -> bytes:
    """Coarticulate Arabic construct-state prefix + authentic English 'Grow' + Arabic continuation."""
    phonetic_text = _to_ar_tts_phonetics(text)
    prefix, suffix = phonetic_text.split("Grow", 1)
    prefix = prefix.strip()
    suffix = suffix.strip()

    grow_frames = await _get_english_grow_frames()
    # Voiceless stop كَامِلَةً creates a clean oral closure right after construct-state مِنَصَّةُ / مِنَصَّةِ
    p1_audio, p1_words = await _collect_tts_retry(f"{prefix} كَامِلَةً، وَتُكْمِلُ.", VOICE_AR, rate=RATE_AR)
    # Warm-up phrase followed by comma gives mid-sentence pitch register + clean silence before suffix
    p2_audio, p2_words = await _collect_tts_retry(f"بِشَكْلٍ كَامِلْ، {suffix}", VOICE_AR, rate=RATE_AR)

    cut_p1_end = _sec_to_frame_byte(p1_words[-2]["offset_s"] + 0.012, len(p1_audio))
    cut_p2_start = _sec_to_frame_byte(p2_words[2]["offset_s"] - 0.012, len(p2_audio))
    return p1_audio[:cut_p1_end] + grow_frames + p2_audio[cut_p2_start:]


async def synth_file(out_path: str, lang: str, text: str, force: bool = False):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if not force and os.path.exists(out_path) and os.path.getsize(out_path) > 16384:
        print(f"[SKIP] {os.path.relpath(out_path, AUDIO_ROOT)} ({os.path.getsize(out_path) // 1024} KB)", flush=True)
        return

    voice = VOICE_AR if lang == "ar" else VOICE_EN
    rate = RATE_AR if lang == "ar" else RATE_EN
    tmp_path = f"{out_path}.tmp"

    if lang == "ar" and "Grow" in text:
        mp3_bytes = await synth_ar_with_grow(text)
        with open(tmp_path, "wb") as f:
            f.write(mp3_bytes)
        os.replace(tmp_path, out_path)
        print(f"[OK]   {os.path.relpath(out_path, AUDIO_ROOT)} [{voice}+Grow {rate}] ({os.path.getsize(out_path) // 1024} KB)", flush=True)
        return

    tts_text = _to_ar_tts_phonetics(text) if lang == "ar" else text

    for attempt in range(1, 6):
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            communicate = edge_tts.Communicate(text=tts_text, voice=voice, rate=rate)
            await asyncio.wait_for(communicate.save(tmp_path), timeout=45.0)
            os.replace(tmp_path, out_path)
            print(f"[OK]   {os.path.relpath(out_path, AUDIO_ROOT)} [{voice} {rate}] ({os.path.getsize(out_path) // 1024} KB)", flush=True)
            return
        except Exception as exc:
            print(f"[WARN] {os.path.basename(out_path)} attempt {attempt} failed: {repr(exc)}", flush=True)
            await asyncio.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to synthesize {out_path}")


async def main():
    validate_scripts()
    force_all = "--force-all" in sys.argv
    only_ar = "--only-ar" in sys.argv
    only_en = "--only-en" in sys.argv
    target_langs = ("ar",) if only_ar else (("en",) if only_en else ("en", "ar"))
    sem = asyncio.Semaphore(4)

    async def bounded_synth(out_path: str, lang: str, text: str):
        async with sem:
            await synth_file(out_path, lang, text, force=force_all)

    tasks = []
    for tour_dir, steps_map in TOUR_STEP_NARRATIONS.items():
        target_dir = os.path.join(AUDIO_ROOT, tour_dir)
        os.makedirs(target_dir, exist_ok=True)
        for step, langs in steps_map.items():
            for lang in target_langs:
                out_path = os.path.join(target_dir, f"step{step}_{lang}.mp3")
                tasks.append(bounded_synth(out_path, lang, langs[lang]))

    for key, langs in PITCH_NARRATIONS.items():
        for lang in target_langs:
            out_path = os.path.join(PITCHES_DIR, f"{key}_{lang}.mp3")
            tasks.append(bounded_synth(out_path, lang, langs[lang]))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
