# c:\Grow\app\initializers\seed_db.py

import os
import sys
import importlib.util
import importlib.machinery
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import text
import csv
import io
import traceback
from datetime import datetime, date, timedelta

# --- Path Configuration (CRITICAL ADDITION) ---
# This block ensures Python can find your 'app' package.
# It calculates the project root directory (e.g., 'c:\Grow').
# If your project structure is 'Grow/app/initializers/seed_db.py',
# then 'current_dir' is '.../app/initializers'.
# 'os.path.join(current_dir, "..")' goes to '.../app'.
# 'os.path.join(current_dir, "..", "..")' goes to '.../Grow'.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..')) # Go up two levels from 'initializers' to 'Grow'

# Add the project root to sys.path if it's not already there.
# This makes 'app' directly importable as a top-level package.
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- END Path Configuration ---

try:
    # Now, with project_root added to sys.path, direct imports from 'app' should work.
    from app.database import SessionLocal, Base # Import Base as well, needed for metadata
    import app.models as models # Import models to ensure all models are registered
    from app.crud.crud import (
        crud_subscription_plan,
        crud_customer,
        crud_user,
        crud_customer_entity,
        crud_global_configuration,
        crud_currency, crud_lg_type, crud_rule, crud_issuing_method,
        crud_lg_status, crud_lg_operational_status,
        crud_bank, crud_template,
        crud_permission, crud_role_permission,
        crud_lg_category,
        crud_internal_owner_contact,
        crud_lg_record,
        crud_approval_request,
        crud_lg_instruction,
        crud_lg_instruction,
        crud_lg_migration,
        log_action,
    )
    from app.schemas.all_schemas import (
        SubscriptionPlanCreate,
        UserCreate,
        CustomerEntityCreate,
        GlobalConfigurationCreate,
        CurrencyCreate, LgTypeCreate, RuleCreate, IssuingMethodCreate, LgStatusCreate,
        LgOperationalStatusCreate, 
        BankCreate, TemplateCreate,
        PermissionCreate, RolePermissionCreate,
        LGCategoryCreate,
        InternalOwnerContactCreate,
        LGRecordCreate,
        CustomerCreate,
        LGInstructionCreate,
        LGInstructionCreate
    )
    from app.constants import ( # Make sure all constants are imported with app. prefix
        UserRole, GlobalConfigKey, ACTION_TYPE_LG_DECREASE_AMOUNT,
        ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT, ACTION_TYPE_LG_REMINDER_TO_BANKS,
        ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE,
        ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE, NOTIFICATION_PRINT_CONFIRMATION, ACTION_TYPE_LG_AMEND,
        # New constants for renewal reminders
        ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST, ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND,
        ACTION_TYPE_LG_REMINDER_TO_INTERNAL_OWNER,
        InstructionTypeCode, SubInstructionCode # NEW: Import for serial generation
    )


except Exception as e:
    print(f"FATAL ERROR: Could not import core modules. Ensure project structure and run command are correct. Error: {e}")
    traceback.print_exc()
    sys.exit(1)

# --- CSV Data Content for Banks (already fixed in previous turn) ---
BANKS_CSV_CONTENT = """
,,,,,,,,
,,,,,,,,
,,,,,,,,
,,,,,,,,
,Bank Name,Code,Head Office Address,Historical Name Changes,SWIFT Code,Phone,Fax,Current Owner
,National Bank of Egypt (NBE),NBE,"1187, Cornich El Nile St., Cairo.",,NBEGEGCX,19623,00202 2574 9624,Egyptian Government
,Banque Misr,BM,"151, Mohamed Farid St., Cairo.",,BMISEGCX,19838,00202 2393 0200,Egyptian Government
,Commercial International Bank (CIB),CIB,"21/23 Charles Du Gaulle St., (ex Giza St.,), Nile Tower, Giza.",,CIBEEGCX,19666,00202 3336 0610,"Actis, EFG Hermes, Fairfax"
,QNB Al Ahli,QNB,"Dar Champellion, 5, Champellion St., Downtown, Cairo.",National Société Générale Bank (NSGB),QNBAEGCX,16516,00202 2391 9000,Qatar National Bank (QNB)
,Arab African International Bank,AAIB,"5, El Saray El Kubra St., Garden City, Cairo.",Bank of Nova Scotia Egypt,ARAIEGCX,19555,00202 2794 6000,"Central Bank of Egypt, Kuwait Investment Auth."
,Banque Du Caire,BDC,"6, Dr. Moustafa Abu Zahra St., Nasr city, Cairo.",,BDCAEGCX,19777,00202 2403 0303,Egyptian Government (majority)
,Bank of Alexandria,ALEX,"49, Kasr El Nile St., Cairo.",Intesa Sanpaolo,ALEXEGCX,19033,00202 2392 7000,Intesa Sanpaolo (Italy)
,Emirates NBD Egypt,ENBD,"Plot 85 Block G - City center- Sector A - Road 90 - Fifth District - Cairo.",BNP Paribas Egypt,EBONEECA,00202 2579 0000,00202 2579 0001,Emirates NBD (UAE)
,Attijariwafa Bank Egypt,ATTIJ,"Star Capital A1 Tower, City Stars, 2 Ali Rashed St., Nasr City, Cairo.",Barclays Bank Egypt,ATTJEGCX,00202 2480 0000,00202 2480 0001,Attijariwafa Bank (Morocco)
,HSBC Bank Egypt,HSBC,"306, Cornich El Nile St., El Maadi, Cairo.",British Bank of the Middle East (BBME),HSBCEGCX,00202 2528 0000,00202 2528 0001,HSBC Holdings (UK)
,Crédit Agricole Egypt,CAE,"Touristic area No. (9/10/11/12/13) Fifth Settlement, Cairo.",Crédit Agricole Indosuez - Calyon Egypt,AGRIEGCX,00202 2565 0000,00202 2565 0001,Crédit Agricole Group (France)
,Al Ahli Bank of Kuwait - Egypt,ABK,"Smart village - Kilo 28 Cairo - Alex. desert road - building 227B & 228B 6th of October.",Piraeus Bank Egypt,ECBAEGCAXXX,19322,00202 3538 1000,Al Ahli Bank of Kuwait
,First Abu Dhabi Bank - Misr,FAB,"84, 90th Street, Fifth Settlement, 11835, P.O. Box 278",National Bank of Abu Dhabi (NBAD) Egypt - Cairo Far East Bank - Bank Audi Egypt - National Bank of Greece - Egypt,FABCEGCX,00202 2578 0000,00202 2578 0001,First Abu Dhabi Bank (UAE)
,Abu Dhabi Islamic Bank - Egypt,ADIB,"9 Rostom St., Garden City, Cairo.",,ABDIEGCAXXX,19951,00202 2792 2000,Abu Dhabi Islamic Bank (UAE)
,Arab Bank Egypt,ARAB,"Plot 43 sector 1-5th Settlement - New Cairo - Cairo.",,ARABEGCX,19100,00202 2614 0000,Arab Bank PLC (Jordan)
,Mashreq Bank Egypt,MSHQ,"Block No. 77, 90 St., The Fifth Compound, New Cairo.",,MSHQEGCA,00202 2539 0000,00202 2539 0001,Mashreq Bank (UAE)
,National Bank of Kuwait - Egypt,NBK,"Plot No. 155, City Center, First Sector, 5th Settlement, New Cairo, Cairo.",,NBKEEGCX,00202 2576 0000,00202 2576 0001,National Bank of Kuwait
,Suez Canal Bank,SCB,"7, 9 Abdel Kader Hamza St., Garden City, Cairo.",,SUEZEGCX,00202 2794 0000,00202 2794 0040,Egyptian Government (majority)
,Faisal Islamic Bank,FAIS,"3-26 July St., Cairo.",,FAISEGCX,00202 2391 0000,00202 2391 0010,Faisal Islamic Holding (Saudi Arabia)
,Housing & Development Bank,HDB,"26 El Krom St., Mohandessin, Dokki Police Station, Giza.",,HDBKEGCX,00202 3303 6000,00202 3303 6050,Egyptian Government
,Al Baraka Bank Egypt,BBE,"29, 90th St. (south), city center Fifth Settlement New Cairo",Egyptian Saudi Finance Bank (ESFB),ABRKEGCAXXX,19373,00202 2120 5858,Al Baraka Banking Group (Bahrain)
,Société Arabe Int. de Banque (SAIB),SAIB,"56, Gameat El Dewal Al Arabia St., Mohandessin, Giza.",,SAIBEGCX,00202 3303 5000,00202 3303 5050,Mixed (public/private)
,Export Development Bank of Egypt,EDB,"78, 90th St. (South), 5th District, New Cairo.",,EDBEEGCX,00202 2565 0000,00202 2565 0001,Egyptian Government
,Egyptian Arab Land Bank,EALB,"78 Gameat El Dewal El Arabia St., Mohandessin, Giza.",,EALBEGCX,00202 3302 8000,00202 3302 8001,Egyptian Government
,Agricultural Bank of Egypt,AGRI,"1, El Seid Club St., Dokki, Giza.",,BDACEGCA,00202 3335 9000,00202 3336 0204,Egyptian Government
,Industrial Development Bank,IDB,"2 Abdel Kader Hamza street, Cairo Centre Building, Garden City, Cairo",,IDBBEGCX,00202 2795 0000,00202 2795 0050,Egyptian Government
,The United Bank,THEUB,"106, El Kasr El Einy St., (Cairo Center Tower), Cairo.",,UNTIEGCX,00202 2393 0000,00202 2393 0049,Egyptian Government
,MIDBank,MIDB,"21/23 Charles De Gaulle St., (ex Giza st.,), Nile Tower, Giza.",,MIDBEGCX,00202 3336 0000,00202 3336 0040,Banque Du Caire
,Abu Dhabi Commercial Bank - Egypt,ADCB,"16 Gammat El Dowel el Arabia Street, Giza.",Alexandria Commercial & Maritime Bank (ACMB),ADCBEGCX,16862,00202 3828 0701,Abu Dhabi Commercial Bank (UAE)
,Egyptian Gulf Bank,EGB,"Block 45, North Teseen Road, 5th settlement, New Cairo.",,EGUBEGCX,00202 2568 0000,00202 2568 0001,Private investors
,Arab International Bank,ARIB,"77 B Nasr Road , Nasr City, Cairo, Arab Republic of Egypt.",,ARIBEGCX,00202 2393 2000,00202 2393 2050,Libyan & Egyptian governments
,CitiBank Egypt,CITI,"46, Al Salam Axis Street, First Sector at the Fifth Settlement, New Cairo.",,CITIEGCX,00202 2461 0000,00202 2461 0001,Citigroup (USA)
,Standard Chartered Egypt,SCEG,"Administrative office building at Cairo Festival City No. 12b03/A- New Cairo",,SCBLEGCX,00202 2461 0000,00202 2461 0001,Standard Chartered (UK)
,Kuwait Finance House - Egypt,KFH,"81, Ninety St., City Center, The Fifth Compound, New Cairo.",Ahli United Bank,KFHHEGCX,00202 2614 9500,00202 2614 9600,Kuwait Finance House
,Bank NXT,NXT,"8, Abdel Khalek Sarwat St., (Cairo-Sky Building), Cairo.",Arab Investment Bank,ARIBEGCX,16697,00202 2586 1100,Private investors
,Arab Banking Corporation - Egypt S.A.E (ABC Egypt),ABC,"90th St. (North), Fifth settlement, New Cairo",Egypt Arab African Bank - BLOM Bank Egypt,EAABEGCX,00202 2586 1199,00202 2811 1555,"Arab Banking Corporation (ABC) Group, Bahrain"
"""

# FIXED: Replaced CSV content with direct list for robustness
CURRENCIES_LIST_DATA = [
    {"name": "Egyptian Pound", "iso_code": "EGP", "symbol": "E£"},
    {"name": "US Dollar", "iso_code": "USD", "symbol": "$"},
    {"name": "Euro", "iso_code": "EUR", "symbol": "€"},
    {"name": "Pound Sterling", "iso_code": "GBP", "symbol": "£"},
    {"name": "Swiss Franc", "iso_code": "CHF", "symbol": "CHF"},
    {"name": "UAE Dirham", "iso_code": "AED", "symbol": "د.إ"},
    {"name": "Saudi Riyal", "iso_code": "SAR", "symbol": "ر.س"},
    {"name": "Afghani", "iso_code": "AFN", "symbol": "Af"},
    {"name": "Lek", "iso_code": "ALL", "symbol": "L"},
    {"name": "Algerian Dinar", "iso_code": "DZD", "symbol": "دج"},
    {"name": "Kwanza", "iso_code": "AOA", "symbol": "Kz"},
    {"name": "East Caribbean Dollar", "iso_code": "XCD", "symbol": "EC$"},
    {"name": "Argentine Peso", "iso_code": "ARS", "symbol": "$"},
    {"name": "Armenian Dram", "iso_code": "AMD", "symbol": "֏"},
    {"name": "Aruban Florin", "iso_code": "AWG", "symbol": "ƒ"},
    {"name": "Australian Dollar", "iso_code": "AUD", "symbol": "A$"},
    {"name": "Azerbaijanian Manat", "iso_code": "AZN", "symbol": "₼"},
    {"name": "Bahamian Dollar", "iso_code": "BSD", "symbol": "B$"},
    {"name": "Bahraini Dinar", "iso_code": "BHD", "symbol": ".د.ب"},
    {"name": "Taka", "iso_code": "BDT", "symbol": "৳"},
    {"name": "Barbados Dollar", "iso_code": "BBD", "symbol": "Bds$"},
    {"name": "Belarussian Ruble", "iso_code": "BYN", "symbol": "Br"},
    {"name": "Belize Dollar", "iso_code": "BZD", "symbol": "BZ$"},
    {"name": "CFA Franc BCEAO", "iso_code": "XOF", "symbol": "CFA"},
    {"name": "Bermudian Dollar", "iso_code": "BMD", "symbol": "BMD"},
    {"name": "Ngultrum", "iso_code": "BTN", "symbol": "Nu."},
    {"name": "Boliviano", "iso_code": "BOB", "symbol": "Bs."},
    {"name": "Mvdol", "iso_code": "BOV", "symbol": "BOV"},
    {"name": "Convertible Mark", "iso_code": "BAM", "symbol": "KM"},
    {"name": "Pula", "iso_code": "BWP", "symbol": "P"},
    {"name": "Brazilian Real", "iso_code": "BRL", "symbol": "R$"},
    {"name": "Brunei Dollar", "iso_code": "BND", "symbol": "B$"},
    {"name": "Bulgarian Lev", "iso_code": "BGN", "symbol": "лв"},
    {"name": "Burundi Franc", "iso_code": "BIF", "symbol": "FBu"},
    {"name": "Cabo Verde Escudo", "iso_code": "CVE", "symbol": "Esc"},
    {"name": "Riel", "iso_code": "KHR", "symbol": "៛"},
    {"name": "CFA Franc BEAC", "iso_code": "XAF", "symbol": "FCFA"},
    {"name": "Canadian Dollar", "iso_code": "CAD", "symbol": "C$"},
    {"name": "Cayman Islands Dollar", "iso_code": "KYD", "symbol": "CI$"},
    {"name": "Unidad de Fomento", "iso_code": "CLF", "symbol": "UF"},
    {"name": "Chilean Peso", "iso_code": "CLP", "symbol": "CLP$"},
    {"name": "Yuan Renminbi", "iso_code": "CNY", "symbol": "¥"},
    {"name": "Colombian Peso", "iso_code": "COP", "symbol": "COL$"},
    {"name": "Unidad de Valor Real", "iso_code": "COU", "symbol": "COU"},
    {"name": "Comoro Franc", "iso_code": "KMF", "symbol": "CF"},
    {"name": "Congolese Franc", "iso_code": "CDF", "symbol": "FC"},
    {"name": "Costa Rican Colon", "iso_code": "CRC", "symbol": "₡"},
    {"name": "Peso Convertible", "iso_code": "CUC", "symbol": "CUC$"},
    {"name": "Cuban Peso", "iso_code": "CUP", "symbol": "₱"},
    {"name": "Czech Koruna", "iso_code": "CZK", "symbol": "Kč"},
    {"name": "Danish Krone", "iso_code": "DKK", "symbol": "kr."},
    {"name": "Djibouti Franc", "iso_code": "DJF", "symbol": "Fdj"},
    {"name": "Dominican Peso", "iso_code": "DOP", "symbol": "RD$"},
    {"name": "El Salvador Colon", "iso_code": "SVC", "symbol": "₡"},
    {"name": "Nakfa", "iso_code": "ERN", "symbol": "Nfk"},
    {"name": "Ethiopian Birr", "iso_code": "ETB", "symbol": "Br"},
    {"name": "Falkland Islands Pound", "iso_code": "FKP", "symbol": "£"},
    {"name": "Fiji Dollar", "iso_code": "FJD", "symbol": "FJ$"},
    {"name": "CFP Franc", "iso_code": "XPF", "symbol": "₣"},
    {"name": "Dalasi", "iso_code": "GMD", "symbol": "D"},
    {"name": "Lari", "iso_code": "GEL", "symbol": "₾"},
    {"name": "Ghana Cedi", "iso_code": "GHS", "symbol": "₵"},
    {"name": "Gibraltar Pound", "iso_code": "GIP", "symbol": "£"},
    {"name": "Quetzal", "iso_code": "GTQ", "symbol": "Q"},
    {"name": "Guinea Franc", "iso_code": "GNF", "symbol": "FG"},
    {"name": "Guyana Dollar", "iso_code": "GYD", "symbol": "GYD"},
    {"name": "Gourde", "iso_code": "HTG", "symbol": "G"},
    {"name": "Lempira", "iso_code": "HNL", "symbol": "L"},
    {"name": "Hong Kong Dollar", "iso_code": "HKD", "symbol": "HK$"},
    {"name": "Forint", "iso_code": "HUF", "symbol": "Ft"},
    {"name": "Iceland Krona", "iso_code": "ISK", "symbol": "kr"},
    {"name": "Indian Rupee", "iso_code": "INR", "symbol": "₹"},
    {"name": "Rupiah", "iso_code": "IDR", "symbol": "Rp"},
    {"name": "SDR (Special Drawing Right)", "iso_code": "XDR", "symbol": "XDR"},
    {"name": "Iranian Rial", "iso_code": "IRR", "symbol": "﷼"},
    {"name": "Iraqi Dinar", "iso_code": "IQD", "symbol": "ع.د"},
    {"name": "New Israeli Sheqel", "iso_code": "ILS", "symbol": "₪"},
    {"name": "Jamaican Dollar", "iso_code": "JMD", "symbol": "J$"},
    {"name": "Yen", "iso_code": "JPY", "symbol": "¥"},
    {"name": "Jordanian Dinar", "iso_code": "JOD", "symbol": "JD"},
    {"name": "Tenge", "iso_code": "KZT", "symbol": "₸"},
    {"name": "Kenyan Shilling", "iso_code": "KES", "symbol": "KSh"},
    {"name": "North Korean Won", "iso_code": "KPW", "symbol": "₩"},
    {"name": "Won", "iso_code": "KRW", "symbol": "₩"},
    {"name": "Kuwaiti Dinar", "iso_code": "KWD", "symbol": "KD"},
    {"name": "Som", "iso_code": "KGS", "symbol": "сom"},
    {"name": "Kip", "iso_code": "LAK", "symbol": "₭"},
    {"name": "Lebanese Pound", "iso_code": "LBP", "symbol": "L.L."},
    {"name": "Loti", "iso_code": "LSL", "symbol": "L"},
    {"name": "Liberian Dollar", "iso_code": "LRD", "symbol": "L$"},
    {"name": "Libyan Dinar", "iso_code": "LYD", "symbol": "ل.د"},
    {"name": "Pataca", "iso_code": "MOP", "symbol": "MOP$"},
    {"name": "Malagasy Ariary", "iso_code": "MGA", "symbol": "Ar"},
    {"name": "Kwacha", "iso_code": "MWK", "symbol": "MK"},
    {"name": "Malaysian Ringgit", "iso_code": "MYR", "symbol": "RM"},
    {"name": "Rufiyaa", "iso_code": "MVR", "symbol": "Rf"},
    {"name": "Ouguiya", "iso_code": "MRU", "symbol": "UM"},
    {"name": "Mauritius Rupee", "iso_code": "MUR", "symbol": "Rs"},
    {"name": "ADB Unit of Account", "iso_code": "XUA", "symbol": "XUA"},
    {"name": "Mexican Peso", "iso_code": "MXN", "symbol": "Mex$"},
    {"name": "Mexican Unidad de Inversion (UDI)", "iso_code": "MXV", "symbol": "MXV"},
    {"name": "Moldovan Leu", "iso_code": "MDL", "symbol": "L"},
    {"name": "Tugrik", "iso_code": "MNT", "symbol": "₮"},
    {"name": "Moroccan Dirham", "iso_code": "MAD", "symbol": "د.م."},
    {"name": "Mozambique Metical", "iso_code": "MZN", "symbol": "MT"},
    {"name": "Kyat", "iso_code": "MMK", "symbol": "K"},
    {"name": "Namibia Dollar", "iso_code": "NAD", "symbol": "N$"},
    {"name": "Nepalese Rupee", "iso_code": "NPR", "symbol": "₨"},
    {"name": "New Zealand Dollar", "iso_code": "NZD", "symbol": "NZ$"},
    {"name": "Cordoba Oro", "iso_code": "NIO", "symbol": "C$"},
    {"name": "Naira", "iso_code": "NGN", "symbol": "₦"},
    {"name": "Norwegian Krone", "iso_code": "NOK", "symbol": "kr"},
    {"name": "Rial Omani", "iso_code": "OMR", "symbol": "ر.ع."},
    {"name": "Pakistan Rupee", "iso_code": "PKR", "symbol": "₨"},
    {"name": "Balboa", "iso_code": "PAB", "symbol": "B/."},
    {"name": "Kina", "iso_code": "PGK", "symbol": "K"},
    {"name": "Guarani", "iso_code": "PYG", "symbol": "₲"},
    {"name": "Nuevo Sol", "iso_code": "PEN", "symbol": "S/."},
    {"name": "Philippine Peso", "iso_code": "PHP", "symbol": "₱"},
    {"name": "Zloty", "iso_code": "PLN", "symbol": "zł"},
    {"name": "Qatari Rial", "iso_code": "QAR", "symbol": "ر.ق"},
    {"name": "Denar", "iso_code": "MKD", "symbol": "ден"},
    {"name": "Romanian Leu", "iso_code": "RON", "symbol": "lei"},
    {"name": "Russian Ruble", "iso_code": "RUB", "symbol": "₽"},
    {"name": "Rwanda Franc", "iso_code": "RWF", "symbol": "RF"},
    {"name": "Saint Helena Pound", "iso_code": "SHP", "symbol": "£"},
    {"name": "Tala", "iso_code": "WST", "symbol": "WS$"},
    {"name": "Dobra", "iso_code": "STN", "symbol": "Db"},
    {"name": "Serbian Dinar", "iso_code": "RSD", "symbol": "дин."},
    {"name": "Seychelles Rupee", "iso_code": "SCR", "symbol": "SR"},
    {"name": "Leone", "iso_code": "SLE", "symbol": "Le"},
    {"name": "Singapore Dollar", "iso_code": "SGD", "symbol": "S$"},
    {"name": "Caribbean guilder", "iso_code": "XCG", "symbol": "ƒ"},
    {"name": "Sucre", "iso_code": "XSU", "symbol": "Sucre"},
    {"name": "Solomon Islands Dollar", "iso_code": "SBD", "symbol": "SI$"},
    {"name": "Somali Shilling", "iso_code": "SOS", "symbol": "Sh.So."},
    {"name": "Rand", "iso_code": "ZAR", "symbol": "R"},
    {"name": "South Sudanese Pound", "iso_code": "SSP", "symbol": "SS£"},
    {"name": "Sri Lanka Rupee", "iso_code": "LKR", "symbol": "Rs"},
    {"name": "Sudanese Pound", "iso_code": "SDG", "symbol": "ج.س."},
    {"name": "Surinam Dollar", "iso_code": "SRD", "symbol": "$"},
    {"name": "Lilangeni", "iso_code": "SZL", "symbol": "L"},
    {"name": "Swedish Krona", "iso_code": "SEK", "symbol": "kr"},
    {"name": "WIR Euro", "iso_code": "CHE", "symbol": "CHE"},
    {"name": "WIR Franc", "iso_code": "CHW", "symbol": "CHW"},
    {"name": "Syrian Pound", "iso_code": "SYP", "symbol": "£"},
    {"name": "New Taiwan Dollar", "iso_code": "TWD", "symbol": "NT$"},
    {"name": "Somoni", "iso_code": "TJS", "symbol": "SM"},
    {"name": "Tanzanian Shilling", "iso_code": "TZS", "symbol": "TSh"},
    {"name": "Baht", "iso_code": "THB", "symbol": "฿"},
    {"name": "Paanga", "iso_code": "TOP", "symbol": "T$"},
    {"name": "Trinidad and Tobago Dollar", "iso_code": "TTD", "symbol": "TT$"},
    {"name": "Tunisian Dinar", "iso_code": "TND", "symbol": "د.ت"},
    {"name": "Turkish Lira", "iso_code": "TRY", "symbol": "₺"},
    {"name": "Turkmenistan New Manat", "iso_code": "TMT", "symbol": "m"},
    {"name": "Uganda Shilling", "iso_code": "UGX", "symbol": "USh"},
    {"name": "Hryvnia", "iso_code": "UAH", "symbol": "₴"},
    {"name": "US Dollar (Next day)", "iso_code": "USN", "symbol": "USN"},
    {"name": "Uruguay Peso en Unidades Indexadas (URUIURUI)", "iso_code": "UYI", "symbol": "UYI"},
    {"name": "Peso Uruguayo", "iso_code": "UYU", "symbol": "$U"},
    {"name": "Uzbekistan Sum", "iso_code": "UZS", "symbol": "лв"},
    {"name": "Vatu", "iso_code": "VUV", "symbol": "Vt"},
    {"name": "Bolivar", "iso_code": "VEF", "symbol": "Bs"},
    {"name": "Bolivar", "iso_code": "VED", "symbol": "Bs.D."},
    {"name": "Dong", "iso_code": "VND", "symbol": "₫"},
    {"name": "Yemeni Rial", "iso_code": "YER", "symbol": "﷼"},
    {"name": "Zambian Kwacha", "iso_code": "ZMW", "symbol": "ZK"},
    {"name": "Zimbabwe Dollar", "iso_code": "ZWL", "symbol": "Z$"},
]

# Define initial permissions
INITIAL_PERMISSIONS = [
    {"name": "system_owner:view_dashboard", "description": "Allows viewing the System Owner dashboard."},
    {"name": "system_owner:view_scheduler", "description": "Allows viewing background tasks schedule."},
    {"name": "system_notification:create", "description": "Allows creating a notification."},
    {"name": "system_notification:edit", "description": "Allows editing a notification."},
    {"name": "system_notification:delete", "description": "Allows deleting a notification."},
    {"name": "system_owner:run_scheduler_job", "description": "Allows running scheduled jobs."},
    {"name": "legal_artifact:create", "description": "Allows posting new terms and conditions and privacy policy."},
    {"name": "subscription_plan:view", "description": "Allows viewing subscription plans."},
    {"name": "subscription_plan:create", "description": "Allows creating new subscription plans."},
    {"name": "subscription_plan:edit", "description": "Allows editing existing subscription plans."},
    {"name": "subscription_plan:delete", "description": "Allows soft-deleting subscription plans."},
    {"name": "customer:view", "description": "Allows viewing customer details."},
    {"name": "customer:create", "description": "Allows onboarding new customers."},
    {"name": "customer:edit", "description": "Allows editing customer details (basic info, plan)."},
    {"name": "customer:delete", "description": "Allows soft-deleting customers."},
    {"name": "customer_entity:view", "description": "Allows viewing customer entities."},
    {"name": "customer_entity:create", "description": "Allows creating new customer entities."},
    {"name": "customer_entity:edit", "description": "Allows editing customer entity details."},
    {"name": "customer_entity:delete", "description": "Allows soft-deleting customer entities."},
    {"name": "global_config:view", "description": "Allows viewing global configurations."},
    {"name": "global_config:create", "description": "Allows creating new global configurations."},
    {"name": "global_config:edit", "description": "Allows editing global configurations."},
    {"name": "global_config:delete", "description": "Allows soft-deleting global configurations."},
    {"name": "bank:view", "description": "Allows viewing bank list."},
    {"name": "bank:create", "description": "Allows creating new banks."},
    {"name": "bank:edit", "description": "Allows editing bank details."},
    {"name": "bank:delete", "description": "Allows soft-deleting banks."},
    {"name": "currency:view", "description": "Allows viewing currency list."},
    {"name": "currency:create", "description": "Allows creating new currencies."},
    {"name": "currency:edit", "description": "Allows editing currency details."},
    {"name": "currency:delete", "description": "Allows soft-deleting currencies."},
    {"name": "lg_type:view", "description": "Allows viewing LG type list."},
    {"name": "lg_type:create", "description": "Allows creating new LG types."},
    {"name": "lg_type:edit", "description": "Allows editing LG types."},
    {"name": "lg_type:delete", "description": "Allows soft-deleting LG types."},
    {"name": "rule:view", "description": "Allows viewing rule list."},
    {"name": "rule:create", "description": "Allows creating new rules."},
    {"name": "rule:edit", "description": "Allows editing rules."},
    {"name": "rule:delete", "description": "Allows soft-deleting rules."},
    {"name": "issuing_method:view", "description": "Allows viewing issuing method list."},
    {"name": "issuing_method:create", "description": "Allows creating new issuing methods."},
    {"name": "issuing_method:edit", "description": "Allows editing issuing methods."},
    {"name": "issuing_method:delete", "description": "Allows soft-deleting issuing methods."},
    {"name": "lg_status:view", "description": "Allows viewing LG status list."},
    {"name": "lg_status:create", "description": "Allows creating new LG statuses."},
    {"name": "lg_status:edit", "description": "Allows editing LG statuses."},
    {"name": "lg_status:delete", "description": "Allows soft-deleting LG statuses."},
    {"name": "lg_operational_status:view", "description": "Allows viewing LG operational status list."},
    {"name": "lg_operational_status:create", "description": "Allows creating new LG operational statuses."},
    {"name": "lg_operational_status:edit", "description": "Allows editing LG operational statuses."},
    {"name": "lg_operational_status:delete", "description": "Allows soft-deleting LG operational statuses."},
    {"name": "universal_category:view", "description": "Allows viewing universal category list."},
    {"name": "universal_category:create", "description": "Allows creating new universal categories."},
    {"name": "universal_category:edit", "description": "Allows editing universal categories."},
    {"name": "universal_category:delete", "description": "Allows soft-deleting universal categories."},
    {"name": "template:view", "description": "Allows viewing template list."},
    {"name": "template:create", "description": "Allows creating new templates."},
    {"name": "template:edit", "description": "Allows editing templates."},
    {"name": "template:delete", "description": "Allows soft-deleting templates."},
    {"name": "audit_log:view", "description": "Allows viewing audit logs."},
    {"name": "lg_record:create", "description": "Allows creating new LG records."},
    {"name": "lg_record:view_own", "description": "Allows viewing own LG records."},
    {"name": "lg_record:view_all", "description": "Allows viewing all LG records within customer organization."},
    {"name": "lg_record:extend", "description": "Allows extending LG records."},
    {"name": "lg_record:amend", "description": "Allows amending LG records."},
    {"name": "lg_record:liquidate", "description": "Allows liquidating LG records."},
    {"name": "lg_record:release", "description": "Allows releasing LG records."},
    {"name": "lg_record:decrease_amount", "description": "Allows decreasing the amount of LG records."},
    {"name": "lg_instruction:update_status", "description": "Allows updating status of LG instructions (e.g., delivered, confirmed)."},
    {"name": "lg_instruction:cancel_last", "description": "Allows canceling the last issued LG instruction."},
    {"name": "lg_instruction:send_reminder", "description": "Allows sending reminder instructions for LGs."},
    {"name": "lg_record:activate_non_operative", "description": "Allows Activating none operative LG"},
    {"name": "user:create", "description": "Allows Corporate Admins to create new users for their customer."},
    {"name": "user:view", "description": "Allows Corporate Admins to view users for their customer."},
    {"name": "user:edit", "description": "Allows Corporate Admins to edit users for their customer."},
    {"name": "user:delete", "description": "Allows Corporate Admins to delete users for their customer."},
    {"name": "corporate_category:create", "description": "Allows Corporate Admins to create customer-specific LG categories."},
    {"name": "corporate_category:view", "description": "Allows Corporate Admins to view customer-specific LG categories."},
    {"name": "corporate_category:edit", "description": "Allows Corporate Admins to edit customer-specific LG categories."},
    {"name": "corporate_category:delete", "description": "Allows soft-deleting LG categories."},
    {"name": "approval_request:view_all", "description": "Allows viewing all approval requests."},
    {"name": "approval_request:approve", "description": "Allows approving pending approval requests."},
    {"name": "approval_request:reject", "description": "Allows rejecting pending approval requests."},
    {"name": "maker_checker:approve", "description": "Allows approving maker-checker tasks."},
    {"name": "customer_config:view", "description": "Allows Corporate Admins to view their customer's operational configurations."},
    {"name": "customer_config:edit", "description": "Allows Corporate Admins to edit their customer's operational configurations."},
    {"name": "email_setting:manage", "description": "Manage customer-specific email sending settings"},
    {"name": "lg_document:view", "description": "Allows viewing of LG documents and generating signed URLs for private access."},
    # NEW PERMISSIONS FOR AUTH_V2 ADMIN ROLES
    {"name": "user:manage_passwords", "description": "Allows System Owners to set/reset any user's password."},
    {"name": "audit_log:view_auth", "description": "Allows viewing authentication-specific audit logs."},
    {"name": "internal_owner_contact:view", "description": "Internal owner view details for reporting use."},
    {"name": "action_center:view", "description": "Allow corporate admins to view action center pending actions"},
    {"name": "system_notification:view", "description": "Allow corporate admins and end users to view notifications"},
    {"name": "system_owner:resume_scheduler_job", "description": "Allow manually running background tasks"},
    {"name": "system_owner:reschedule_scheduler_job", "description": "Allow manually running background tasks"},
    {"name": "lg_record:ai_scan", "description": "Allow scan new lg record"},
    {"name": "lg_instruction:cancel", "description": "Allow cancelling last trasnaction"},
    {"name": "report:lg_type_mix:view", "description": "Allows viewing LG Type Mix report."},
    {"name": "report:avg_bank_processing_time:view", "description": "Allows viewing Average Bank Processing Time report."},
    {"name": "report:bank_market_share:view", "description": "Allows viewing Bank Market Share report."},

]

# Define role-permission mappings
ROLE_PERMISSIONS_MAPPING = {
    UserRole.SYSTEM_OWNER.value: [p["name"] for p in INITIAL_PERMISSIONS],
    UserRole.CORPORATE_ADMIN.value: [
        "customer_entity:view", "customer_entity:create", "customer_entity:edit", "customer_entity:delete",
        "user:create", "user:view", "user:edit", "user:delete",
        "corporate_category:create", "corporate_category:view", "corporate_category:edit", "corporate_category:delete",
        "lg_record:view_all", "lg_record:view_own",
        "lg_record:create", "lg_record:extend", "lg_record:amend", "lg_record:liquidate", "lg_record:release", "lg_record:decrease_amount",
        "lg_instruction:update_status", "lg_instruction:cancel_last", "lg_instruction:send_reminder",
        "maker_checker:approve",
        "approval_request:view_all",
        "approval_request:approve",
        "approval_request:reject",
        "template:view",
        "customer_config:view",
        "customer_config:edit",
        "email_setting:manage","lg_document:view",
        "audit_log:view_auth","internal_owner_contact:view", "audit_log:view",
        "action_center:view", "system_notification:view",
        "report:lg_type_mix:view",
        "report:avg_bank_processing_time:view",
        "report:bank_market_share:view",
    ],
    UserRole.END_USER.value: [
        "lg_record:create", "lg_record:view_own", "lg_record:extend", "lg_record:amend",
        "lg_record:liquidate", "lg_record:release", "lg_record:decrease_amount",
        "lg_instruction:update_status", "lg_instruction:cancel_last", "lg_instruction:send_reminder",
        "template:view","lg_document:view", "internal_owner_contact:view", "system_notification:view", "lg_record:ai_scan", "lg_instruction:cancel","lg_record:activate_non_operative",
    ],
    UserRole.CHECKER.value: [
        "lg_record:view_own", "maker_checker:approve", "system_notification:view",
        "approval_request:view_all"
    ],
    UserRole.VIEWER.value: [
        "lg_record:view_all", "lg_document:view", "lg_record:view_own"
    ]
}

# Make seed_db an async function
async def seed_db():
    """
    Seeds the database with initial data for testing purposes.
    Includes subscription plans, a system owner, global configs, common lists, banks, templates,
    a sample customer, and RBAC permissions/role assignments.
    This function is idempotent and will not re-create existing data.
    """
    db: Session = SessionLocal()
    try:
        # Uncomment this line to create all tables before seeding
        Base.metadata.create_all(bind=db.get_bind()) # Use db.get_bind() to get the engine from the session
        print("DEBUG: Ensured database tables exist.")

        print("Starting database seeding (idempotent mode)...")

        # 0. Seed Permissions and Role-Permission Mappings (RBAC)
        print("\n--- Seeding RBAC Permissions and Roles ---")
        for perm_data in INITIAL_PERMISSIONS:
            try:
                existing_perm = crud_permission.get_by_name(db, perm_data["name"])
                if not existing_perm:
                    crud_permission.create(db, PermissionCreate(**perm_data))
                    print(f"  Created permission: {perm_data['name']}")
                else:
                    # Update existing permission if description differs
                    if existing_perm.description != perm_data["description"]:
                        crud_permission.update(db, existing_perm, PermissionCreate(**perm_data))
                        print(f"  Updated permission description: {perm_data['name']}")
                    else:
                        print(f"  Permission already exists: {perm_data['name']}")
                db.flush() # Flush after each permission creation/update
            except Exception as e:
                print(f"  ERROR seeding permission {perm_data['name']}: {e}")
                traceback.print_exc()
                # Do not rollback here, let the main transaction handle it.
                # If an error happens, we'll see it, and likely the main commit will fail.

        for role_name, perm_names in ROLE_PERMISSIONS_MAPPING.items():
            print(f"  Processing role: {role_name}")
            for perm_name in perm_names:
                try:
                    permission = crud_permission.get_by_name(db, perm_name)
                    if permission:
                        existing_role_perm = crud_role_permission.get_by_role_and_permission(db, role_name, permission.id)
                        if not existing_role_perm:
                            crud_role_permission.create(db, RolePermissionCreate(role=role_name, permission_id=permission.id))
                            print(f"    Assigned permission '{perm_name}' to role '{role_name}'")
                        else:
                            print(f"    Permission '{perm_name}' already assigned to role '{role_name}'")
                    else:
                        print(f"    WARNING: Permission '{perm_name}' not found for role '{role_name}'. Skipping assignment.")
                    db.flush() # Flush after each role-permission creation
                except Exception as e:
                    print(f"    ERROR assigning permission '{perm_name}' to role '{role_name}': {e}")
                    traceback.print_exc()
                    # Do not rollback here.
        db.commit() # Final commit for RBAC after all permissions and role assignments

        print("--- RBAC Seeding Complete ---")

        # 1. Seed Subscription Plans
        print("\n--- Seeding Subscription Plans ---")
        plans_to_seed = [
            {"name": "Basic LG Plan", "description": "Default plan for LG Custody with basic features.",
             "duration_months": 12, "monthly_price": 50.00, "annual_price": 500.00,
             "max_users": 5, "max_records": 100, "can_maker_checker": False,
             "can_multi_entity": False, "can_ai_integration": False, "can_image_storage": True},
            {"name": "Premium LG Plan", "description": "Premium plan with all features enabled.",
             "duration_months": 12, "monthly_price": 150.00, "annual_price": 1500.00,
             "max_users": 20, "max_records": 1000, "can_maker_checker": True,
             "can_multi_entity": True, "can_ai_integration": True, "can_image_storage": True}
        ]
        for plan_data in plans_to_seed:
            try:
                if not crud_subscription_plan.get_by_name(db, name=plan_data["name"]):
                    crud_subscription_plan.create(db, obj_in=SubscriptionPlanCreate(**plan_data))
                    print(f"  Added '{plan_data['name']}' subscription plan.")
                else:
                    print(f"  '{plan_data['name']}' subscription plan already exists.")
                db.flush() # Flush after each plan creation
            except Exception as e:
                print(f"  ERROR seeding subscription plan '{plan_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all plans are processed

        # 2. Seed System Owner User
        print("\n--- Seeding System Owner User ---")
        system_owner_email = "system.owner@example.com"
        try:
            # CORRECTED: Access User model via the 'models' module that was explicitly loaded
            existing_system_owner = db.query(models.User).filter(models.User.email == system_owner_email).first()
            if not existing_system_owner:
                # CORRECTED: Access User model via the 'models' module
                db_system_owner = models.User(
                    email=system_owner_email,
                    role=UserRole.SYSTEM_OWNER,
                    customer_id=None,
                    has_all_entity_access=True,
                    must_change_password=False
                )
                db_system_owner.set_password("SecureSystemOwnerPassword123")
                db.add(db_system_owner)
                db.flush()
                db.refresh(db_system_owner)
                log_action(db, user_id=db_system_owner.id, action_type="CREATE", entity_type="User", entity_id=db_system_owner.id, details={"email": db_system_owner.email, "role": db_system_owner.role})
                print(f"  Added System Owner user: {system_owner_email}")
            else:
                print(f"  System Owner user '{system_owner_email}' already exists.")
            db.commit() # Commit after system owner creation
        except Exception as e:
            print(f"  ERROR seeding system owner user '{system_owner_email}': {e}")
            traceback.print_exc()
            db.rollback() # Rollback on error
        
        system_owner_user = db.query(models.User).filter(models.User.email == "system.owner@example.com").first()
        system_owner_id = system_owner_user.id if system_owner_user else None

        if system_owner_id is None:
            print("FATAL ERROR: System Owner user could not be found or created. Cannot proceed with customer seeding.")
            raise Exception("System Owner user not found.")
        # 3. Seed Global Configurations
        print("\n--- Seeding Global Configurations ---")
        global_configs_to_seed = [
            {"key": GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY, "value_min": "5", "value_max": "90", "value_default": "30", "unit": "days", "description": "LGs marked for auto-renewal are renewed automatically when their days to expiry are equal to or less than this value."},
            {"key": GlobalConfigKey.AUTO_RENEW_REMINDER_START_DAYS_BEFORE_EXPIRY, "value_min": "7", "value_max": "30", "value_default": "60", "unit": "days", "description": "For LGs not marked for auto-renewal, reminders are sent when their days to expiry are equal to or less than this value."},
            {"key": GlobalConfigKey.FORCED_RENEW_DAYS_BEFORE_EXPIRY, "value_min": "5", "value_max": "90", "value_default": "15", "unit": "days", "description": "LGs not marked for auto-renewal, are renewed automatically when their days to expiry are equal to or less than this value."},
            {"key": GlobalConfigKey.NUMBER_OF_DAYS_FOR_NEXT_REMINDER, "value_min": "1", "value_max": "15", "value_default": "7", "unit": "days", "description": "For LGs that do not reach the forced renewal threshold, reminders will continue to be sent at intervals defined by Number of Days for Next Reminder until action is taken."},
            {"key": GlobalConfigKey.MAX_DAYS_FOR_LAST_INSTRUCTION_CANCELLATION, "value_min": "1", "value_max": "30", "value_default": "7", "unit": "days", "description": "Maximum allowable time frame after an instruction is issued during which a cancellation request can be initiated."},
            {"key": GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY, "value_min": "1", "value_max": "60", "value_default": "7", "unit": "days", "description": "Minimum days after instruction delivery to send reminder to bank."},
            {"key": GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE, "value_min": "1", "value_max": "60", "value_default": "3", "unit": "days", "description": "Minimum days after instruction issuance to send reminder to bank."},
            {"key": GlobalConfigKey.REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE, "value_min": "30", "value_max": "180", "value_default": "90", "unit": "days", "description": "Maximum days after instruction issuance to stop suggesting reminders."},
            {"key": GlobalConfigKey.COMMON_COMMUNICATION_LIST, "value_min": None, "value_max": None, "value_default": "[]", "unit": "emails_json_array", "description": "Common email list for corporate admin notifications in JSON array format."},
            {"key": GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS, "value_min": "1", "value_max": "30", "value_default": "7", "unit": "days", "description": "Maximum days of any approval request to stay pending after which the request should automatically be rejected."},
            # NEW: Global Configs for print reminder/escalation
            {"key": GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER, "value_min": "1", "value_max": "10", "value_default": "2", "unit": "days", "description": "Days after approval when the first reminder to print a bank letter is sent to the maker."},
            {"key": GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION, "value_min": "3", "value_max": "15", "value_default": "5", "unit": "days", "description": "Days after approval when an escalation email is sent to the maker and checker if the bank letter is still not printed."},
            # NEW: Global Configs for Renewal Reminders to Users & Admins (Feature 1)
            {"key": GlobalConfigKey.RENEWAL_REMINDER_FIRST_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "7", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the first renewal reminder to users/admins. (e.g., if auto-renew is 30 days, and this is 7 days, first reminder is at 37 days)."},
            {"key": GlobalConfigKey.RENEWAL_REMINDER_SECOND_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "14", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the second (escalation) renewal reminder to users/admins. (e.g., if auto-renew is 30 days, and this is 14 days, second reminder is at 44 days)."},
            # NEW: Auth V2 password policy configs
            {"key": GlobalConfigKey.PASSWORD_MIN_LENGTH, "value_min": "8", "value_max": "64", "value_default": "8", "unit": "characters", "description": "Minimum length for user passwords."},
            {"key": GlobalConfigKey.PASSWORD_REQUIRE_UPPERCASE, "value_min": "false", "value_max": "true", "value_default": "true", "unit": "boolean", "description": "Require at least one uppercase letter in passwords."},
            {"key": GlobalConfigKey.PASSWORD_REQUIRE_LOWERCASE, "value_min": "false", "value_max": "true", "value_default": "true", "unit": "boolean", "description": "Require at least one lowercase letter in passwords."},
            {"key": GlobalConfigKey.PASSWORD_REQUIRE_DIGIT, "value_min": "false", "value_max": "true", "value_default": "true", "unit": "boolean", "description": "Require at least one digit in passwords."},
            {"key": GlobalConfigKey.PASSWORD_RESET_TOKEN_EXPIRY_MINUTES, "value_min": "5", "value_max": "120", "value_default": "15", "unit": "minutes", "description": "Expiration time for password reset tokens in minutes."},
            {"key": GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED, "value_min": "1", "value_max": "60", "value_default": "3", "unit": "days", "description": "Minimum days after instruction issuance to stop suggesting undelivered report."},
            {"key": GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED, "value_min": "1", "value_max": "60", "value_default": "3", "unit": "days", "description": "Minimum days after instruction issuance to stop suggesting undelivered report."},
            {"key": GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE, "value_min": "1", "value_max": "60", "value_default": "3", "unit": "days", "description": "Minimum days after instruction issuance to send reminder to bank."},
            {"key": GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER, "value_min": "1", "value_max": "10", "value_default": "2", "unit": "days", "description": "Days after approval when the first reminder to print a bank letter is sent to the maker."},
            {"key": GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION, "value_min": "3", "value_max": "15", "value_default": "5", "unit": "days", "description": "Days after approval when an escalation email is sent to the maker and checker if the bank letter is still not printed."},
            {"key": GlobalConfigKey.RENEWAL_REMINDER_FIRST_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "7", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the first renewal reminder to users/admins."},
            {"key": GlobalConfigKey.RENEWAL_REMINDER_SECOND_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "14", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the second (escalation) renewal reminder to users/admins."},
            {"key": GlobalConfigKey.GRACE_PERIOD_DAYS, "value_min": "30", "value_max": "30", "value_default": "30", "unit": "days", "description": "Number of days after subscription end date to remain in a read-only state."},
            {"key": GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE, "value_min": "1", "value_max": "60", "value_default": "3", "unit": "days", "description": "Minimum days after instruction issuance to send reminder to bank."},
            {"key": GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER, "value_min": "1", "value_max": "10", "value_default": "2", "unit": "days", "description": "Days after approval when the first reminder to print a bank letter is sent to the maker."},
            {"key": GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION, "value_min": "3", "value_max": "15", "value_default": "5", "unit": "days", "description": "Days after approval when an escalation email is sent to the maker and checker if the bank letter is still not printed."},
            {"key": GlobalConfigKey.RENEWAL_REMINDER_FIRST_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "7", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the first renewal reminder to users/admins."},
            {"key": GlobalConfigKey.RENEWAL_REMINDER_SECOND_THRESHOLD_DAYS, "value_min": "1", "value_max": "30", "value_default": "14", "unit": "days", "description": "Number of days BEFORE auto/forced renewal threshold to send the second (escalation) renewal reminder to users/admins."},
            {"key": GlobalConfigKey.GRACE_PERIOD_DAYS, "value_min": "1", "value_max": "90", "value_default": "30", "unit": "days", "description": "Number of days after subscription end date to remain in a read-only state."}
        ]
        for config_data in global_configs_to_seed:
            try:
                if not crud_global_configuration.get_by_key(db, key=config_data["key"]):
                    crud_global_configuration.create(db, obj_in=GlobalConfigurationCreate(**config_data))
                    print(f"  Added Global Configuration: {config_data['key'].value}")
                else:
                    # Update existing config if description, default, min/max values differ
                    existing_config = crud_global_configuration.get_by_key(db, key=config_data["key"])
                    update_needed = False
                    if existing_config.value_default != config_data["value_default"]: update_needed = True
                    if existing_config.value_min != config_data["value_min"]: update_needed = True
                    if existing_config.value_max != config_data["value_max"]: update_needed = True
                    if existing_config.unit != config_data["unit"]: update_needed = True
                    if existing_config.description != config_data["description"]: update_needed = True

                    if update_needed:
                        crud_global_configuration.update(db, existing_config, GlobalConfigurationCreate(**config_data))
                        print(f"  Updated Global Configuration: {config_data['key'].value}")
                    else:
                        print(f"  Global Configuration '{config_data['key'].value}' already exists and is up-to-date.")
                db.flush() # Flush after each global config creation
                db.commit() # Commit after each global config to isolate failures # Kept this as per original to isolate failures
            except Exception as e:
                db.rollback() # Rollback on individual failure
                print(f"  ERROR seeding global configuration '{config_data['key'].value}': {e}")
                traceback.print_exc()
        # db.commit() # Removed this line as commit is now inside the loop

        # 4. Seed Currencies
        print("\n--- Seeding Currencies ---")
        for currency_data in CURRENCIES_LIST_DATA: # Use direct list data
            try:
                # CORRECTED: Access Currency model via the 'models' module
                if not crud_currency.get_by_iso_code(db, iso_code=currency_data["iso_code"]):
                    crud_currency.create(db, obj_in=CurrencyCreate(**currency_data))
                    print(f"  Added Currency: {currency_data['name']} ({currency_data['iso_code']})")
                else:
                    print(f"  Currency '{currency_data['name']}' already exists.")
                db.flush() # Flush after each currency creation
            except Exception as e:
                print(f"  ERROR seeding currency '{currency_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all currencies are processed

        # 5. Seed LG Types
        print("\n--- Seeding LG Types ---")
        lg_types_to_seed = [
            {"name": "Performance Guarantee", "description": "Guarantees performance of contractual obligations."},
            {"name": "Bid Bond", "description": "Guarantees that a bidder will enter into a contract if awarded."},
            {"name": "Advance Payment LG", "description": "Guarantees repayment of an advance payment."},
            {"name": "Financial Guarantee", "description": "Guarantees a financial obligation."},
        ]
        for lg_type_data in lg_types_to_seed:
            try:
                # CORRECTED: Access LgType model via the 'models' module
                if not crud_lg_type.get_by_name(db, name=lg_type_data["name"]):
                    crud_lg_type.create(db, obj_in=LgTypeCreate(**lg_type_data))
                    print(f"  Added LG Type: {lg_type_data['name']}")
                else:
                    print(f"  LG Type '{lg_type_data['name']}' already exists.")
                db.flush() # Flush after each LG type creation
            except Exception as e:
                print(f"  ERROR seeding LG type '{lg_type_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all LG types are processed

        # 6. Seed Rules
        print("\n--- Seeding Rules ---")
        rules_to_seed = [
            {"name": "URDG 758", "description": "Uniform Rules for Demand Guarantees, ICC Publication 758."},
            {"name": "URC 522", "description": "Uniform Rules for Collections, ICC Publication 522."},
            {"name": "ISP98", "description": "International Standby Practices."},
            {"name": "Other", "description": "Other rules not specified by standard codes."},
        ]
        for rule_data in rules_to_seed:
            try:
                # CORRECTED: Access Rule model via the 'models' module
                if not crud_rule.get_by_name(db, name=rule_data["name"]):
                    crud_rule.create(db, obj_in=RuleCreate(**rule_data))
                    print(f"  Added Rule: {rule_data['name']}")
                else:
                    print(f"  Rule '{rule_data['name']}' already exists.")
                db.flush() # Flush after each rule creation
            except Exception as e:
                print(f"  ERROR seeding rule '{rule_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all rules are processed

        # 7. Seed Issuing Methods
        print("\n--- Seeding Issuing Methods ---")
        issuing_methods_to_seed = [
            {"name": "Manual Delivery", "description": "Physical delivery of the LG."},
            {"name": "SWIFT MT760", "description": "Issued via SWIFT message type MT760."},
            {"name": "Bank Portal", "description": "Issued through bank's online portal."},
        ]
        for method_data in issuing_methods_to_seed:
            try:
                # CORRECTED: Access IssuingMethod model via the 'models' module
                if not crud_issuing_method.get_by_name(db, name=method_data["name"]):
                    crud_issuing_method.create(db, obj_in=IssuingMethodCreate(**method_data))
                    print(f"  Added Issuing Method: {method_data['name']}")
                else:
                    print(f"  Issuing Method '{method_data['name']}' already exists.")
                db.flush() # Flush after each method creation
            except Exception as e:
                print(f"  ERROR seeding issuing method '{method_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all issuing methods are processed

        # 8. Seed LG Statuses
        print("\n--- Seeding LG Statuses ---")
        lg_statuses_to_seed = [
            {"name": "Valid", "description": "The LG is valid and in force."},
            {"name": "Released", "description": "The LG has been released by the beneficiary."},
            {"name": "Liquidated", "description": "The LG has been fully liquidated."},
            {"name": "Expired", "description": "The LG's expiry date has passed."},
        ]
        for status_data in lg_statuses_to_seed:
            try:
                # CORRECTED: Access LgStatus model via the 'models' module
                if not crud_lg_status.get_by_name(db, name=status_data["name"]):
                    crud_lg_status.create(db, obj_in=LgStatusCreate(**status_data))
                    print(f"  Added LG Status: {status_data['name']}")
                else:
                    print(f"  LG Status '{status_data['name']}' already exists.")
                db.flush() # Flush after each status creation
            except Exception as e:
                print(f"  ERROR seeding LG status '{status_data['name']}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all LG statuses are processed


        # 11. Seed Banks
        print("\n--- Seeding Banks ---")
        banks_to_seed = []
        reader = csv.reader(io.StringIO(BANKS_CSV_CONTENT))
        all_bank_rows = list(reader)
        header_row_idx = -1
        for i, row in enumerate(all_bank_rows):
            if 'Bank Name' in row and 'SWIFT Code' in row and 'Phone' in row and 'Head Office Address' in row:
                header_row_idx = i
                break
        if header_row_idx == -1:
            print("  ERROR: Banks CSV missing essential headers (Bank Name, SWIFT Code, Phone).")
        else:
            headers = [h.strip() for h in all_bank_rows[header_row_idx]]
            try:
                name_col_idx = headers.index('Bank Name')
                code_col_idx = headers.index('Code') if 'Code' in headers else -1
                hist_names_col_idx = headers.index('Historical Name Changes') if 'Historical Name Changes' in headers else -1
                swift_col_idx = headers.index('SWIFT Code')
                phone_col_idx = headers.index('Phone')
                fax_col_idx = headers.index('Fax') if 'Fax' in headers else -1
                address_col_idx = headers.index('Head Office Address') if 'Head Office Address' in headers else -1
                for i in range(header_row_idx + 1, len(all_bank_rows)):
                    row = all_bank_rows[i]
                    if not row or all(not cell.strip() for cell in row):
                        continue
                    try:
                        if len(row) > max(name_col_idx, swift_col_idx, phone_col_idx):
                            bank_name = row[name_col_idx].strip()
                            short_name = row[code_col_idx].strip() if code_col_idx != -1 and row[code_col_idx].strip() else None
                            historical_names_str = row[hist_names_col_idx].strip() if hist_names_col_idx != -1 and row[hist_names_col_idx].strip() else ""
                            swift_code = row[swift_col_idx].strip()
                            phone_number = row[phone_col_idx].strip()
                            fax = row[fax_col_idx].strip() if fax_col_idx != -1 and row[fax_col_idx].strip() else None
                            former_names = [name.strip() for name in historical_names_str.split(',') if name.strip()] if historical_names_str else []
                            bank_address = row[address_col_idx].strip() if address_col_idx != -1 and row[address_col_idx].strip() else ""
                            if bank_name and swift_code and phone_number:
                                banks_to_seed.append({
                                    "name": bank_name,
                                    "address": bank_address,
                                    "phone_number": phone_number,
                                    "fax": fax,
                                    "former_names": former_names,
                                    "swift_code": swift_code,
                                    "short_name": short_name
                                })
                            else:
                                print(f"  WARNING: Skipping bank row due to missing essential data: {row}")
                        else:
                            print(f"  WARNING: Skipping malformed bank data row (not enough columns): {row}")
                    except IndexError:
                        print(f"  WARNING: Skipping malformed bank data row due to IndexError: {row}")
                        continue
            except ValueError as e:
                print(f"  ERROR: Banks CSV header issue: {e}")
                traceback.print_exc()
        for bank_data in banks_to_seed:
            try:
                # Check for existing bank by name or swift_code
                if not crud_bank.get_by_name(db, name=bank_data["name"]) and (not bank_data["swift_code"] or not crud_bank.get_by_swift_code(db, swift_code=bank_data["swift_code"])):
                    crud_bank.create(db, obj_in=BankCreate(**bank_data))
                    print(f"  Added Bank: {bank_data['name']}")
                else:
                    print(f"  Bank '{bank_data['name']}' or SWIFT code '{bank_data['swift_code']}' already exists. Skipping.")
                db.flush() # Flush after each bank creation
            except Exception as e:
                print(f"  ERROR seeding bank '{bank_data['name']}': {e}")
                traceback.print_exc()
                # Do not rollback here.
                continue # Continue to next bank
        db.commit()  # Commit after all banks are processed

        # 12. Seed Templates
        print("\n--- Seeding Templates ---")
        templates_to_seed = [
            TemplateCreate(
                name="LG Instruction - Extension", # Changed name for clarity of purpose
                template_type="LETTER",
                action_type="LG_EXTENSION",
                content="""
                Subject: Instruction to Extend Letter of Guarantee #{{lg_number}}

                To {{issuing_bank_name}},

                Kindly arrange to extend Letter of Guarantee number {{lg_number}}
                with original expiry date {{old_expiry_date}} until {{new_expiry_date}}.
                The amount of the guarantee is {{lg_amount}} {{lg_currency}}.
                Issued by {{issuing_bank_name}}.

                Your prompt attention to this matter is appreciated.

                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False, # Explicitly marks this as an instruction/letter template
                subject="Instruction to Extend LG #{{lg_number}}" # Added subject for consistency, though not strictly used for LETTERS
            ),
            TemplateCreate(
                name="LG Notification - Extension Confirmation", # New name for email template
                template_type="EMAIL",
                action_type="LG_EXTENSION", # Same action type as the instruction, distinguished by is_notification_template
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that an extension instruction has been issued for Letter of Guarantee:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Old Expiry Date:</b> {{old_expiry_date}}</li>
                        <li><b>New Expiry Date:</b> {{new_expiry_date}}</li>
                        <li><b>Amount:</b> {{lg_amount}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                    </ul>
                    <p>An official instruction document (Serial: {{instruction_serial}}) has been generated for this extension.</p>
                    <p>Should you have any questions, please contact us.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                    <p>{{customer_name}}</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True, # Explicitly marks this as a notification email template
                subject="LG Extension Confirmation: {{lg_number}} Extended to {{new_expiry_date}}" # Dynamic subject
            ),
            TemplateCreate(
                name="LG Release Instruction Letter",
                template_type="LETTER",
                action_type="LG_RELEASE",
                content="""
                Subject: Instruction to Release Letter of Guarantee #{{lg_serial_number}}

                To {{issuing_bank_name}},

                Please be advised that Letter of Guarantee number {{lg_serial_number}} is no longer required and can be released.
                Total documents received from bank: {{total_original_documents}}
                Pending replies from bank: {{pending_replies_count}}

                Kindly confirm the release and return the original LG to us.

                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False, # Explicitly mark this
                subject="Instruction to Release LG #{{lg_serial_number}}" # Added subject
            ),
            TemplateCreate(
                name="LG Notification - Release Confirmation",
                template_type="EMAIL",
                action_type="LG_RELEASE",
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that an instruction has been issued to release Letter of Guarantee:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Amount:</b> {{lg_amount}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                    </ul>
                    <p>The LG is now marked as 'Released' in the system. An official instruction document (Serial: {{instruction_serial}}) has been generated.</p>
                    <p>Should you have any questions, please contact us.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                    <p>{{customer_name}}</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="LG Release Confirmation: {{lg_number}} Released"
            ),
            TemplateCreate(
                name="LG Liquidation Instruction Letter",
                template_type="LETTER",
                action_type="LG_LIQUIDATE",
                content="""
                Subject: Instruction for {{liquidation_type}} Liquidation of Letter of Guarantee #{{lg_serial_number}}

                To {{issuing_bank_name}},

                Please be advised that Letter of Guarantee number {{lg_serial_number}} requires {{liquidation_type}} liquidation.
                Original LG Amount: {{original_lg_amount}} {{lg_currency}}
                New LG Amount: {{new_lg_amount}} {{lg_currency}}

                Total documents received from bank: {{total_original_documents}}
                Pending replies from bank: {{pending_replies_count}}

                Kindly confirm the liquidation and process accordingly.
                
                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False,
                subject="Instruction for LG Liquidation #{{lg_serial_number}}" # Added subject
            ),
            TemplateCreate(
                name="LG Notification - Liquidation Confirmation",
                template_type="EMAIL",
                action_type="LG_LIQUIDATE",
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that an instruction has been issued for {{liquidation_type}} liquidation of Letter of Guarantee:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Original Amount:</b> {{original_lg_amount}} {{lg_currency}}</li>
                        <li><b>New Amount:</b> {{new_lg_amount}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                    </ul>
                    <p>The LG has been updated in the system. An official instruction document (Serial: {{instruction_serial}}) has been generated.</p>
                    <p>Should you have any questions, please contact us.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                    <p>{{customer_name}}</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="LG Liquidation Confirmation: {{lg_number}} {{liquidation_type}} Liquidated"
            ),
            TemplateCreate( # NEW Template for Decrease Amount Instruction Letter
                name="LG Decrease Amount Instruction Letter",
                template_type="LETTER",
                action_type=ACTION_TYPE_LG_DECREASE_AMOUNT, # Use constant
                content="""
                Subject: Instruction to Decrease Amount of Letter of Guarantee #{{lg_serial_number}}

                To {{issuing_bank_name}},

                Please be advised that Letter of Guarantee number {{lg_serial_number}} requires a decrease in its guaranteed amount.
                Original LG Amount: {{original_lg_amount_formatted}} {{lg_currency}}
                Amount Decreased By: {{decrease_amount_formatted}} {{lg_currency}}
                New Remaining LG Amount: {{new_lg_amount_formatted}} {{lg_currency}}

                Kindly confirm this amendment and process accordingly.

                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False,
                subject="Instruction to Decrease LG Amount #{{lg_serial_number}}" # Added subject
            ),
            TemplateCreate( # NEW Template for Decrease Amount Notification Email
                name="LG Notification - Amount Decrease Confirmation",
                template_type="EMAIL",
                action_type=ACTION_TYPE_LG_DECREASE_AMOUNT, # Use constant
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that an instruction has been issued to decrease the amount of Letter of Guarantee:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Original Amount:</b> {{original_lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Decreased By:</b> {{decrease_amount_formatted}} {{lg_currency}}</li>
                        <li><b>New Amount:</b> {{new_lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                    </ul>
                    <p>The LG amount has been updated in the system. An official instruction document (Serial: {{instruction_serial}}) has been generated.</p>
                    <p>Should you have any questions, please contact us.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                    <p>{{customer_name}}</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="LG Amount Decrease Confirmation: {{lg_number}} Amount Decreased"
            ),
            TemplateCreate( # NEW Template for Undelivered LG Instructions Notification
                name="Undelivered LG Instructions Notification",
                template_type="EMAIL",
                action_type=ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT, # Use the new constant
                content="""
                <html>
                <body>
                    <p>Dear Corporate Admin,</p>
                    <p>This is an automated report from the Treasury Management Platform regarding undelivered Letters of Guarantee instructions for <b>{{customer_name}}</b>.</p>
                    <p>We identified <b>{{undelivered_instructions_count}}</b> instructions that were issued between <b>{{report_start_days}}</b> and <b>{{report_stop_days}}</b> days ago, but have not yet been marked as delivered to the bank.</p>
                    <p>Please review the details below and ensure these instructions are processed promptly.</p>
                    <br>
                    <h3>Undelivered Instructions Details:</h3>
                    {{undelivered_instructions_table}}
                    <br>
                    <p>Please take necessary action to confirm delivery for these instructions.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="{{customer_name}} - Urgent: {{undelivered_instructions_count}} Undelivered LG Instructions Found"
            ),
                TemplateCreate( # NEW Template for LG Reminder to Banks Letter
                name="LG Reminder to Banks Letter",
                template_type="LETTER",
                action_type=ACTION_TYPE_LG_REMINDER_TO_BANKS, # Use the new constant
                content="""
                Subject: REMINDER: Letter of Guarantee #{{lg_serial_number}} - Instruction Serial #{{original_instruction_serial}}

                To {{issuing_bank_name}},

                This is a follow-up reminder regarding our instruction dated {{original_instruction_date}} (Serial #{{original_instruction_serial}}) concerning Letter of Guarantee number {{lg_serial_number}}.

                The original instruction was of type '{{original_instruction_type}}' with an amount of {{lg_amount_formatted}} {{lg_currency}}, issued by {{issuing_bank_name}}.

                As of today, {{current_date}}, it has been {{days_overdue}} days since the instruction was issued (or delivered on {{original_instruction_delivery_date}} if applicable), and we have not yet received your confirmation or reply.

                Kindly provide an update on the status of this instruction at your earliest convenience.

                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False, # It's a physical letter
                subject="REMINDER: LG #{{lg_serial_number}} - Instruction #{{original_instruction_serial}}" # Dynamic subject
            ),
            # NEW: Template for Approval Ready for Print Notification (Initial email to Maker)
            TemplateCreate(
                name="Approval Ready For Print Notification",
                template_type="EMAIL",
                action_type="APPROVAL_READY_FOR_PRINT", # NEW ACTION TYPE
                content="""
                <html>
                <body>
                    <p>Dear {{maker_name}},</p>
                    <p>Good news! Your request for action ({{action_type}}) on LG #{{lg_number}} (Instruction Serial: {{instruction_serial_number}}) has been <b>APPROVED</b>.</p>
                    <p>The official bank letter/instruction is now ready for you to print, sign, and deliver to the bank.</p>
                    <p>Please click the link below to view and print the letter:</p>
                    <p><a href="{{print_link}}">Print Letter for LG {{lg_number}}</a></p>
                    <p>You can also find this and other pending print tasks in your Action Center: <a href="{{action_center_link}}">Go to Action Center</a></p>
                    <p>Kindly complete this step as soon as possible.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Approved: LG {{lg_number}} Letter Ready for Printing"
            ),
            # NEW: Template for Print Instruction Reminder (2 days)
            TemplateCreate(
                name="Print Instruction Reminder",
                template_type="EMAIL",
                action_type="PRINT_REMINDER", # NEW ACTION TYPE
                content="""
                <html>
                <body>
                    <p>Dear {{maker_name}},</p>
                    <p>This is a friendly reminder that the bank letter for LG #{{lg_number}} (Instruction Serial: {{instruction_serial_number}}) is still awaiting printing and delivery.</p>
                    <p>It has been {{days_overdue}} days since this instruction was approved.</p>
                    <p>Please ensure this critical step is completed without further delay:</p>
                    <p><a href="{{print_link}}">Print Letter for LG {{lg_number}}</a></p>
                    <p>Thank you for your prompt attention.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Reminder: LG {{lg_number}} Letter Awaiting Print"
            ),
            # NEW: Template for Print Instruction Escalation (5 days)
            TemplateCreate(
                name="Print Instruction Escalation",
                template_type="EMAIL",
                action_type="PRINT_ESCALATION", # NEW ACTION TYPE
                content="""
                <html>
                <body>
                    <p style="color: red; font-weight: bold;">{{subject_prefix}} Dear {{maker_name}} and {{checker_email}},</p>
                    <p style="color: red; font-weight: bold;"><b>URGENT ACTION REQUIRED:</b> The bank letter for LG #{{lg_number}} (Instruction Serial: {{instruction_serial_number}}) has not yet been printed and delivered.</p>
                    <p style="color: red; font-weight: bold;">It has been {{days_overdue}} days since this instruction was approved, and it is now considered overdue.</p>
                    <p style="color: red; font-weight: bold;">This delay may impact critical bank operations. Please ensure immediate action is taken to print and deliver this letter:</p>
                    <p><a href="{{print_link}}">Print Letter for LG {{lg_number}}</a></p>
                    <p>Your immediate attention is crucial.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="ESCALATION: Urgent LG {{lg_number}} Letter Not Printed!"
            ),
             # NEW: LG Activate Non-Operative Instruction Letter
            TemplateCreate(
                name="LG Activate Non-Operative Instruction Letter",
                template_type="LETTER",
                action_type="LG_ACTIVATE_NON_OPERATIVE",
                content="""
                Subject: Instruction to Activate Non-Operative Advance Payment LG #{{lg_number}}

                To {{issuing_bank_name}},

                Please arrange to activate the Advance Payment LG number {{lg_number}}.
                This guarantee becomes operative upon receipt of payment as follows:
                Payment Method: {{payment_method}}
                Amount: {{payment_amount_formatted}}
                Reference: {{payment_reference}}
                Payment Date: {{payment_date}}
                Issuing Bank for Payment: {{payment_issuing_bank_name}}

                Kindly confirm the activation of this guarantee.

                Sincerely,
                {{customer_name}}
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=False,
                subject="Instruction to Activate LG #{{lg_number}}" # Added subject
            ),
 
                # NEW: LG Record email notification
                TemplateCreate(
                name="LG Recorded Confirmation Notification",
                template_type="EMAIL",
                action_type="LG_RECORDED", # This is the action_type your crud_lg_record.py is looking for.
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that a new Letter of Guarantee has been recorded in the system:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>LG Amount:</b> {{lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                        <li><b>LG Type:</b> {{lg_type}}</li>
                        <li><b>LG Category:</b> {{lg_category}}</li>
                        <li><b>Recorded By:</b> {{user_email}}</li>
                    </ul>
                    <p>You can view the full details by logging into the Treasury Platform.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="New LG Recorded: {{lg_number}}"
            ),
            
            # NEW: LG Activate Non-Operative Notification Email
            TemplateCreate(
                name="LG Notification - Activation Confirmation",
                template_type="EMAIL",
                action_type="LG_ACTIVATE_NON_OPERATIVE",
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that an instruction has been issued to activate Advance Payment LG:</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Amount:</b> {{lg_amount}} {{lg_currency}}</li>
                        <li><b>Issuing Bank:</b> {{issuing_bank_name}}</li>
                        <li><b>Payment Received:</b> {{payment_amount_formatted}} on {{payment_date}} (Ref: {{payment_reference}})</li>
                    </ul>
                    <p>The LG is now marked as 'Operative' in the system. An official instruction document (Serial: {{instruction_serial}}) has been generated.</p>
                    <p>Should you have any questions, please contact us.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="LG Activation Confirmation: {{lg_number}} is now Operative"
            ),          
            TemplateCreate(
                name="Print Confirmation Notification",
                template_type="EMAIL",
                action_type=NOTIFICATION_PRINT_CONFIRMATION, # Use the new constant
                content="""
                <html>
                <body>
                    <p>Dear {{maker_email}},</p>
                    <p>This is to confirm that the bank letter for LG #<b>{{lg_number}}</b> (Instruction Serial: <b>{{instruction_serial_number}}</b>, for action: {{action_type}}) has been successfully marked as 'Printed' in the system.</p>
                    <p>You have completed your step for this transaction. You will receive further notifications regarding bank replies if applicable.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Confirmed: LG {{lg_number}} Instruction {{instruction_serial_number}} Printed"
            ),
            # NEW: LG Amend Notification Email (No instruction letter as per your clarification)
            TemplateCreate(
                name="LG Notification - Amendment Confirmation",
                template_type="EMAIL",
                action_type="LG_AMEND",
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is to confirm that Letter of Guarantee #{{lg_number}} has been amended as per the bank letter.</p>
                    <p>Summary of Changes: {{amended_fields_summary}}</p>
                    <p>The updated LG details are reflected in the system. You can view the full details in the system. The amendment document ID is {{amendment_document_id}}.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="LG Amendment Confirmation: LG {{lg_number}} Amended"
            ),
            # NEW: Template for First Renewal Reminder (Feature 1)
            TemplateCreate(
                name="LG Renewal First Reminder",
                template_type="EMAIL",
                action_type="LG_RENEWAL_REMINDER_FIRST",
                content="""
                <html>
                <body>
                    <p>Dear User,</p>
                    <p>This is a reminder that Letter of Guarantee #{{lg_number}} is approaching its expiry date.</p>
                    <ul>
                        <li><b>LG Type:</b> {{lg_type}}</li>
                        <li><b>Amount:</b> {{lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Expiry Date:</b> {{expiry_date}}</li>
                        <li><b>Days Until Expiry:</b> {{days_until_expiry}} days</li>
                        <li><b>Auto-Renewal Status:</b> {{auto_renewal_status}}</li>
                    </ul>
                    <p>Please review this LG in the system and take appropriate action if needed.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Reminder: LG {{lg_number}} is Nearing Expiry"
            ),
            # NEW: Template for Second Renewal Reminder (Escalation) (Feature 1)
            TemplateCreate(
                name="LG Renewal Second Reminder",
                template_type="EMAIL",
                action_type="LG_RENEWAL_REMINDER_SECOND",
                content="""
                <html>
                <body>
                    <p style="color: red; font-weight: bold;">{{subject_prefix}} Dear User,</p>
                    <p style="color: red; font-weight: bold;">This is an URGENT reminder that Letter of Guarantee #{{lg_number}} is critically close to its expiry date.</p>
                    <ul style="color: red; font-weight: bold;">
                        <li><b>LG Type:</b> {{lg_type}}</li>
                        <li><b>Amount:</b> {{lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Expiry Date:</b> {{expiry_date}}</li>
                        <li><b>Days Until Expiry:</b> {{days_until_expiry}} days</li>
                        <li><b>Auto-Renewal Status:</b> {{auto_renewal_status}}</li>
                    </ul>
                    <p style="color: red; font-weight: bold;">Immediate action is required to avoid expiry and potential loss of coverage.</p>
                    <p>Please log in to the system to extend or take other necessary action.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="URGENT: LG {{lg_number}} is Expiring Soon!"
            ),
            # NEW: Template for Internal Owner Renewal Reminder (Feature 2)
            TemplateCreate(
                name="LG Internal Owner Renewal Reminder",
                template_type="EMAIL",
                action_type="LG_REMINDER_TO_INTERNAL_OWNER",
                content="""
                <html>
                <body>
                    <p>Dear {{internal_owner_email}},</p>
                    <p>This is a reminder regarding Letter of Guarantee #{{lg_number}} which is not set for auto-renewal and is approaching its expiry date.</p>
                    <ul>
                        <li><b>LG Type:</b> {{lg_type}}</li>
                        <li><b>Amount:</b> {{lg_amount_formatted}} {{lg_currency}}</li>
                        <li><b>Expiry Date:</b> {{expiry_date}}</li>
                        <li><b>Days Until Expiry:</b> {{days_until_expiry}} days</li>
                    </ul>
                    <p>Please review the status of this LG and consider initiating an extension, release, or liquidation if required, as it will not automatically renew.</p>
                    <p>Regards,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Action Required: Non-Auto-Renew LG {{lg_number}} Nearing Expiry"
            ),
            TemplateCreate(
                name="Print Reminder",
                template_type="EMAIL",
                action_type="PRINT_REMINDER",
                content="""
                <html>
                <body>
                    <p>Dear {{maker_name}},</p>
                    <p>This is a friendly reminder that an approved Letter of Guarantee instruction is pending printing.</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Instruction Serial:</b> {{instruction_serial_number}}</li>
                        <li><b>Action Type:</b> {{action_type}}</li>
                        <li><b>Days Overdue:</b> {{days_overdue}} days</li>
                    </ul>
                    <p>Please click the link below to view and print the relevant bank letter to finalize this action.</p>
                    <p><a href="{{print_link}}">View and Print Letter</a></p>
                    <p>You can also find this request in your action center: <a href="{{action_center_link}}">Go to Action Center</a></p>
                    <p>Thank you,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Reminder: Please Print Letter for LG #{{lg_number}}"
            ),
            TemplateCreate(
                name="Print Escalation",
                template_type="EMAIL",
                action_type="PRINT_ESCALATION",
                content="""
                <html>
                <body>
                    <p>Dear {{maker_name}},</p>
                    <p>This is an urgent notification regarding an approved Letter of Guarantee instruction that remains unprinted. This issue has been escalated to your Checker ({{checker_email}}) for their attention.</p>
                    <ul>
                        <li><b>LG Number:</b> {{lg_number}}</li>
                        <li><b>Instruction Serial:</b> {{instruction_serial_number}}</li>
                        <li><b>Action Type:</b> {{action_type}}</li>
                        <li><b>Days Overdue:</b> {{days_overdue}} days</li>
                    </ul>
                    <p>Please click the link below to view and print the bank letter immediately to avoid further delays.</p>
                    <p><a href="{{print_link}}">View and Print Letter</a></p>
                    <p>You can also find this request in your action center: <a href="{{action_center_link}}">Go to Action Center</a></p>
                    <p>Thank you,</p>
                    <p>The {{platform_name}} Team</p>
                </body>
                </html>
                """,
                is_global=True,
                customer_id=None,
                is_notification_template=True,
                subject="Urgent: Print Escalation for LG #{{lg_number}}"
            ),
        ]

        for template_data in templates_to_seed:
            try:
                # Use get_by_name_and_action_type with is_notification_template for precise checking
                if not crud_template.get_by_name_and_action_type(
                    db,
                    name=template_data.name,
                    action_type=template_data.action_type,
                    customer_id=template_data.customer_id, # Pass customer_id or None
                    is_notification_template=template_data.is_notification_template
                ):
                    crud_template.create(db, obj_in=template_data)
                    print(f"  Added Global Template: {template_data.name} (Notification: {template_data.is_notification_template})")
                else:
                    # Update if exists to ensure content is up-to-date, but only if the content or subject is different
                    existing_template = crud_template.get_by_name_and_action_type(
                        db,
                        name=template_data.name,
                        action_type=template_data.action_type,
                        customer_id=template_data.customer_id,
                        is_notification_template=template_data.is_notification_template
                    )
                    # Check if content or subject has changed for update
                    if existing_template and (existing_template.content != template_data.content or existing_template.subject != template_data.subject):
                        crud_template.update(db, existing_template, template_data)
                        print(f"  Updated Global Template: {template_data.name} (Notification: {template_data.is_notification_template})")
                    else:
                        print(f"  Global Template '{template_data.name}' already exists and is up-to-date.")
                db.flush() # Flush after each template operation
            except Exception as e:
                print(f"  ERROR seeding template '{template_data.name}': {e}")
                traceback.print_exc()
        db.commit() # Commit after all templates are processed


        # 13. Seed Sample Customers
        print("\n--- Seeding Sample Customers ---")
        today = date.today()

        # Seed initial customers (Acme, Globex, Cyberdyne) first to get their IDs
        # Use existing names to prevent duplicates in case of partial previous runs
        
        # Acme Corporation
        acme_corp_data = {
            "name": "Acme Corporation",
            "address": "123 Acme St, Cairo",
            "contact_email": "contact@acmecorp.com",
            "contact_phone": "+201001234567",
            "subscription_plan_id": crud_subscription_plan.get_by_name(db, name="Premium LG Plan").id,
            "initial_corporate_admin": {
                "email": "corp.admin@acmecorp.com",
                "password": "Password123!",
            },
            "initial_entities": [
                {"entity_name": "Acme East Division", "address": "123 Maple St, Anytown, USA", "commercial_register_number": "REG-ACME-EAST-12345", "tax_id": "TAX-ACME-EAST-ABCDE", "code": "AE01", "contact_person": "Jane Doe", "contact_email": "jane.doe@acmecorp.com"},
                {"entity_name": "Acme West Division", "address": "123 Maple St, Anytown, USA", "commercial_register_number": "REG-ACME-WEST-12345", "tax_id": "TAX-ACME-WEST-ABCDE", "code": "AW02", "contact_person": "John Smith", "contact_email": "john.smith@acmecorp.com"},
            ]
        }
        acme_corp = db.query(models.Customer).filter(models.Customer.name == acme_corp_data["name"]).first()
        if not acme_corp:
            acme_corp = crud_customer.onboard_customer(
                db, customer_in=CustomerCreate(**acme_corp_data), user_id_caller=system_owner_id
            )
            db.commit() # Commit customer onboarding
            print(f"  Onboarded customer: {acme_corp.name}")
        else:
            print(f"  Customer '{acme_corp.name}' already exists. Fetching existing data.")
        acme_corp_id = acme_corp.id if acme_corp else None
        acme_east_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == acme_corp_id, models.CustomerEntity.entity_name == "Acme East Division").first() if acme_corp_id else None
        acme_east_entity_id = acme_east_entity.id if acme_east_entity else None
        acme_west_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == acme_corp_id, models.CustomerEntity.entity_name == "Acme West Division").first() if acme_corp_id else None
        acme_west_entity_id = acme_west_entity.id if acme_west_entity else None
        acme_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@acmecorp.com").first()
        acme_admin_user_id = acme_admin_user.id if acme_admin_user else None

# Globex Industries
        globex_data = {
            "name": "Globex Industries",
            "address": "456 Global Rd, Giza",
            "contact_email": "info@globex.com",
            "contact_phone": "+201007654321",
            "subscription_plan_id": crud_subscription_plan.get_by_name(db, name="Basic LG Plan").id,
            "initial_corporate_admin": {
                "email": "corp.admin@globex.com",
                "password": "Password123!",
            },
            "initial_entities": [] # No initial entities, "Main Entity" will be created by onboard_customer
        }
        
        # Try to fetch the customer first
        globex = db.query(models.Customer).filter(models.Customer.name == globex_data["name"]).first()

        if not globex:
            # If customer doesn't exist, onboard it. onboard_customer will create the default entity.
            globex = crud_customer.onboard_customer( # Removed await as per previous fix
                db, customer_in=CustomerCreate(**globex_data), user_id_caller=system_owner_id
            )
            db.commit() # Commit customer onboarding to make it and its default entities visible
            # After a commit, the session's objects might be detached/expired.
            # Re-querying or refreshing is the safest way to get the latest state including relationships.
            globex = db.query(models.Customer).options(selectinload(models.Customer.entities)).filter(models.Customer.id == globex.id).first()
            print(f"  Onboarded customer: {globex.name}")
        else:
            print(f"  Customer '{globex.name}' already exists. Fetching existing data.")
            # If customer already exists, ensure it's loaded with its entities for the next step
            globex = db.query(models.Customer).options(selectinload(models.Customer.entities)).filter(models.Customer.id == globex.id).first()
        
        # Ensure globex object is not None and has an ID
        if not globex or not globex.id:
            raise Exception(f"Failed to obtain Globex customer object or its ID after onboarding/fetching.")

        globex_id = globex.id

        # Now, find the 'Main Entity' entity through the customer's relationship.
        # We assume it exists because onboard_customer creates it for Basic plans.
        globex_main_entity = next(
            (entity for entity in globex.entities if entity.entity_name == "Main Entity" and not entity.is_deleted),
            None
        )

        if not globex_main_entity:
            # This is a critical error for seeding. If the customer exists, its default entity MUST exist.
            # If it doesn't, it indicates a problem with onboard_customer or a corrupted seed state.
            print(f"FATAL ERROR: 'Main Operations' entity for Globex (ID: {globex_id}) was expected but not found. This indicates a critical issue with initial customer onboarding or a corrupted database state for this customer.")
            raise Exception("Failed to ensure 'Main Operations' entity for Globex. Please investigate `onboard_customer` logic or prior seeding.")

        globex_main_entity_id = globex_main_entity.id if globex_main_entity else None
        
        # Final check to ensure the ID is indeed available
        if globex_main_entity_id is None:
            raise Exception("Failed to retrieve ID for 'Main Operations' entity for Globex.")

        globex_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@globex.com").first()
        globex_admin_user_id = globex_admin_user.id if globex_admin_user else None

        # Cyberdyne Systems
        cyberdyne_data = {
            "name": "Cyberdyne Systems",
            "address": "789 AI Blvd, Maadi",
            "contact_email": "contact@cyberdyne.com",
            "contact_phone": "+201009876543",
            "subscription_plan_id": crud_subscription_plan.get_by_name(db, name="Premium LG Plan").id,
            "initial_corporate_admin": {
                "email": "corp.admin@cyberdyne.com",
                "password": "Password123!",
            },
            "initial_entities": [
                {"entity_name": "R&D Lab", "address": "123 Cyberdyne, USA", "commercial_register_number": "REG-CYBR-R&D-12345", "tax_id": "TAX-CYBR-R&D-ABCDE", "code": "RD01", "contact_person": "Dr. Sarah Connor", "contact_email": "sarah.connor@cyberdyne.com"},
                {"entity_name": "Sales Department", "address": "123 Cyberdyne, USA", "commercial_register_number": "REG-CYBR-SALE-12345", "tax_id": "TAX-CYBR-SALE-ABCDE", "code": "SD02", "contact_person": "Miles Dyson", "contact_email": "miles.dyson@cyberdyne.com"},
            ]
        }
        cyberdyne = db.query(models.Customer).filter(models.Customer.name == cyberdyne_data["name"]).first()
        if not cyberdyne:
            cyberdyne = crud_customer.onboard_customer(
                db, customer_in=CustomerCreate(**cyberdyne_data), user_id_caller=system_owner_id
            )
            db.commit() # Commit customer onboarding
            print(f"  Onboarded customer: {cyberdyne.name}")
        else:
            print(f"  Customer '{cyberdyne.name}' already exists. Fetching existing data.")
        cyberdyne_id = cyberdyne.id if cyberdyne else None
        cyberdyne_rd_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == cyberdyne_id, models.CustomerEntity.entity_name == "R&D Lab").first() if cyberdyne_id else None
        cyberdyne_rd_entity_id = cyberdyne_rd_entity.id if cyberdyne_rd_entity else None
        cyberdyne_sales_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == cyberdyne_id, models.CustomerEntity.entity_name == "Sales Department").first() if cyberdyne_id else None
        cyberdyne_sales_entity_id = cyberdyne_sales_entity.id if cyberdyne_sales_entity else None
        cyberdyne_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@cyberdyne.com").first()
        cyberdyne_admin_user_id = cyberdyne_admin_user.id if cyberdyne_admin_user else None


        # Now, fetch the IDs of these newly created categories, entities, and users
        # CORRECTED: Access models via the 'models' module
        # LG Status IDs
        valid_status = db.query(models.LgStatus).filter(models.LgStatus.name == "Valid").first()
        valid_status_id = valid_status.id if valid_status else None
        expired_status = db.query(models.LgStatus).filter(models.LgStatus.name == "Expired").first()
        expired_status_id = expired_status.id if expired_status else None

        # LG Operational Status IDs
        operative_op_status = db.query(models.LgOperationalStatus).filter(models.LgOperationalStatus.name == "Operative").first()
        operative_op_status_id = operative_op_status.id if operative_op_status else None
        non_operative_op_status = db.query(models.LgOperationalStatus).filter(models.LgOperationalStatus.name == "Non-Operative").first()
        non_operative_op_status_id = non_operative_op_status.id if non_operative_op_status else None

        # Issuing Method IDs
        swift_mt760_method = db.query(models.IssuingMethod).filter(models.IssuingMethod.name == "SWIFT MT760").first()
        swift_mt760_method_id = swift_mt760_method.id if swift_mt760_method else None
        manual_delivery_method = db.query(models.IssuingMethod).filter(models.IssuingMethod.name == "Manual Delivery").first()
        manual_delivery_method_id = manual_delivery_method.id if manual_delivery_method else None
        bank_portal_method = db.query(models.IssuingMethod).filter(models.IssuingMethod.name == "Bank Portal").first()
        bank_portal_method_id = bank_portal_method.id if bank_portal_method else None

        # Rule IDs
        urcg_rule = db.query(models.Rule).filter(models.Rule.name == "URDG 758").first()
        urcg_rule_id = urcg_rule.id if urcg_rule else None
        other_rule = db.query(models.Rule).filter(models.Rule.name == "Other").first()
        other_rule_id = other_rule.id if other_rule else None

        # Customer and related IDs (Fetch these even if they print "Skipping" earlier due to prior errors)
        # We need to ensure these objects exist to get their IDs.
        system_owner_user = db.query(models.User).filter(models.User.email == "system.owner@example.com").first()
        system_owner_id = system_owner_user.id if system_owner_user else None

        acme_corp = db.query(models.Customer).filter(models.Customer.name == "Acme Corporation").first()
        acme_corp_id = acme_corp.id if acme_corp else None
        acme_east_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == acme_corp_id, models.CustomerEntity.entity_name == "Acme East Division").first() if acme_corp_id else None
        acme_east_entity_id = acme_east_entity.id if acme_east_entity else None
        acme_west_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == acme_corp_id, models.CustomerEntity.entity_name == "Acme West Division").first() if acme_corp_id else None
        acme_west_entity_id = acme_west_entity.id if acme_west_entity else None
        acme_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@acmecorp.com").first()
        acme_admin_user_id = acme_admin_user.id if acme_admin_user else None
        acme_it_category = db.query(models.LGCategory).filter(models.LGCategory.customer_id == acme_corp_id, models.LGCategory.name == "IT Projects").first() if acme_corp_id else None
        acme_it_category_id = acme_it_category.id if acme_it_category else None
        acme_hr_category = db.query(models.LGCategory).filter(models.LGCategory.customer_id == acme_corp_id, models.LGCategory.name == "HR Department").first() if acme_corp_id else None
        acme_hr_category_id = acme_hr_category.id if acme_hr_category else None
        globex = db.query(models.Customer).filter(models.Customer.name == "Globex Industries").first()
        globex_id = globex.id if globex else None
        globex_main_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == globex_id, models.CustomerEntity.entity_name == "Main Entity").first() if globex_id else None
        globex_main_entity_id = globex_main_entity.id if globex_main_entity else None
        globex_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@globex.com").first()
        globex_admin_user_id = globex_admin_user.id if globex_admin_user else None
        globex_mfg_category = db.query(models.LGCategory).filter(models.LGCategory.customer_id == globex_id, models.LGCategory.name == "Manufacturing").first() if globex_id else None
        globex_mfg_category_id = globex_mfg_category.id if globex_mfg_category else None
        cyberdyne = db.query(models.Customer).filter(models.Customer.name == "Cyberdyne Systems").first()
        cyberdyne_id = cyberdyne.id if cyberdyne else None
        cyberdyne_rd_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == cyberdyne_id, models.CustomerEntity.entity_name == "R&D Lab").first() if cyberdyne_id else None
        cyberdyne_rd_entity_id = cyberdyne_rd_entity.id if cyberdyne_rd_entity else None
        cyberdyne_sales_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == cyberdyne_id, models.CustomerEntity.entity_name == "Sales Department").first() if cyberdyne_id else None
        cyberdyne_sales_entity_id = cyberdyne_sales_entity.id if cyberdyne_sales_entity else None
        cyberdyne_admin_user = db.query(models.User).filter(models.User.email == "corp.admin@cyberdyne.com").first()
        cyberdyne_admin_user_id = cyberdyne_admin_user.id if cyberdyne_admin_user else None
        cyberdyne_rg_category = db.query(models.LGCategory).filter(models.LGCategory.customer_id == cyberdyne_id, models.LGCategory.name == "Research Grants").first() if cyberdyne_id else None
        cyberdyne_rg_category_id = cyberdyne_rg_category.id if cyberdyne_rg_category else None
        
        # Currency IDs
        egp_currency = db.query(models.Currency).filter(models.Currency.iso_code == "EGP").first()
        egp_currency_id = egp_currency.id if egp_currency else None
        usd_currency = db.query(models.Currency).filter(models.Currency.iso_code == "USD").first()
        usd_currency_id = usd_currency.id if usd_currency else None
        eur_currency = db.query(models.Currency).filter(models.Currency.iso_code == "EUR").first()
        eur_currency_id = eur_currency.id if eur_currency else None

        # LG Type IDs
        performance_lg_type = db.query(models.LgType).filter(models.LgType.name == "Performance Guarantee").first()
        performance_lg_type_id = performance_lg_type.id if performance_lg_type else None
        bid_bond_lg_type = db.query(models.LgType).filter(models.LgType.name == "Bid Bond").first()
        bid_bond_lg_type_id = bid_bond_lg_type.id if bid_bond_lg_type else None
        advance_payment_lg_type = db.query(models.LgType).filter(models.LgType.name == "Advance Payment LG").first()
        advance_payment_lg_type_id = advance_payment_lg_type.id if advance_payment_lg_type else None
        financial_lg_type = db.query(models.LgType).filter(models.LgType.name == "Financial Guarantee").first()
        financial_lg_type_id = financial_lg_type.id if financial_lg_type else None

        # Bank IDs
        nbe_bank = db.query(models.Bank).filter(models.Bank.name == "National Bank of Egypt (NBE)").first()
        nbe_bank_id = nbe_bank.id if nbe_bank else None
        cib_bank = db.query(models.Bank).filter(models.Bank.name == "Commercial International Bank (CIB)").first()
        cib_bank_id = cib_bank.id if cib_bank else None
        qnb_bank = db.query(models.Bank).filter(models.Bank.name == "QNB Al Ahli").first()
        qnb_bank_id = qnb_bank.id if qnb_bank else None
        aaib_bank = db.query(models.Bank).filter(models.Bank.name == "Arab African International Bank").first()
        aaib_bank_id = aaib_bank.id if aaib_bank else None
        hsbc_bank = db.query(models.Bank).filter(models.Bank.name == "HSBC Bank Egypt").first()
        hsbc_bank_id = hsbc_bank.id if hsbc_bank else None

        # Check if any critical IDs are None before proceeding with LG seeding
        critical_ids_for_lg_seeding = {
            "valid_status_id": valid_status_id,
            "expired_status_id": expired_status_id,
            "operative_op_status_id": operative_op_status_id,
            "non_operative_op_status_id": non_operative_op_status_id,
            "swift_mt760_method_id": swift_mt760_method_id,
            "manual_delivery_method_id": manual_delivery_method_id,
            "bank_portal_method_id": bank_portal_method_id,
            "urcg_rule_id": urcg_rule_id,
            "other_rule_id": other_rule_id,
            "acme_corp_id": acme_corp_id,
            "acme_east_entity_id": acme_east_entity_id,
            "acme_west_entity_id": acme_west_entity_id,
            "acme_admin_user_id": acme_admin_user_id,
            "acme_it_category_id": acme_it_category_id,
            "acme_hr_category_id": acme_hr_category_id,
            "globex_id": globex_id,
            "globex_main_entity_id": globex_main_entity_id,
            "globex_admin_user_id": globex_admin_user_id,
            "globex_mfg_category_id": globex_mfg_category_id,
            "cyberdyne_id": cyberdyne_id,
            "cyberdyne_rd_entity_id": cyberdyne_rd_entity_id,
            "cyberdyne_sales_entity_id": cyberdyne_sales_entity_id,
            "cyberdyne_admin_user_id": cyberdyne_admin_user_id,
            "cyberdyne_rg_category_id": cyberdyne_rg_category_id,
            "egp_currency_id": egp_currency_id,
            "usd_currency_id": usd_currency_id,
            "eur_currency_id": eur_currency_id,
            "performance_lg_type_id": performance_lg_type_id,
            "bid_bond_lg_type_id": bid_bond_lg_type_id,
            "advance_payment_lg_type_id": advance_payment_lg_type_id,
            "financial_lg_type_id": financial_lg_type_id,
            "nbe_bank_id": nbe_bank_id,
            "cib_bank_id": cib_bank_id,
            "qnb_bank_id": qnb_bank_id,
            "aaib_bank_id": aaib_bank_id,
            "hsbc_bank_id": hsbc_bank_id,
        }

        missing_ids_list = [key for key, value in critical_ids_for_lg_seeding.items() if value is None]
        if missing_ids_list:
            print(f"FATAL ERROR: Missing critical IDs for LG seeding: {', '.join(missing_ids_list)}. Ensure previous seeding steps completed successfully.")
            raise Exception("Missing critical IDs for LG seeding.") # Raise to stop seeding

        lg_records_to_seed = [
            # Acme Corporation LG (one basic LG)
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 1, # Added
                "issuer_name": "Acme Contractors Ltd.",
                "issuer_id": "ACL001",
                "lg_number": "ACME/AE/PG/001-TEST", # MODIFIED: Added -TEST to ensure uniqueness for re-seeding
                "lg_amount": 1000000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=90),
                "expiry_date": today + timedelta(days=180),
                "lg_period_months": 6, # Added
                "auto_renewal": True,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Performance Guarantee for Phase 1 construction.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "1 Main St, Cairo",
                "issuing_bank_phone": "19623",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "PROJ-ACME-001"},
                "internal_contract_project_id": "CTR-PH1-ACME",
                "notes": "Key guarantee for large project.",
            },
            # Acme Corporation Non-Operative LG
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 2, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Alpha Solutions",
                "issuer_id": "AS003",
                "lg_number": "ACME/AE/AP/003",
                "lg_amount": 250000.00,
                "lg_currency_id": eur_currency_id,
                "issuance_date": today - timedelta(days=60),
                "expiry_date": today + timedelta(days=120),
                "lg_period_months": 6, # Added
                "auto_renewal": False,
                "lg_type_id": advance_payment_lg_type_id,
                "lg_status_id": valid_status_id,
                "lg_operational_status_id": non_operative_op_status_id, # Non-Operative
                "payment_conditions": "Payment must be received before LG becomes operative. Ref: INV-2025-001",
                "description_purpose": "Advance Payment for software development project.",
                "issuing_bank_id": qnb_bank_id,
                "issuing_bank_address": "3 Finance Square, Cairo",
                "issuing_bank_phone": "16516",
                "issuing_method_id": bank_portal_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "PROJ-ACME-003"},
                "internal_contract_project_id": "SWDEV-PH2",
                "notes": "Waiting for client payment to activate LG.",
            },
            # Globex Industries LG (one basic LG)
            {
                "customer_id": globex_id,
                "beneficiary_corporate_id": globex_main_entity_id,
                "lg_sequence_number": 1, # Added
                "issuer_name": "Globex Construction",
                "issuer_id": "GC005",
                "lg_number": "GLOBEX/MO/PG/005",
                "lg_amount": 2500000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=180),
                "expiry_date": today + timedelta(days=90),
                "lg_period_months": 9, # Added
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Performance Guarantee for manufacturing plant expansion.",
                "issuing_bank_id": qnb_bank_id,
                "issuing_bank_address": "5 Industry Road, Metropolis",
                "issuing_bank_phone": "16516",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.globexindustries@example.com",
                "internal_owner_phone": "+1-555-002-9999",
                "internal_owner_id": "IOGLOBEX02",
                "manager_email": "manager.globexindustries@example.com",
                "lg_category_id": globex_mfg_category_id,
                "additional_field_values": {"Batch No.": "BTM-GBLX-2025-01"},
                "internal_contract_project_id": "PLANT-EXP-25",
                "notes": "Large project, active monitoring.",
            },
            # Cyberdyne Systems LG (one basic LG)
            {
                "customer_id": cyberdyne_id,
                "beneficiary_corporate_id": cyberdyne_rd_entity_id,
                "lg_sequence_number": 1, # Added
                "issuer_name": "Research Partners Inc.",
                "issuer_id": "RPI007",
                "lg_number": "CYBER/RD/AP/007",
                "lg_amount": 500000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=120),
                "expiry_date": today + timedelta(days=240),
                "lg_period_months": 8, # Added
                "auto_renewal": True,
                "lg_type_id": advance_payment_lg_type_id,
                "lg_status_id": valid_status_id,
                "lg_operational_status_id": operative_op_status_id, # Operative
                "payment_conditions": "Advance received on 2025-01-15.",
                "description_purpose": "Advance Payment for AI research grant.",
                "issuing_bank_id": aaib_bank_id,
                "issuing_bank_address": "7 Research Blvd, LA",
                "issuing_bank_phone": "19555",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.cyberdynesystems@example.com",
                "internal_owner_phone": "+1-555-003-9999",
                "internal_owner_id": "IOCYBER003",
                "manager_email": "manager.cyberdynesystems@example.com",
                "lg_category_id": cyberdyne_rg_category_id,
                "additional_field_values": {"Grant ID": "GRT-AI-001"},
                "internal_contract_project_id": "RSRCH-AI-2025",
                "notes": "Initial grant, awaiting renewal confirmation."
            },
            { # Another expired LG for testing
                "customer_id": globex_id,
                "beneficiary_corporate_id": globex_main_entity_id,
                "lg_sequence_number": 2, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Expired Test Co.",
                "issuer_id": "ETC021",
                "lg_number": "GLOBEX/MO/EX/021",
                "lg_amount": 10000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=365),
                "expiry_date": today - timedelta(days=30),
                "lg_period_months": 11, # Added
                "auto_renewal": False,
                "lg_type_id": financial_lg_type_id,
                "lg_status_id": expired_status_id, # Set to Expired status
                "description_purpose": "Expired LG for testing purposes.",
                "issuing_bank_id": cib_bank_id,
                "issuing_bank_address": "21 Expired St, Metropolis",
                "issuing_bank_phone": "19666",
                "issuing_method_id": manual_delivery_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.globexindustries@example.com",
                "internal_owner_phone": "+1-555-002-9999",
                "internal_owner_id": "IOGLOBEX02",
                "manager_email": "manager.globexindustries@example.com",
                "lg_category_id": globex_mfg_category_id,
                "internal_contract_project_id": "TEST-EXP-001",
                "notes": "This LG is intentionally expired for testing."
            },
            # NEW: LGs for testing Feature 1 & 2 reminders

            # Feature 1: LGs for Users & Admins Reminders
            # Auto-renew LG, First Reminder due (~30-7 = 23 days from expiry)
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 3, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Auto-Renew Reminder 1",
                "issuer_id": "ARR1_001",
                "lg_number": "ACME/REMINDER/AR1/001",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=100),
                "expiry_date": today + timedelta(days=23), # auto_renewal_days (30) - first_reminder_offset (7) = 23
                "lg_period_months": 4, # Added
                "auto_renewal": True,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Auto-renew LG for first reminder testing.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Reminder Bank Address",
                "issuing_bank_phone": "19623",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "REM-AR1-001"},
                "internal_contract_project_id": "REM-TEST-AR1",
                "notes": "Auto-renew LG, first reminder.",
            },
            # Auto-renew LG, Second Reminder due (~30-14 = 16 days from expiry)
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 4, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Auto-Renew Reminder 2",
                "issuer_id": "ARR2_002",
                "lg_number": "ACME/REMINDER/AR2/002",
                "lg_amount": 75000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=120),
                "expiry_date": today + timedelta(days=16), # auto_renewal_days (30) - second_reminder_offset (14) = 16
                "lg_period_months": 4, # Added
                "auto_renewal": True,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Auto-renew LG for second reminder testing.",
                "issuing_bank_id": cib_bank_id,
                "issuing_bank_address": "Reminder Bank Address",
                "issuing_bank_phone": "19666",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "REM-AR2-002"},
                "internal_contract_project_id": "REM-TEST-AR2",
                "notes": "Auto-renew LG, second reminder.",
            },
            # Non-Auto-renew LG, First Reminder due (~15-7 = 8 days from expiry)
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_west_entity_id,
                "lg_sequence_number": 1, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Non-Auto-Renew Reminder 1",
                "issuer_id": "NARR1_003",
                "lg_number": "ACME/REMINDER/NAR1/003",
                "lg_amount": 10000.00,
                "lg_currency_id": egp_currency_id,
                "issuance_date": today - timedelta(days=50),
                "expiry_date": today + timedelta(days=8), # forced_renew_days (15) - first_reminder_offset (7) = 8
                "lg_period_months": 1, # Added
                "auto_renewal": False,
                "lg_type_id": bid_bond_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Non-auto-renew LG for first reminder testing.",
                "issuing_bank_id": qnb_bank_id,
                "issuing_bank_address": "Reminder Bank Address",
                "issuing_bank_phone": "16516",
                "issuing_method_id": manual_delivery_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_hr_category_id,
                "internal_contract_project_id": "REM-TEST-NAR1",
                "notes": "Non-auto-renew LG, first reminder.",
            },
            # Non-Auto-renew LG, Second Reminder due (~15-14 = 1 day from expiry)
            {
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_west_entity_id,
                "lg_sequence_number": 2, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Non-Auto-Renew Reminder 2",
                "issuer_id": "NARR2_004",
                "lg_number": "ACME/REMINDER/NAR2/004",
                "lg_amount": 25000.00,
                "lg_currency_id": egp_currency_id,
                "issuance_date": today - timedelta(days=70),
                "expiry_date": today + timedelta(days=1), # forced_renew_days (15) - second_reminder_offset (14) = 1
                "lg_period_months": 2, # Added
                "auto_renewal": False,
                "lg_type_id": bid_bond_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Non-auto-renew LG for second reminder testing.",
                "issuing_bank_id": aaib_bank_id,
                "issuing_bank_address": "Reminder Bank Address",
                "issuing_bank_phone": "19555",
                "issuing_method_id": manual_delivery_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_hr_category_id,
                "internal_contract_project_id": "REM-TEST-NAR2",
                "notes": "Non-auto-renew LG, second reminder.",
            },

            # Feature 2: Internal Owner Renewal Reminders for Non-Auto-Renew LGs
            # LG for initial internal owner reminder (expiry within 60 days, not auto-renew)
            {
                "customer_id": globex_id,
                "beneficiary_corporate_id": globex_main_entity_id,
                "lg_sequence_number": 3, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Internal Owner Reminder 1",
                "issuer_id": "IOR1_001",
                "lg_number": "GLOBEX/OWNREM/001",
                "lg_amount": 100000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=30),
                "expiry_date": today + timedelta(days=55), # Within 60 days
                "lg_period_months": 2, # Added
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "LG for initial internal owner reminder testing.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Owner Reminder Bank Address",
                "issuing_bank_phone": "19623",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.globexindustries@example.com",
                "internal_owner_phone": "+1-555-002-9999",
                "internal_owner_id": "IOGLOBEX02",
                "manager_email": "manager.globexindustries@example.com",
                "lg_category_id": globex_mfg_category_id,
                "internal_contract_project_id": "OWNREM-TEST-1",
                "notes": "Internal owner reminder - initial.",
            },
            # LG for follow-up internal owner reminder (last sent 8 days ago, interval 7 days)
            {
                "customer_id": globex_id,
                "beneficiary_corporate_id": globex_main_entity_id,
                "lg_sequence_number": 4, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Internal Owner Reminder 2",
                "issuer_id": "IOR2_002",
                "lg_number": "GLOBEX/OWNREM/002",
                "lg_amount": 200000.00,
                "lg_currency_id": eur_currency_id,
                "issuance_date": today - timedelta(days=40),
                "expiry_date": today + timedelta(days=30), # Still within 60 days
                "lg_period_months": 2, # Added
                "auto_renewal": False,
                "lg_type_id": financial_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "LG for follow-up internal owner reminder testing.",
                "issuing_bank_id": cib_bank_id,
                "issuing_bank_address": "Owner Reminder Bank Address",
                "issuing_bank_phone": "19666",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.globexindustries@example.com",
                "internal_owner_phone": "+1-555-002-9999",
                "internal_owner_id": "IOGLOBEX02",
                "manager_email": "manager.globexindustries@example.com",
                "lg_category_id": globex_mfg_category_id,
                "internal_contract_project_id": "OWNREM-TEST-2",
                "notes": "Internal owner reminder - follow-up. Last sent 8 days ago.",
            },
            # LG that already had action recorded (should skip reminder)
            {
                "customer_id": globex_id,
                "beneficiary_corporate_id": globex_main_entity_id,
                "lg_sequence_number": 5, # Added, unique per beneficiary_corporate_id
                "issuer_name": "Internal Owner Reminder Skipped",
                "issuer_id": "IORS_003",
                "lg_number": "GLOBEX/OWNREM/003",
                "lg_amount": 50000.00,
                "lg_currency_id": egp_currency_id,
                "issuance_date": today - timedelta(days=20),
                "expiry_date": today + timedelta(days=45),
                "lg_period_months": 1, # Added
                "auto_renewal": False,
                "lg_type_id": bid_bond_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "LG to test skipping internal owner reminder if action recorded.",
                "issuing_bank_id": qnb_bank_id,
                "issuing_bank_address": "Owner Reminder Bank Address",
                "issuing_bank_phone": "16516",
                "issuing_method_id": manual_delivery_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.globexindustries@example.com",
                "internal_owner_phone": "+1-555-002-9999",
                "internal_owner_id": "IOGLOBEX02",
                "manager_email": "manager.globexindustries@example.com",
                "lg_category_id": globex_mfg_category_id,
                "internal_contract_project_id": "OWNREM-TEST-SKIP",
                "notes": "Internal owner reminder - should be skipped due to action.",
            },

            # Print reminder test LGs (these are the ones with special logic for creation_date/is_printed)
            { # This LG is for a print reminder scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 5, # Updated to be unique
                "issuer_name": "Print Reminder Test LG",
                "issuer_id": "PRTLG001",
                "lg_number": "ACME/AE/PRINT/001",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=3), # Approved 3 days ago for testing 2-day reminder
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print reminder functionality.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Test Bank Address",
                "issuing_bank_phone": "11111",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "PRT-001"},
                "internal_contract_project_id": "PRT-TEST-1",
                "notes": "Used for automated print reminder tests."
            },
            { # This LG is for a print reminder scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 7, # Updated to be unique
                "issuer_name": "Print Reminder Test LG",
                "issuer_id": "PRTLG001",
                "lg_number": "ACME/AE/Wa/001",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=3),
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print reminder functionality.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Test Bank Address",
                "issuing_bank_phone": "11111",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "WA-001"},
                "internal_contract_project_id": "PRT-TEST-1",
                "notes": "Used for automated print reminder tests."
            },
            { # This LG is for a print reminder scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 8, # Updated to be unique
                "issuer_name": "Print Reminder Test LG",
                "issuer_id": "PRTLG001",
                "lg_number": "ACME/AE/WA/002",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=5), # Approved 3 days ago for testing 2-day reminder
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print reminder functionality.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Test Bank Address",
                "issuing_bank_phone": "11111",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "WA-001"},
                "internal_contract_project_id": "PRT-TEST-1",
                "notes": "Used for automated print reminder tests."
            },
            { # This LG is for a print reminder scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 9, # Updated to be unique
                "issuer_name": "Print Reminder Test LG",
                "issuer_id": "PRTLG001",
                "lg_number": "ACME/AE/WA/003",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=10), # Approved 3 days ago for testing 2-day reminder
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print reminder functionality.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Test Bank Address",
                "issuing_bank_phone": "11111",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "WA-001"},
                "internal_contract_project_id": "PRT-TEST-1",
                "notes": "Used for automated print reminder tests."
            },
            { # This LG is for a print reminder scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 10, # Updated to be unique
                "issuer_name": "Print Reminder Test LG",
                "issuer_id": "PRTLG001",
                "lg_number": "ACME/AE/WA/004",
                "lg_amount": 50000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=3), # Approved 3 days ago for testing 2-day reminder
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print reminder functionality.",
                "issuing_bank_id": nbe_bank_id,
                "issuing_bank_address": "Test Bank Address",
                "issuing_bank_phone": "11111",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "WA-001"},
                "internal_contract_project_id": "PRT-TEST-1",
                "notes": "Used for automated print reminder tests."
            },
            { # This LG is for a print escalation scenario
                "customer_id": acme_corp_id,
                "beneficiary_corporate_id": acme_east_entity_id,
                "lg_sequence_number": 6, # Updated to be unique
                "issuer_name": "Print Escalation Test LG",
                "issuer_id": "PETLG001",
                "lg_number": "ACME/AE/ESCAL/001",
                "lg_amount": 75000.00,
                "lg_currency_id": usd_currency_id,
                "issuance_date": today - timedelta(days=6), # Approved 6 days ago for testing 5-day escalation
                "expiry_date": today + timedelta(days=365),
                "lg_period_months": 12,
                "auto_renewal": False,
                "lg_type_id": performance_lg_type_id,
                "lg_status_id": valid_status_id,
                "description_purpose": "Test LG for print escalation functionality.",
                "issuing_bank_id": cib_bank_id,
                "issuing_bank_address": "Test Bank Address 2",
                "issuing_bank_phone": "22222",
                "issuing_method_id": swift_mt760_method_id,
                "applicable_rule_id": urcg_rule_id,
                "internal_owner_email": "lg.owner.acmecorporation@example.com",
                "internal_owner_phone": "+1-555-001-9999",
                "internal_owner_id": "IOACME001",
                "manager_email": "manager.acmecorporation@example.com",
                "lg_category_id": acme_it_category_id,
                "additional_field_values": {"Project ID": "ESCAL-001"},
                "internal_contract_project_id": "PRT-TEST-ESCAL",
                "notes": "Used for automated print escalation tests."
            }
        ]
        
        # This section creates initial Customer, CustomerEntity, User, LGCategory
        # We need to ensure these are created and committed *before* attempting to retrieve their IDs for LG seeding.
        # Moved Customer onboarding and LG Category seeding to an earlier block.
        # This ensures IDs are available for the LGRecord seeding section.

        # ... (rest of the code for fetching IDs) ...

        for lg_data in lg_records_to_seed:
            try:
                # Calculate lg_period_months
                issue_date = lg_data["issuance_date"]
                expiry_date = lg_data["expiry_date"]
                # Calculate difference in days
                delta_days = (expiry_date - issue_date).days
                # Approximate months (integer division, could be more precise with dateutil.relativedelta if needed)
                lg_data["lg_period_months"] = max(1, round(delta_days / 30.44)) # Avg days in month

                existing_lg_record = db.query(models.LGRecord).filter(models.LGRecord.lg_number == lg_data["lg_number"]).first()
                if existing_lg_record:
                    print(f"  LG Record '{lg_data['lg_number']}' already exists. Skipping.")
                    # Logic for updating test LGs' dates and statuses (as per original file)
                    # This is still important for idempotency of test data setup for background tasks
                    if lg_data["lg_number"].startswith("ACME/AE/PRINT/") or lg_data["lg_number"].startswith("ACME/AE/ESCAL/"):
                        db_lg_record = existing_lg_record
                        db_lg_record.issuance_date = lg_data["issuance_date"]
                        db.add(db_lg_record)
                        db.flush()
                        print(f"  Updated issuance_date for test LG {lg_data['lg_number']}.")

                        related_approval_request = db.query(models.ApprovalRequest).filter(
                            models.ApprovalRequest.entity_type == "LGRecord",
                            models.ApprovalRequest.entity_id == db_lg_record.id,
                            models.ApprovalRequest.action_type.in_([
                                "LG_RELEASE", "LG_LIQUIDATE", "LG_DECREASE_AMOUNT",
                                "LG_ACTIVATE_NON_OPERATIVE"
                            ])
                        ).first()

                        if related_approval_request:
                            related_approval_request.created_at = datetime.combine(lg_data["issuance_date"], datetime.min.time())
                            related_approval_request.status = models.ApprovalRequestStatusEnum.APPROVED
                            related_approval_request.related_instruction_id = None
                            related_approval_request.request_details = related_approval_request.request_details if related_approval_request.request_details else {}
                            related_approval_request.request_details["print_notification_status"] = "NONE"
                            db.add(related_approval_request)
                            db.flush()
                            print(f"  Updated ApprovalRequest {related_approval_request.id} for test LG {lg_data['lg_number']}.")

                            related_instruction = db.query(models.LGInstruction).filter(
                                models.LGInstruction.lg_record_id == db_lg_record.id,
                                models.LGInstruction.approval_request_id == related_approval_request.id
                            ).first()
                            if related_instruction:
                                related_instruction.instruction_date = datetime.combine(lg_data["issuance_date"], datetime.min.time())
                                related_instruction.is_printed = False
                                # Removed hardcoded sequence numbers, will be updated by crud_lg_instruction.create if needed
                                # related_instruction.global_seq_per_lg = 1
                                # related_instruction.type_seq_per_lg = 1
                                db.add(related_instruction)
                                db.flush()
                                print(f"  Updated LGInstruction {related_instruction.id} for test LG {lg_data['lg_number']}.")
                            else: # This means no related_instruction exists yet for this AR
                                print(f"  Creating dummy instruction for test LG {lg_data['lg_number']} related to Approval Request {related_approval_request.id}.")
                                # --- CRITICAL CHANGE START ---
                                # If this is an LG_EXTENSION action, we do NOT create the instruction here.
                                # The user's action will create it.
                                if related_approval_request.action_type == "LG_EXTENSION":
                                    print(f"    Skipping dummy instruction creation for LG {db_lg_record.lg_number} (Action: {related_approval_request.action_type}) as it's meant for user-initiated extension.")
                                    # Ensure related_instruction_id is None for this AR
                                    related_approval_request.related_instruction_id = None
                                    db.add(related_approval_request)
                                    db.flush()
                                    # We need to commit the AR change here so it's visible.
                                    # This is an exception to the general seed_db commit strategy.
                                    db.commit() 
                                    continue # Skip to the next LG record in the loop
                                # --- CRITICAL CHANGE END ---
                                dummy_instruction_type_str = related_approval_request.action_type.replace("LG_", "")
                                # --- CRITICAL CHANGE END ---
                                # Map action type string to InstructionTypeCode enum member
                                instruction_type_code_map = {
                                    "EXTENSION": InstructionTypeCode.EXT,
                                    "RELEASE": InstructionTypeCode.REL,
                                    "LIQUIDATE": InstructionTypeCode.LIQ,
                                    "DECREASE_AMOUNT": InstructionTypeCode.DEC,
                                    "AMEND": InstructionTypeCode.AMD,
                                    "ACTIVATE_NON_OPERATIVE": InstructionTypeCode.ACT,
                                    "REMINDER_TO_BANKS": InstructionTypeCode.REM,
                                }
                                instruction_type_code = instruction_type_code_map.get(dummy_instruction_type_str)

                                # For dummy instructions, we'll use a generic sub-instruction code
                                sub_instruction_code = SubInstructionCode.ORIGINAL # Using ORIGINAL as it's a primary instruction

                                dummy_template = crud_template.get_single_template(db, related_approval_request.action_type, is_global=True, is_notification_template=False)
                                if not dummy_template:
                                    print(f"    WARNING: No template found for {related_approval_request.action_type}. Cannot create dummy instruction.")
                                else:
                                    # Fetch codes for serial generation
                                    beneficiary_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == db_lg_record.beneficiary_corporate_id).first()
                                    lg_category = db.query(models.LGCategory).filter(models.LGCategory.id == db_lg_record.lg_category_id).first()

                                    if not beneficiary_entity or not lg_category:
                                        print(f"    WARNING: Missing beneficiary_entity or lg_category for LG {db_lg_record.id}. Cannot generate serial for dummy instruction.")
                                    else:
                                        # AWAIT the async function call here
                                        dummy_serial_number, global_seq_val, type_seq_val = await crud_lg_instruction.get_next_serial_number(
                                            db,
                                            lg_record_id=db_lg_record.id,
                                            entity_code=beneficiary_entity.code,
                                            lg_category_code=lg_category.code,
                                            lg_sequence_number=str(db_lg_record.lg_sequence_number).zfill(4),
                                            instruction_type_code=instruction_type_code,
                                            sub_instruction_code=sub_instruction_code
                                        )

                                        # AWAIT the async function call here
                                        new_dummy_instruction = await crud_lg_instruction.create(
                                            db,
                                            obj_in=LGInstructionCreate(
                                                lg_record_id=db_lg_record.id,
                                                instruction_type=related_approval_request.action_type,
                                                serial_number=dummy_serial_number,
                                                template_id=dummy_template.id,
                                                status="Instruction Issued",
                                                instruction_date=datetime.combine(lg_data["issuance_date"], datetime.min.time()),
                                                maker_user_id=related_approval_request.maker_user_id,
                                                approval_request_id=related_approval_request.id,
                                                is_printed=False
                                            ),
                                            global_seq_per_lg=global_seq_val, # Pass calculated values
                                            type_seq_per_lg=type_seq_val # Pass calculated values
                                        )
                                        db.flush()
                                        related_approval_request.related_instruction_id = new_dummy_instruction.id
                                        db.add(related_approval_request)
                                        db.flush()
                                        print(f"    Created dummy instruction {new_dummy_instruction.serial_number} linked to AR {related_approval_request.id}.")

                    continue # Skip to next LG record as it was an existing test LG

                internal_owner_contact_obj = crud_internal_owner_contact.create_or_get(
                    db,
                    obj_in=InternalOwnerContactCreate(
                        email=lg_data["internal_owner_email"],
                        phone_number=lg_data["internal_owner_phone"],
                        internal_id=lg_data.get("internal_owner_id"),
                        manager_email=lg_data["manager_email"]
                    ),
                    customer_id=lg_data["customer_id"],
                    user_id=system_owner_id # Using system_owner_id for seeding
                )

                lg_record_create_payload = {
                    key: value for key, value in lg_data.items()
                    if key not in ["lg_sequence_number", "lg_period_months", "lg_type_id"] # Exclude auto-generated or relational keys
                }
                lg_record_create_payload["lg_type_id"] = lg_data["lg_type_id"]
                lg_record_create_payload["internal_owner_contact_id"] = internal_owner_contact_obj.id

                # AWAIT the async function call here
                await crud_lg_record.create(
                    db,
                    obj_in=LGRecordCreate(**lg_record_create_payload),
                    customer_id=lg_data["customer_id"],
                    user_id=system_owner_id # Using system_owner_id for seeding
                )
                print(f"  Added LG Record: {lg_data['lg_number']} for Customer ID {lg_data['customer_id']}")
            except Exception as e:
                print(f"  ERROR seeding LG record '{lg_data.get('lg_number', 'N/A')}': {e}")
                traceback.print_exc()
        db.commit() # Final commit for all LG records

        print("\nDatabase seeding complete!")
    except Exception as e:
        db.rollback()
        print(f"FATAL ERROR during seeding: {e}")
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(seed_db()) # Run the async function