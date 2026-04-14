import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.insert(0, 'backend')

from main import (
    analyze_application,
    normalize_identifier,
)

# Test Case: Application matches via Customer ID but has different EC number
# History has CUST001 with EC0410 and Name "Old Name"
# Application has CUST001 with EC9999 and Name "New Name"
# The system should flag fraud for CUST001 if names are different,
# but it should NOT flag fraud for EC0410 if the application is not using EC0410.

history_data = {
    'EC_NUMBER': ['0410'],
    'CUSTOMER_NO': ['CUST001'],
    'CUSTOMER_NAME1': ['Old Name'],
    'BOOK_DATE': [datetime.now() - timedelta(days=30)],
    'AMOUNT_FINANCED': [5000],
}
history_df = pd.DataFrame(history_data)
history_df["_ec_key"] = history_df["EC_NUMBER"].map(normalize_identifier)
history_df["_customer_key"] = history_df["CUSTOMER_NO"].map(normalize_identifier)

history_lookup = {}
for idx, row in history_df.iterrows():
    key_ec = normalize_identifier(row['EC_NUMBER'])
    key_cust = normalize_identifier(row['CUSTOMER_NO'])
    if key_ec:
        history_lookup.setdefault(key_ec, []).append(idx)
    if key_cust:
        history_lookup.setdefault(key_cust, []).append(idx)

# Application with same Customer ID but DIFFERENT EC number
app_row = pd.Series({
    'EC_NUMBER': '9999',
    'CUSTOMER_NO': 'CUST001',
    'CUSTOMER_NAME1': 'New Name',
    'BOOK_DATE': datetime.now(),
})

result = analyze_application(app_row, 0, history_df, history_lookup)
print(f"Anomaly Reasons: {result['anomaly_reasons']}")

# Expected: Identity fraud for Account Number 'CUST001'
# UNEXPECTED: Identity fraud for EC number '0410' (This is the bug!)
for reason in result['anomaly_reasons']:
    if "EC number '0410'" in reason:
        print("BUG REPRODUCED: Found identity fraud for EC number '0410' which the application did not use!")
