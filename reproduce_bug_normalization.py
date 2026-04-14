import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.insert(0, 'backend')

from main import (
    analyze_application,
    normalize_identifier,
)

# Test Case: Normalization consistency
# History has EC0410 (string)
# Application has 410 (number)
# They should match and flag fraud if names are different.

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

# Application with EC as a number (that normalizes to '410')
# Wait, normalize_identifier('0410') is '0410'.
# normalize_identifier(410.0) is '410'.
# So they DON'T match if leading zeros are important.
# In many systems, leading zeros in employee numbers/ECs ARE important.
# However, if Excel strips them, we might have a problem.

print(f"Normalized '0410': {normalize_identifier('0410')}")
print(f"Normalized 410.0: {normalize_identifier(410.0)}")

# If the user says '0410' and it's being compared to '410', 
# my groupby('_ec_key') fix ensures that we use the SAME normalization for both.

app_row = pd.Series({
    'EC_NUMBER': 410.0,
    'CUSTOMER_NO': 'CUST001',
    'CUSTOMER_NAME1': 'New Name',
    'BOOK_DATE': datetime.now(),
})

# With current normalize_identifier, '0410' != '410'.
# So they won't match via EC_NUMBER.
# But they WILL match via CUSTOMER_NO.
# Once matched via CUSTOMER_NO, the system will group by _ec_key.
# The history row has _ec_key='0410'.
# The application has app_ec_key='410'.
# Since '410' != '0410', it won't combine names for '0410'.

result = analyze_application(app_row, 0, history_df, history_lookup)
print(f"Anomaly Reasons: {result['anomaly_reasons']}")
