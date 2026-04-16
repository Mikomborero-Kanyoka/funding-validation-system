#!/usr/bin/env python3
"""Test script to verify enhanced anomaly detection"""

import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.insert(0, 'backend')

from main import (
    analyze_application,
    build_application_record,
    normalize_identifier,
    parse_date,
    RECENT_APPLICATION_WINDOW_DAYS,
)

# Create sample test data
print("Testing enhanced anomaly detection...\n")

# Test Case 1: Recent loan (within 14 days) - should flag "recent_duplicate"
print("Test 1: Recent loan within 14 days")
print("-" * 50)

history_data = {
    'EC_NUMBER': ['EC001', 'EC001', 'EC002'],
    'CUSTOMER_NO': ['CUST001', 'CUST001', 'CUST002'],
    'CUSTOMER_NAME1': ['John Doe', 'John Doe', 'Jane Smith'],
    'BOOK_DATE': [
        datetime.now() - timedelta(days=5),  # 5 days ago
        datetime.now() - timedelta(days=30),  # 30 days ago
        datetime.now() - timedelta(days=60),  # 60 days ago
    ],
    'AMOUNT_FINANCED': [5000, 3000, 2000],
    'ACCOUNT_NUMBER': ['ACC001', 'ACC001', 'ACC002'],
}
history_df = pd.DataFrame(history_data)
history_df["_ec_key"] = history_df["EC_NUMBER"].map(normalize_identifier)
history_df["_customer_key"] = history_df["CUSTOMER_NO"].map(normalize_identifier)

# Build history lookup
history_lookup = {}
for idx, row in history_df.iterrows():
    key_ec = normalize_identifier(row['EC_NUMBER'])
    key_cust = normalize_identifier(row['CUSTOMER_NO'])
    if key_ec:
        history_lookup.setdefault(key_ec, []).append(idx)
    if key_cust:
        history_lookup.setdefault(key_cust, []).append(idx)

# Create application row (same EC/CUST as recent history)
app_row = pd.Series({
    'EC_NUMBER': 'EC001',
    'CUSTOMER_NO': 'CUST001',
    'CUSTOMER_NAME1': 'John Doe',
    'AMOUNT_FINANCED': 7000,
    'BOOK_DATE': datetime.now(),
    'ACCOUNT_NUMBER': 'ACC001',
})

result = analyze_application(app_row, 0, history_df, history_lookup)
print(f"Application: EC={result['ec_number']}, CUST={result['customer_no']}, Name={result['applicant_name']}")
print(f"Category: {result['category']}")
print(f"Anomaly Reasons: {result['anomaly_reasons']}")
print(f"Main Reason: {result['reason']}")
print(f"Match Count: {result['history_match_count']}, Recent: {result['recent_match_count']}")
assert result['category'] == 'anomaly', "Should be flagged as anomaly"
assert "Previous loan found within the last" in result['anomaly_reasons'][0], "Should detect recent loan"
print("PASSED\n")

# Test Case 2: Different names, same EC/ID beyond 14 days - should flag "name_mismatch"
print("Test 2: Different names with same EC/ID (beyond 14 days)")
print("-" * 50)

history_data2 = {
    'EC_NUMBER': ['EC002', 'EC002'],
    'CUSTOMER_NO': ['CUST002', 'CUST002'],
    'CUSTOMER_NAME1': ['Jane Smith', 'Jane Mitchell'],  # Different names!
    'BOOK_DATE': [
        datetime.now() - timedelta(days=60),  # 60 days ago
        datetime.now() - timedelta(days=90),  # 90 days ago
    ],
    'AMOUNT_FINANCED': [5000, 3000],
    'ACCOUNT_NUMBER': ['ACC002', 'ACC002'],
}
history_df2 = pd.DataFrame(history_data2)
history_df2["_ec_key"] = history_df2["EC_NUMBER"].map(normalize_identifier)
history_df2["_customer_key"] = history_df2["CUSTOMER_NO"].map(normalize_identifier)

# Build history lookup
history_lookup2 = {}
for idx, row in history_df2.iterrows():
    key_ec = normalize_identifier(row['EC_NUMBER'])
    key_cust = normalize_identifier(row['CUSTOMER_NO'])
    if key_ec:
        history_lookup2.setdefault(key_ec, []).append(idx)
    if key_cust:
        history_lookup2.setdefault(key_cust, []).append(idx)

# Create application row (same EC/CUST but different name)
app_row2 = pd.Series({
    'EC_NUMBER': 'EC002',
    'CUSTOMER_NO': 'CUST002',
    'CUSTOMER_NAME1': 'Jane Anderson',  # Different name!
    'AMOUNT_FINANCED': 8000,
    'BOOK_DATE': datetime.now(),
    'ACCOUNT_NUMBER': 'ACC002',
})

result2 = analyze_application(app_row2, 0, history_df2, history_lookup2)
print(f"Application: EC={result2['ec_number']}, CUST={result2['customer_no']}, Name={result2['applicant_name']}")
print(f"Category: {result2['category']}")
print(f"Anomaly Reasons: {result2['anomaly_reasons']}")
print(f"Main Reason: {result2['reason']}")
assert result2['category'] == 'anomaly', "Should be flagged as anomaly"
assert any("possible fraud" in reason for reason in result2['anomaly_reasons']), "Should detect name mismatch"
print("PASSED\n")

# Test Case 3: No anomalies - should be clear
print("Test 3: No anomalies (clear record)")
print("-" * 50)

history_data3 = {
    'EC_NUMBER': ['EC003'],
    'CUSTOMER_NO': ['CUST003'],
    'CUSTOMER_NAME1': ['Bob Jones'],
    'BOOK_DATE': [datetime.now() - timedelta(days=100)],  # > 14 days ago
    'AMOUNT_FINANCED': [5000],
    'ACCOUNT_NUMBER': ['ACC003'],
}
history_df3 = pd.DataFrame(history_data3)

history_lookup3 = {}
for idx, row in history_df3.iterrows():
    key_ec = normalize_identifier(row['EC_NUMBER'])
    key_cust = normalize_identifier(row['CUSTOMER_NO'])
    if key_ec:
        history_lookup3.setdefault(key_ec, []).append(idx)
    if key_cust:
        history_lookup3.setdefault(key_cust, []).append(idx)

app_row3 = pd.Series({
    'EC_NUMBER': 'EC003',
    'CUSTOMER_NO': 'CUST003',
    'CUSTOMER_NAME1': 'Bob Jones',
    'AMOUNT_FINANCED': 6000,
    'BOOK_DATE': datetime.now(),
    'ACCOUNT_NUMBER': 'ACC003',
})

result3 = analyze_application(app_row3, 0, history_df3, history_lookup3)
print(f"Application: EC={result3['ec_number']}, CUST={result3['customer_no']}, Name={result3['applicant_name']}")
print(f"Category: {result3['category']}")
print(f"Anomaly Reasons: {result3['anomaly_reasons']}")
print(f"Main Reason: {result3['reason']}")
assert result3['category'] == 'clear', "Should be clear"
assert len(result3['anomaly_reasons']) == 0, "Should have no anomaly reasons"
print("PASSED\n")

# Test Case 4: Multiple anomalies - both recent AND name mismatch
print("Test 4: Multiple anomalies (both recent AND name mismatch)")
print("-" * 50)

history_data4 = {
    'EC_NUMBER': ['EC004', 'EC004'],
    'CUSTOMER_NO': ['CUST004', 'CUST004'],
    'CUSTOMER_NAME1': ['Alice Brown', 'Alice King'],  # Different names
    'BOOK_DATE': [
        datetime.now() - timedelta(days=3),  # Recent!
        datetime.now() - timedelta(days=90),  # Old
    ],
    'AMOUNT_FINANCED': [5000, 3000],
    'ACCOUNT_NUMBER': ['ACC004', 'ACC004'],
}
history_df4 = pd.DataFrame(history_data4)

history_lookup4 = {}
for idx, row in history_df4.iterrows():
    key_ec = normalize_identifier(row['EC_NUMBER'])
    key_cust = normalize_identifier(row['CUSTOMER_NO'])
    if key_ec:
        history_lookup4.setdefault(key_ec, []).append(idx)
    if key_cust:
        history_lookup4.setdefault(key_cust, []).append(idx)

app_row4 = pd.Series({
    'EC_NUMBER': 'EC004',
    'CUSTOMER_NO': 'CUST004',
    'CUSTOMER_NAME1': 'Alice Turner',  # Different name!
    'AMOUNT_FINANCED': 9000,
    'BOOK_DATE': datetime.now(),
    'ACCOUNT_NUMBER': 'ACC004',
})

result4 = analyze_application(app_row4, 0, history_df4, history_lookup4)
print(f"Application: EC={result4['ec_number']}, CUST={result4['customer_no']}, Name={result4['applicant_name']}")
print(f"Category: {result4['category']}")
print(f"Anomaly Reasons ({len(result4['anomaly_reasons'])}): {result4['anomaly_reasons']}")
print(f"Main Reason: {result4['reason']}")
assert result4['category'] == 'anomaly', "Should be flagged as anomaly"
assert len(result4['anomaly_reasons']) >= 2, f"Should have 2+ anomalies, got {len(result4['anomaly_reasons'])}"
print("PASSED\n")

print("=" * 50)
print("All tests passed!")
print("=" * 50)
