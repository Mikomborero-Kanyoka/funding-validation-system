# Funding Guard 🛡️

A rule-based engine for validating funding applications against historical records using Excel datasets.

---

## 🚨 Problem

Funding organizations often face challenges such as:
- Accidentally funding the same individual multiple times  
- Violating internal policies (e.g. funding too soon or exceeding limits)  
- Lack of visibility into pending or delayed applications  
- Manual validation processes that are slow and error-prone  

These issues can lead to inefficiencies, financial loss, and reduced trust.

---

## ✅ Solution

Funding Guard provides a lightweight, rule-based system that:
- Validates new funding applications against historical data  
- Detects duplicate or repeated funding  
- Enforces time-based and amount-based constraints  
- Flags overdue or unfulfilled applications  
- Maintains an up-to-date funding history  

No AI. No heavy backend. Just fast, explainable logic.

---

## ⚙️ Features

- 📂 Upload Excel files (historical data + new applications)  
- 🔍 Identity matching (ID-based and fuzzy name matching)  
- ⏱️ Time-based validation (e.g. no funding within a defined period)  
- 💰 Funding amount validation rules  
- ⚠️ Detection of overdue applications (e.g. pending > 120 days)  
- 🔄 Automatic history updates (append and merge logic)  
- 📊 Human-readable validation reports  

---

## 🧠 How It Works

1. Upload historical funding data  
2. Upload new funding applications  
3. The system matches applicants against existing records  
4. Validation rules are applied:
   - Duplicate detection  
   - Time constraints  
   - Funding limits  
5. Applications are flagged with clear explanations  
6. The funding history is updated with new records  

---

## 📊 Example Output

- ✅ Approved — No prior funding detected  
- ⚠️ Flagged — Similar name found in previous records  
- ❌ Rejected — Recently funded within restricted period  
- ⚠️ Overdue — Application pending for more than 120 days  

---

## 🧾 Data Requirements

### Historical Data (Excel)
- Name  
- ID Number (recommended)  
- Amount Funded  
- Status (PENDING / FUNDED / REJECTED)  
- Application Date  
- Funding Date  

### New Applications (Excel)
- Name  
- ID Number (if available)  
- Requested Amount  
- Application Date  

---

## 🎯 Use Cases

- NGOs distributing aid  
- Microfinance institutions  
- Government funding programs  
- Scholarship and grant allocation systems  

---

## 🚀 Getting Started

```bash
git clone https://github.com/your-username/funding-guard.git
cd funding-guard
npm install
npm run dev