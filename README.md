# 🧖 Hamman Analytics Pipeline

End-to-end ETL pipeline that transforms raw Excel data from a wellness spa into a clean, structured dataset ready for business analytics and reporting.

---

## 🚀 Project Summary

This project automates the transformation of operational data into a reliable analytical dataset.

It processes raw Excel files containing:

- Customer master data
- Daily transaction records

And converts them into a clean dataset suitable for tools like Power BI.

---

## ⚙️ Key Features

- Automated data extraction from Excel
- Data cleaning and standardization
- Customer identity matching and enrichment
- Business rule implementation (services, payments, pricing)
- Gender inference based on behavioral and contextual data
- Final dataset generation for analytics
- Export to CSV for downstream consumption

---

## 🔄 ETL Workflow

The pipeline is structured into clear stages:

### 1. Extract
Reads raw data from Excel sources.

### 2. Clean Customers
- Normalize names
- Clean DNI values
- Remove duplicates
- Generate matching keys

### 3. Clean Transactions
- Standardize columns
- Handle missing values
- Clean names and dates
- Normalize categorical fields

### 4. Merge & Matching
- Match transactions with customers using a safe key
- Complete missing DNI, names, and phone numbers
- Apply fallback and recovery logic

### 5. Business Rules
- Normalize payment methods
- Classify memberships / gift cards
- Standardize services and descriptions
- Infer service type from price
- Assign gender using multiple rules

### 6. Final Dataset
- Select analytical fields
- Standardize schema
- Export clean dataset to CSV

---

## ⚙️ Tech Stack

- **Python**
- **Pandas**
- **OpenPyXL**
- **python-dotenv**

---


## 🧱 Project Structure

```text
src/
├── config.py
├── etl.py
└── run_pipeline.py

data/
├── raw/
└── processed/
```
---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/WLozanoH/hamman-analytics-pipeline.git
cd hamman-analytics-pipeline
```

### 2. Create virtual environment

```
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create environment file

- Create a `.env` file in the project root based on .env.example


### 5. How to Run

```bash
python src/run_pipeline.py
```

---

### 📤 Output

The pipeline generates a clean dataset with fields such as:

- fecha
- nombre
- dni
- genero
- servicios
- descripcion
- metodo_de_pago
- total

Ready for direct use in BI tools.

---
### 🧠 What This Project Demonstrates
- Real-world ETL design and implementation
- Data cleaning strategies for messy operational data
- Identity resolution using conservative matching
- Business logic translation into data transformations
- Modular and maintainable pipeline structure

---
## 🔐 Notes

- Sensitive data is not included in the repository.
- Environment variables are managed using `.env`
- Use `.env.example` as a template.
- The pipeline is designed for scalability and integration with SQL/BI tools

---

## 👤 Author

Wilmer Lozano

Data Analyst | ETL Developer
