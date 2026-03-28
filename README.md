# 🧖 Hamman Analytics Pipeline

End-to-end data pipeline for a wellness spa, transforming raw Excel data into structured datasets for analytics.

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/WLozanoH/hamman-analytics-pipeline.git
cd hamman-analytics-pipeline
```

### 2. Create virtual environment

```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run pipeline

```bash
python src/run_pipeline.py
```
---

## 📂 Project Structure

```text
hamman-analytics-pipeline/
│
├── src/
│   └── run_pipeline.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```
---
## 🔐 Notes

- Sensitive data is not included in the repository.
- `.env` file is required for configuration.