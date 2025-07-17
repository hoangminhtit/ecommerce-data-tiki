# 📦 E-commerce Data Pipeline 

After a two-month break, I have finished this project as preparation for the upcoming year at my university.  
This project demonstrates a basic **data pipeline**, including:

- Crawling e-commerce product data  
- Transforming and cleaning raw data  
- Loading it into a PostgreSQL database  
- Visualizing insights (e.g., price, category distribution...)

---

## 🛠️ Technologies I used:
- Python (Selenium, Pandas ,...)
- PostgreSQL
- Apache Airflow (for scheduling + orchestration)
- DBCeaver (connect to Postgres database)
- Docker

---

## 🚀 How to run this project

```bash
# 1. Clone this repository
git clone https://github.com/hoangminhtit/ecommerce-data-tiki.git

# 2. Navigate to the project folder
cd your-repo

# 3. Start the containers
docker compose up --build -d
```

---
## You can access:
- Apache Airflow: http://localhost:8080/
- Postgres: You can connect to the PostgreSQL database using **DBeaver** or any SQL client (host: localhost, port: 5432, username: postgres, password: postgres)

