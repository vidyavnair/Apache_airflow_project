from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import json

# API Base URL
BASE_URL = "https://dummyjson.com"

# Database Configuration
DB_NAME = "dummyjson_products1.db"

def init_db():
    """Initialize database with products table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pid INTEGER,
            title TEXT,
            category TEXT,
            price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

class Login:
   
    def __init__(self):
        self.token = None

    def login(self):
        
        payload = {"username": "emilys", "password": "emilyspass"}
        response = requests.post(f"{BASE_URL}/auth/login", json=payload)
        if response.status_code == 200:
            data = response.json()
            self.token = data.get("accessToken")
            print(f"Login successful. Token: {self.token}")
        else:
            raise Exception("Login failed")

class Categories(Login):
    """Handles fetching categories from API."""
    def __init__(self):
        super().__init__()
        self.categories = []

    def fetch_categories(self):
        """Fetch available categories from API."""
        self.login()
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(f"{BASE_URL}/products/categories", headers=headers)
        if response.status_code == 200:
            self.categories = response.json()
            print(f"Available Categories: {self.categories}")
        else:
            raise Exception("Failed to fetch categories")

class Products(Categories):
    """Handles fetching products for each category and saving to database."""
    def __init__(self):
        super().__init__()
        init_db()

    def fetch_and_save_products(self):
        """Fetch products by category and save to database."""
        self.fetch_categories()
        headers = {"Authorization": f"Bearer {self.token}"}
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        for category in self.categories:
            url = category['url']
            names=category['name']
     
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                products = response.json().get("products", [])
                
                for product in products:
                    
                    
                    try:
                        cursor.execute("INSERT INTO products (pid, title, category, price) VALUES (?, ?, ?, ?)",
                                        (product['id'], product['title'], names, product['price']))
                        print(f"Saved Product: {product['title']} - {names} - ${product['price']}")
                    except Exception as e:
                        print(e)
            else:
                print(f"Failed to fetch products for category: {category}")
        conn.commit()
        conn.close()

# Airflow DAG Definition
def login_task():
    Login().login()

def fetch_categories_task():
    Categories().fetch_categories()

def fetch_products_task():
    Products().fetch_and_save_products()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dummyjson',
    default_args=default_args,
    description='Fetch data from DummyJSON API',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
)

login_operator = PythonOperator(
    task_id='login',
    python_callable=login_task,
    dag=dag,
)

categories_operator = PythonOperator(
    task_id='categories',
    python_callable=fetch_categories_task,
    dag=dag,
)

products_operator = PythonOperator(
    task_id='products',
    python_callable=fetch_products_task,
    dag=dag,
)

# Task Dependencies
login_operator >> categories_operator >> products_operator

