import pandas as pd
from sqlalchemy import create_engine

# Step 1: Load the CSV file
df = pd.read_csv("/home/hadoop/Documents/Codes/Airflow/webstories.csv")
print(df.head())

# Step 2: Database connection details
user = 'username'
password = '****'
host = 'localhost'
port = '5432'
database = 'postgres'

# Step 3: Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# Step 4: Write DataFrame to PostgreSQL
# pandas 2.1.4 supports passing engine directly
df.to_sql('webstories', con=engine, if_exists='append', index=False)

print("✅ Data loaded successfully.")
