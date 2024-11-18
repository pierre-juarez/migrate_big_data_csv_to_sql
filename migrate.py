import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables
load_dotenv()

# Parameters connection
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
port = os.getenv('DB_PORT')

connection_string = f'DRIVER={{SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'

# Read csv file
csv_file = os.getenv('PATH_FILE')
df = pd.read_csv(csv_file, sep=";")

df = df.dropna(subset=['sku','with_cu'])
df = df.drop_duplicates(subset=['sku'], keep='last')

print(f"ruta del archivo: {csv_file}")

# Define block size
block_size = 1000

try:
  conn = pyodbc.connect(connection_string)
  cursor = conn.cursor()
  print('Conexión establecida')
  
  # Dividing the data into blocks
  for start in range(0, len(df), block_size):
    block = df.iloc[start:start+block_size]
    
    
   # Make MERGE query for the current block 
    values = []
    for index, row in block.iterrows():
      sku = row['sku']
      with_cu = row['with_cu']
      print(f"Procesando SKU: {sku}, with_cu: {with_cu}")
      current_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
      values.append(f"('{sku}', '{with_cu}', '{current_timestamp}')")
      
    
    # Create the VALUES string
    values_str = ', '.join(values)
    
    # Create the MERGE query
    values_str = ', '.join(values)
    merge_query = f"""MERGE INTO tb_products AS target
                      USING (VALUES {values_str}) AS source (sku, with_cu, last_updated)
                      ON target.sku = source.sku
                      WHEN MATCHED THEN
                        UPDATE SET target.with_cu = source.with_cu, target.last_updated = source.last_updated
                      WHEN NOT MATCHED THEN
                        INSERT (sku, with_cu, last_updated)
                        VALUES (source.sku, source.with_cu, source.last_updated);
                      """
    
    # Execute the query
    cursor.execute(merge_query)
    conn.commit()
    print(f"Bloque de registros {start}-{start+block_size} procesado.")
    
  print("Migración completada")                  
  
except Exception as e:
    print(f"Error al conectar o ejecutar la consulta: {e}")
finally:
  if conn:
    cursor.close()
    conn.close()
    
