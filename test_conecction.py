import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Parameters connection
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
port = os.getenv('DB_PORT')

connection_string = f'DRIVER={{SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'

try:
  conn = pyodbc.connect(connection_string)
  print('Conexión establecida')
  
except Exception as e:
    print(f"Error al conectar con el servidor: {e}")
finally:
  if conn:
    conn.close()
    
