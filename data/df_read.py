import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://brenno:123456789@209.209.40.88:19073/folhaDB')
sql = 'SELECT * FROM folha.folhas_pagamentos'
df = pd.read_sql_query(sql, con=engine)

print(df)