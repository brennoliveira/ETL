import psycopg2 as pg, pandas as pd, sqlite3
from sqlalchemy import create_engine


def read_df_1(link, sql):
  engine = create_engine(link)
  df = pd.read_sql_query(sql, con=engine)

  print(df)
  return df


def read_df_2(link, sql):
  conn = sqlite3.connect(link)
  curs = conn.cursor()
  curs.execute(sql)
  conn.commit()
  conn.close()


def read_df_3(sql):
  connection = pg.connect(user='brenno', password='123456789', host='209.209.40.88', port='19073', database='folhaDB')
  curs = connection.cursor()
  connection.rollback()
  curs.execute(sql)
  print(curs.fetchall())
  connection.commit()

