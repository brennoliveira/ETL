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
  connection = pg.connect(user='admin', password='123456789', host='oracle-74473-0.cloudclusters.net', port='12272', database='XE')
  curs = connection.cursor()
  # connection.rollback()
  curs.execute(sql)
  print(curs.fetchall())
  connection.commit()



def connect_db():
  DIALECT = 'oracle'
  SQL_DRIVER = 'cx_oracle'
  USERNAME = 'admin' #enter your username
  PASSWORD = '123456789' #enter your password
  HOST = 'oracle-74473-0.cloudclusters.net' #enter the oracle db host url
  PORT = 12272 # enter the oracle port number
  SERVICE = 'XE' # enter the oracle db service name
  ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

  engine = create_engine(ENGINE_PATH_WIN_AUTH)
  return engine