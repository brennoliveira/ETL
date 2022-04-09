import timeit
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

# BASE = declarative_base()
metadata = sa.MetaData(bind=None)

def connect_db():
  DIALECT = 'oracle'
  SQL_DRIVER = 'cx_oracle'
  USERNAME = 'admin' #enter your username
  PASSWORD = '123456789' #enter your password
  HOST = 'oracle-74473-0.cloudclusters.net' #enter the oracle db host url
  PORT = 12272 # enter the oracle port number
  SERVICE = 'XE' # enter the oracle db service name
  ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

  try:
    # start = timeit.default_timer()

    engine = sa.create_engine(ENGINE_PATH_WIN_AUTH)
    # end = timeit.default_timer()
    # exec_time =  (end - start)
    # print(f"Tempo de execução: {exec_time:.2f}s")
    print('connecting...')
    return engine

  except Exception as e:
    print('error: {e}')
    return None