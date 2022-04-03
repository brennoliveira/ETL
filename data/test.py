import df_read as dfr
import pandas as pd


sql = '''SELECT * FROM SOCIOS'''


engine = dfr.connect_db()
test_df = pd.read_sql_query(sql, engine)
print(test_df)