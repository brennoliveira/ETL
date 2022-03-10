import df_read as dfr

link = 'postgresql://brenno:123456789@209.209.40.88:19073/folhaDB'

sql = 'SELECT * FROM folha.folhas_pagamentos'

# df = dfr.read_df_1(link, sql)


dfr.read_df_3(sql)