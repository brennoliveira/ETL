import df_read as dfr

link = 'postgresql://brenno:123456789@209.209.40.88:19073/folhaDB'

sql = '''SELECT * FROM folha.colaboradores;'''

# sql = '''DELETE from folha.colaboradores WHERE cod_colab=3837;
# SELECT * FROM folha.colaboradores;'''

# sql = '''DELETE from folhadw.dm_faixas_etarias CASCADE;
#     DELETE FROM folhadw.dm_rubricas CASCADE;
#     DELETE FROM folhadw.dm_setores CASCADE;
#     DELETE FROM folhadw.dm_tempos_folhas CASCADE;
#     DELETE FROM folhadw.dm_tempos_servicos CASCADE;
#     DELETE FROM folhadw.ft_lancamentos CASCADE;
#     DELETE FROM folhadw.dm_cargos CASCADE;
#     '''

# df = dfr.read_df_1(link, sql)


dfr.read_df_3(sql)