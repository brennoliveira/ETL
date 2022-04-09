import sqlalchemy as sa
from connection.connect_db import connect_db
from ETL.extraction import ExtractArtista, ExtractCopias, ExtractGravadoras, ExtractItensLocacao, ExtractLocacoes, ExtractSocios, ExtractTiposSocios, ExtractTitulos 
from ETL.transformation import TransformArtistas,TransformFTLocacoes,TransformGravadoras,TransformSocios,TransformTitulos,TransformTempo
from ETL.load import LoadArtista, LoadGravadora, LoadSocio, LoadTitulo,LoadTempo,LoadFTLocacoes
from ETL.limpar import LimparBase

engine = connect_db()
# print(engine.table_names())
metadata = sa.MetaData(bind=None)

operacional, dimensional = [], []
table_artistas = sa.Table('ARTISTAS', metadata, autoload=True, autoload_with=engine)
table_copias = sa.Table('COPIAS', metadata, autoload=True, autoload_with=engine)
table_gravadoras = sa.Table('GRAVADORAS', metadata, autoload=True, autoload_with=engine)
table_itensLocacoes = sa.Table('ITENS_LOCACOES', metadata, autoload=True, autoload_with=engine)
table_locacoes = sa.Table('LOCACOES', metadata, autoload=True, autoload_with=engine)
table_socios = sa.Table('SOCIOS', metadata, autoload=True, autoload_with=engine)
table_tipoSocios = sa.Table('TIPOS_SOCIOS', metadata, autoload=True, autoload_with=engine)
table_titulos = sa.Table('TITULOS', metadata, autoload=True, autoload_with=engine)
# table_tempo = sa.Table('TEMPO', metadata, autoload=False, autoload_with=engine)


dm_artista = sa.Table('DM_ARTISTA', metadata, autoload=True, autoload_with=engine)
dm_gravadora = sa.Table('DM_GRAVADORA', metadata, autoload=True, autoload_with=engine)
dm_socio = sa.Table('DM_SOCIO', metadata, autoload=True, autoload_with=engine)
dm_tempo = sa.Table('DM_TEMPO', metadata, autoload=True, autoload_with=engine)
dm_titulo = sa.Table('DM_TITULO', metadata, autoload=True, autoload_with=engine)
ft_locacoes = sa.Table('FT_LOCACOES', metadata, autoload=True, autoload_with=engine)



# atistas = ExtractArtista(engine, table_artistas)
# copias = ExtractCopias(engine, table_copias)
# gravadoras = ExtractGravadoras(engine, table_gravadoras)
# itensLocacoes = ExtractItensLocacao(engine, table_itensLocacoes)
# locacoes = ExtractLocacoes(engine, table_locacoes)
# socios = ExtractSocios(engine, table_socios)
# tiposSocios = ExtractTiposSocios(engine, table_tipoSocios)
# titulos = ExtractTitulos(engine, table_titulos)
# temp = ExtractTempo(engine, table_locacoes)

# atistas = TransformArtistas(engine, table_artistas)
# gravadoras = TransformGravadoras(engine, table_gravadoras)
# socios = TransformSocios(engine, table_socios)
# titulos = TransformTitulos(engine, table_titulos)
# tempo = TransformTempo(engine, table_socios)
# fatos = TransformFTLocacoes(engine, table_locacoes, table_titulos)

LimparBase(engine, ft_locacoes)
LimparBase(engine, dm_artista)
LimparBase(engine, dm_gravadora)
LimparBase(engine, dm_socio)
LimparBase(engine, dm_titulo)
LimparBase(engine, dm_tempo)

LoadArtista(engine, table_artistas, dm_artista)
LoadGravadora(engine, table_gravadoras, dm_gravadora)
LoadSocio(engine, table_socios, dm_socio)
LoadTitulo(engine, table_titulos, dm_titulo)
LoadTempo(engine, table_socios, dm_tempo)
LoadFTLocacoes(engine, table_locacoes, table_titulos, ft_locacoes)



