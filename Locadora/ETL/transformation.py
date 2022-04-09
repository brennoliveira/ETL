from datetime import date, datetime
import timeit
from ETL.extraction import ExtractArtista,ExtractCopias,ExtractGravadoras,ExtractItensLocacao,ExtractLocacoes,ExtractTiposSocios,ExtractSocios,ExtractTitulos
from entities.dimensional import dmArtista,dmGravadora,dmSocio,dmTempo,dmTitulo,ftLocacoes
from connection.connect_db import connect_db



def TransformArtistas(engine, table):
  dw = []
  print("Iniciando transformação de Artista")
  start = timeit.default_timer()
  artistas = ExtractArtista(engine, table)

  for a in artistas:
    dw.append(dmArtista.DM_Artista(a.cod_art, a.tpo_art, a.nac_bras, a.nom_art))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw


def TransformGravadoras(engine, table):
  dw = []
  print("Iniciando transformação de Gravadora")
  start = timeit.default_timer()
  gravadoras = ExtractGravadoras(engine, table)

  for a in gravadoras:
    dw.append(dmGravadora.DM_Gravadora(a.cod_grav,a.uf_grav,a.nac_bras,a.nom_grav))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw


def TransformSocios(engine, table):
  dw = []
  print("Iniciando transformação de Socios")
  start = timeit.default_timer()
  Socios = ExtractSocios(engine, table)

  for a in Socios:
    dw.append(dmSocio.DM_Socio(a.cod_soc,a.nom_soc,a.cod_tps))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw


def TransformTitulos(engine, table):
  dw = []
  print("Iniciando transformação de Titulos")
  start = timeit.default_timer()
  Titulos = ExtractTitulos(engine, table)

  for a in Titulos:
    dw.append(dmTitulo.DM_Titulo(a.cod_tit,a.tpo_tit,a.cla_tit,a.dsc_tit))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw


def TransformTempo(engine, table):
  dw = []
  id_tempo = 0
  print("Iniciando transformação de Tempo")
  start = timeit.default_timer()
  Tempo = ExtractSocios(engine, table)

# %Y - 2022 (Year)
# %m - 04 (month)
# %b - Apr (month)
# %B - April (month)
# %d - 07 (day)
# %H - 16 (24 hour format)
# %p - PM (period)

  for a in Tempo:
    id_tempo += 1
    dw.append(dmTempo.DM_Tempo(id_tempo, a.dat_cad.strftime("%Y"), a.dat_cad.strftime("%m"), 
                          a.dat_cad.strftime("%Y%m"), a.dat_cad.strftime("%b"),
                          a.dat_cad.strftime("%b%Y"), a.dat_cad.strftime("%B"),
                          a.dat_cad.strftime("%d"), datetime.now(),
                          a.dat_cad.strftime("%H"), a.dat_cad.strftime("%p")))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw

def TransformFTLocacoes(engine, table, table1):
  dw = []
  id_tempo, valor_arrecadado = 0, 0
  print("Iniciando transformação de FTLocacoes")
  start = timeit.default_timer()
  locs = ExtractLocacoes(engine, table)
  titulos = ExtractTitulos(engine, table1)

  for loc in locs:
    valor_arrecadado += loc.val_loc
    for item in titulos:
      id_tempo +=1
      dw.append(ftLocacoes.FT_Locacoes(loc.cod_soc, item.cod_tit,
                item.cod_art, item.cod_grav,
                id_tempo, valor_arrecadado, int(tempo_dev(loc)), multa(loc)))

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw

def tempo_dev(locacao):
  result = 0
  if locacao.sta_pgto != "P":

    result = (int(locacao.dat_venc.strftime("%m")) * 30) + int(locacao.dat_venc.strftime("%d"))
    return result

  return result


def multa(locacao):
  valor = 0
  atraso = int(locacao.dat_venc.strftime("%d"))
  if atraso > 1:
    valor += atraso * 0.4
    return valor
  return valor