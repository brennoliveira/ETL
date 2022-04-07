from datetime import date, datetime
import timeit
from ETL.extraction import ExtractArtista,ExtractCopias,ExtractGravadoras,ExtractItensLocacao,ExtractLocacoes,ExtractTiposSocios,ExtractSocios,ExtractTitulos,ExtractTempo
from entities.dimensional import dmArtista,dmGravadora,dmSocio,dmTempo,dmTitulo,ftLocacoes

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
  print("Iniciando transformação de Artista")
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


# def TransformTempo(engine, table):
#   dw = []
#   id_tempo = 0
#   print("Iniciando transformação de Tempo")
#   start = timeit.default_timer()
#   Tempo = ExtractTempo(engine, table)

# # %Y - 2022 (Year)
# # %m - 04 (month)
# # %b - Apr (month)
# # %B - April (month)
# # %d - 07 (day)
# # %H - 16 (24 hour format)
# # %p - PM (period)

#   for a in Tempo:
#     dw.append(dmTempo.DM_Tempo(id_tempo, int(a.tempo_op.strftime("%Y")), int(a.tempo_op.strftime("%m")), 
#                           int(a.tempo_op.strftime("%Y%m")), a.tempo_op.strftime("%b"),
#                           a.tempo_op.strftime("%b%Y"), a.tempo_op.strftime("%B"),
#                           a.tempo_op.strftime("%d"), ("07/04/2022"),
#                           int(a.tempo_op.strftime("%H")), a.tempo_op.strftime("%p")))
#     id_tempo += 1

#   end = timeit.default_timer()
#   exec_time = (end - start)
#   print(f"tempo de execução: {exec_time:.2f}s")
#   return dw

def TransformarTempo(temp):
    count = 0
    tempdw = []
    print("Iniciando processo de transformação do Tempo")
    start = timeit.default_timer()
    temp 
        
    for i in temp:
       count+=1
       tempdw.append(dmTempo.DM_tempo(count,i.TempLoc.strftime("%Y"),i.TempLoc.strftime("%m"),i.TempLoc.strftime("%Y")+i.TempLoc.strftime("%m"),NomMes(i.TempLoc.strftime("%m"))[0:3],NomMes(i.TempLoc.strftime("%m"))[0:3]+"/"+i.TempLoc.strftime("%Y"),NomMes(i.TempLoc.strftime("%m")),i.TempLoc.strftime("%d"),i.TempLoc,i.TempLoc.strftime("%H"),Turn(i.TempLoc.strftime("%H"))))

    #for a in tempdw:
    #    print(a.turno)

    #for a in tempdw: 
    #  print(a.sg_mes)
        
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação do tempo. "
          f"- Tempo de transformação: {r} segundos")
    
    return tempdw

def Turn(hora):
    turno = "Teste"
    h = int(hora)
    if(h < 12):
        turno = "MANHA"
    elif(h > 12 and h <= 18):
        turno = "TARDE"
    elif(h > 18):
        turno = "NOITE"
    return turno

def NomMes(num):
    nome = "Teste"
    if(num == "01"):
        nome = "Janeiro"
    elif(num == "02"):
        nome = "Fevereiro"
    elif(num == "03"):
        nome = "Março"
    elif(num == "04"):
        nome = "Abril"
    elif(num == "05"):
        nome = "Maio"
    elif(num == "06"):
        nome = "Junho"
    elif(num == "07"):
        nome = "Julho"
    elif(num == "08"):
        nome = "Agosto"
    elif(num == "09"):
        nome = "Setemebro"
    elif(num == "10"):
        nome = "Outubro"
    elif(num == "11"):
        nome = "Novembro"
    elif(num == "12"):
        nome = "Dezembro"
    
    return nome

def TransformFTLocacoes(engine, table, table1):
  dw = []
  id_tempo, valor_arrecadado = 0, 0
  print("Iniciando transformação de FTLocacoes")
  start = timeit.default_timer()
  locs = ExtractLocacoes(engine, table)
  itsLocs = ExtractItensLocacao(engine, table1)

  for loc in locs:
    valor_arrecadado += loc.val_loc
    for item in itsLocs:
      id_tempo +=1
      dw.append(ftLocacoes.FT_Locacoes(loc.cod_soc, item.cod_tit),
                getIdArtista(item.cod_tit), getIdAGravadora(item.cod_tit),
                id_tempo, valor_arrecadado, tempo_dev(loc), )

  end = timeit.default_timer()
  exec_time = (end - start)
  print(f"tempo de execução: {exec_time:.2f}s")
  return dw


def getIdArtista(id):
  artistas = ExtractArtista()
  for artista in artistas:
    if id == artista.cod_art:
      id = artista.cod_art
    return id

def getIdAGravadora(id):
  gravadoras = ExtractGravadoras()
  for gravadora in gravadoras:
    if id == gravadora.cod_grav:
      id = gravadora.cod_grav
    return id

def tempo_dev(locacao):
  result = 0
  if locacao.sta_pgto != "P":
    result = date.today() - locacao.dat_vencstrftime("%Y", "%m", "%d")
    return result

  return result

def multa(locacao):
  valor = 0
  atraso = tempo_dev(locacao)
  if atraso > 1:
    valor += atraso * 0.4
    return multa
  return valor

