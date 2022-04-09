import timeit
from ETL.transformation import TransformArtistas,TransformFTLocacoes,TransformGravadoras,TransformSocios,TransformTempo,TransformTitulos

def LoadArtista(engine, table_OP, table_DM):
  print("Iniciando processo de Carregamento dos Artistas")
  start = timeit.default_timer()
  listadw = TransformArtistas(engine, table_OP)

  for item in listadw:
    a = table_DM.insert().values(id_art=item.id_art, tpo_art=item.tpo_art, nac_bras=item.nac_bras, nom_art=item.nom_art)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")


def LoadGravadora(engine, table_OP, table_DM):
  print("Iniciando processo de Carregamento de Gravadora")
  start = timeit.default_timer()
  listadw = TransformGravadoras(engine, table_OP)

  for item in listadw:
    a = table_DM.insert().values(id_grav=item.id_grav, uf_grav=item.uf_grav, nac_bras=item.nac_bras, nom_grav=item.nom_grav)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")

  
def LoadSocio(engine, table_OP, table_DM):
  print("Iniciando processo de Carregamento de Gravadora")
  start = timeit.default_timer()
  listadw = TransformSocios(engine, table_OP)

  for item in listadw:
    a = table_DM.insert().values(id_soc=item.id_soc, nom_soc=item.nom_soc, tipo_socio=item.tipo_socio)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")


def LoadTitulo(engine, table_OP, table_DM):
  print("Iniciando processo de Carregamento de Titulo")
  start = timeit.default_timer()
  listadw = TransformTitulos(engine, table_OP)

  for item in listadw:
    a = table_DM.insert().values(id_titulo=item.id_titulo, tpo_titulo=item.tpo_titulo, cla_titulo=item.cla_titulo, dsc_titulo=item.dsc_titulo)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")


def LoadTempo(engine, table_OP, table_DM):
  print("Iniciando processo de Carregamento de Tempo")
  start = timeit.default_timer()
  listadw = TransformTempo(engine, table_OP)

  for item in listadw:
    a = table_DM.insert().values(id_tempo=item.id_tempo, nu_ano=item.nu_ano, nu_mes=item.nu_mes,
                                nu_anomes=item.nu_anomes, sg_mes=item.sg_mes, nm_mesano=item.nm_mesano,
                                nm_mes=item.nm_mes, nu_dia=item.nu_dia, dt_tempo=item.dt_tempo,
                                nu_hora=item.nu_hora, turno=item.turno)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")


def LoadFTLocacoes(engine, table_OP1, tableOP_2, table_DM):
  print("Iniciando processo de Carregamento de Titulo")
  start = timeit.default_timer()
  listadw = TransformFTLocacoes(engine, table_OP1, tableOP_2)

  for item in listadw:
    a = table_DM.insert().values(id_soc=item.id_soc, id_titulo=item.id_titulo,
                                id_art=item.id_art, id_grav=item.id_grav,
                                id_tempo=item.id_tempo, valor_arrecadado=item.valor_arrecadado,
                                tempo_devolucao=item.tempo_devolucao, multa_atraso=item.multa_atraso)
    engine.execute(a)
              
  end = timeit.default_timer()
  exec_time = end - start
  print(f"empo: {exec_time:.2f}s")