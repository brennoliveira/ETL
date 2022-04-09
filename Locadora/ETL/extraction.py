import timeit
import sqlalchemy as sa
from entities.operational import Artistas, Copias, Gravadoras, ItensLocacao, Locacoes, Socios, Titulos, TiposSocios, Tempo

def ExtractArtista(engine, table):
    artistas = []
    count = 0
    print("Iniciando extração de Artista")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        artistas.append(Artistas.Artistas(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return artistas

def ExtractCopias(engine, table):
  copias = []
  count = 0
  print("Iniciando extração de Copias")
  start = timeit.default_timer()

  stmt = sa.select([table])
  result = engine.execute(stmt).fetchall()
        
  for value in result:
      copias.append(Copias.Copias(**value))
      count += 1
  

  end = timeit.default_timer()
  exec_time =  (end - start)
  print(f'itens exraidos: {count}')
  print(f"Tempo de execução: {exec_time:.2f}s")
  
  return copias


def ExtractGravadoras(engine, table):
    gravadoras = []
    count = 0
    print("Iniciando extração de Gravadoras")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        gravadoras.append(Gravadoras.Gravadoras(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return gravadoras

  
def ExtractItensLocacao(engine, table):
    itens_locacao = []
    count = 0
    print("Iniciando extração de ItensLocacao")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        itens_locacao.append(ItensLocacao.ItensLocacoes(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return itens_locacao


def ExtractLocacoes(engine, table):
    locacoes = []
    count = 0
    print("Iniciando extração de Locacoes")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        locacoes.append(Locacoes.Locacoes(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return locacoes


def ExtractSocios(engine, table):
    socios = []
    count = 0
    print("Iniciando extração de Socios")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        socios.append(Socios.Socios(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return socios


def ExtractTiposSocios(engine, table):
    tipos_socios = []
    count = 0
    print("Iniciando extração de TiposSocios")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        tipos_socios.append(TiposSocios.TiposSocios(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return tipos_socios

def ExtractTitulos(engine, table):
    titulos = []
    count = 0
    print("Iniciando extração de Titulos")
    start = timeit.default_timer()

    stmt = sa.select([table])
    result = engine.execute(stmt).fetchall()
            
    for value in result:
        titulos.append(Titulos.Titulos(**value))
        count += 1
    

    end = timeit.default_timer()
    exec_time =  (end - start)
    print(f'itens exraidos: {count}')
    print(f"Tempo de execução: {exec_time:.2f}s")
    
    return titulos


# def ExtractTempo(engine, table):
#     tempo = []
#     count = 0
#     print("Iniciando extração de Tempo")
#     start = timeit.default_timer()

#     stmt = sa.select([table])
#     result = engine.execute(stmt).fetchall()
            
#     for value in result:
#         tempo.append(Tempo.Tempo(value[1]))
#         count += 1
    

#     end = timeit.default_timer()
#     exec_time =  (end - start)
#     print(f'itens exraidos: {count}')
#     print(f"Tempo de execução: {exec_time:.2f}s")
    
#     return Tempo