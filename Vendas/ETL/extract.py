import timeit
import sqlalchemy as sa
import pandas as pd
from entities.operacional import clientes,fornecedores,itens_de_nota,itens_de_pedido,notas_fiscais,parcelas,pedidos,produtos

def ExtractClientes(engine):
  lista = []
  items = 0
  print("extracting Clientes")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM clientes'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(clientes.Clientes(**item))
    items += 1
    
  print(lista[2].to_string())
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractFornecedores(engine):
  lista = []
  items = 0
  print("extracting fornecedores")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM fornecedores'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(fornecedores.Fornecedores(**item))
    items += 1
  print(lista[2].to_string())
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractItensDeNota(engine):
  lista = []
  items = 0
  print("extracting ItensDeNota")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM itens_de_nota'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(itens_de_nota.ItensDeNotas(**item))
    items += 1
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractItensDePedido(engine):
  lista = []
  items = 0
  print("extracting ItensDePedido")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM itens_de_pedido'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(itens_de_pedido.ItensDePedido(**item))
    items += 1
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractNotasFiscais(engine):
  lista = []
  items = 0
  print("extracting NotasFiscais")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM notas_fiscais'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(notas_fiscais.NotasFiscais(**item))
    items += 1
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractParcelas(engine):
  lista = []
  items = 0
  print("extracting Parcelas")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM parcelas'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(parcelas.Parcelas(**item))
    items += 1
    
  print(lista[2].to_string())

  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractPedidos(engine):
  lista = []
  items = 0
  print("extracting Pedidos")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM pedidos'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(pedidos.Pedidos(**item))
    items += 1
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista


def ExtractProdutos(engine):
  lista = []
  items = 0
  print("extracting Produtos")
  start = timeit.default_timer()
  
  sql = 'SELECT * FROM produtos'
  df = pd.read_sql(sql, engine)
  
  
  for _, item in df.iterrows():
    lista.append(produtos.Produtos(**item))
    items += 1
    
  end = timeit.default_timer()
  exec_time = end - start
  print(f"itens: {items} \ntempo: {exec_time:.2f}s")

  return lista