from ETL.extract import ExtractClientes,ExtractFornecedores,ExtractItensDeNota,ExtractItensDePedido,ExtractNotasFiscais,ExtractParcelas,ExtractPedidos,ExtractProdutos
from entities.dimensional import dm_clientes,dm_fornecedores,dm_tempo,dm_tipos_vendas,ft_impontualidade,ft_vendas,dm_produtos
import timeit



def TransformaClientes(engineDM):
    listaDW = []
    print("transformação de Clientes")
    start = timeit.default_timer()
    
    lista_op =  ExtractClientes(engineDM)
        
    for i in lista_op:
       listaDW.append(dm_clientes.DMClientes(i.cod_cli, i.nom_cli, 'ARACAJU', 'SE'))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW


def TransformaFornecedores(engineDM):
    listaDW = []
    print("transformação de Fornecedores")
    start = timeit.default_timer()
    
    lista_op =  ExtractFornecedores(engineDM)
        
    for i in lista_op:
       listaDW.append(dm_fornecedores.DMFornecedores(i.cod_forn, i.nom_forn, defineRegiao(lista_op)))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW

def defineRegiao(lista):
  regiao = ''
  NORDESTE = ("SE", "BA", "AL", "MA", "CE", "PI", "PB", 'PE', "RN")
  NORTE = ("AM", "PA", "AC", "TO", "RR", "RO", "AP")
  CENTRO = ("DF", "GO", "MS", "MT")
  SUDESTE = ("RJ", "SP", "ES", "MG")
  SUL = ("RS", "SC", "PR")
  for i in lista:
    if i.uf_forn in NORTE:
      regiao = "NORTE"
    elif i.uf_forn in SUL:
      regiao = "SUL"
    elif i.uf_forn in SUDESTE:
      regiao = "SUDESTE"
    elif i.uf_forn in NORDESTE:
      regiao = "NORDESTE"
    elif i.uf_forn in CENTRO:
      regiao = "CENTRO"
  return regiao

def TransformaProdutos(engineDM):
    listaDW = []
    print("transformação de Produtos")
    start = timeit.default_timer()
    
    lista_op =  ExtractProdutos(engineDM)
        
    for i in lista_op:
       listaDW.append(dm_produtos.DMProdutos(i.cod_prod, i.dsc_prod, i.preco_pro))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW


def TransformaTiposVendas(engineDM):
    listaDW = []
    id_tipo_venda = 0
    print("transformação de TiposVendas")
    start = timeit.default_timer()
    
    lista_op =  ExtractNotasFiscais(engineDM)
        
    for i in lista_op:
      id_tipo_venda += 1
      listaDW.append(dm_tipos_vendas.DMTiposVendas(id_tipo_venda, i.sta_nota))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW


def TransformaTempo(engineDM):
    listaDW = []
    id_tempo = 0
    print("transformação de Tempo")
    start = timeit.default_timer()
    
    lista_op =  ExtractParcelas(engineDM)
        
    for i in lista_op:
      id_tempo += 1
      listaDW.append(dm_tempo.DMTempo(id_tempo, i.dat_venc.strftime("%Y"),
                                    i.dat_venc.strftime("%m"), i.dat_venc.strftime("%Y%d"),
                                    i.dat_venc.strftime("%b"), i.dat_venc.strftime("%b%Y"),
                                    i.dat_venc.strftime("%B"), i.dat_venc.strftime("%d")))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW


def TransformaFTImpontualidade(engineDM):
    listaDW = []
    id_tempo = 0
    print("transformação de FTImpontualidade")
    start = timeit.default_timer()
    
    lista_cli =  ExtractClientes(engineDM)
    lista_par = ExtractParcelas(engineDM)
    
    for i in lista_cli:
      for j in lista_par:
        id_tempo += 1
        listaDW.append(ft_impontualidade.FTImpotualidade(id_tempo, i.cod_cli,
                                      calcParcAtrasadas(lista_par), calcParcTotla(lista_par)))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW


def calcParcAtrasadas(parcelas):
  total = 0
  for i in parcelas:
    if i.parc_paga != "V":
      total += i.val_parc
  return total

def calcParcTotla(parcelas):
  total = 0
  for i in parcelas:
    total += i.val_parc

  return total


def TransformaFTVendas(engineDM):
    listaDW = []
    id_tempo = 0
    print("transformação de FTVendas")
    start = timeit.default_timer()
    
    lista_not =  ExtractNotasFiscais(engineDM)
    lista_itns = ExtractItensDeNota(engineDM)
    
    # for i in lista_itns:
    for j in lista_not:
      id_tempo += 1
      listaDW.append(ft_vendas.FTVendas(id_tempo, id_tempo, id_tempo, j.cod_forn, j.val_total))

    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")
    
    return listaDW