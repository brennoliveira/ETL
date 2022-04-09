from ETL.transform import TransformaClientes,TransformaFornecedores,TransformaFTImpontualidade,TransformaFTVendas,TransformaProdutos,TransformaTempo,TransformaTiposVendas
from ETL.limpar import LimparBase
import timeit
from connection.connect import engine, metadata
import sqlalchemy as sa

dm_clientes = sa.Table('DM_CLIENTES', metadata, autoload=True, autoload_with=engine)
dm_tempo = sa.Table('DM_TEMPO', metadata, autoload=True, autoload_with=engine)
dm_fornecedores = sa.Table('DM_FORNECEDORES', metadata, autoload=True, autoload_with=engine)
dm_produtos = sa.Table('DM_PRODUTOS', metadata, autoload=True, autoload_with=engine)
dm_tipos_vendas = sa.Table('DM_TIPOS_VENDAS', metadata, autoload=True, autoload_with=engine)
ft_vendas = sa.Table('FT_VENDAS', metadata, autoload=True, autoload_with=engine)
ft_impontualidade = sa.Table('FT_IMPONTUALIDADE', metadata, autoload=True, autoload_with=engine)

def LoadDmCliente():
    LimparBase(engine,dm_clientes)

    print("Iniciando processo de Carregamento dos Clientes")
    start = timeit.default_timer()
    lista = TransformaClientes(engine)
        
    for item in lista :
        ins = dm_clientes.insert().values(id_cliente = item.cod_cli, nome_cliente = item.nom_cli,
                                          cidade_cli = item.cidade_cli, uf_cli = item.uf_cli)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")


def LoadDmTempo():
    LimparBase(engine,dm_tempo)

    print("Iniciando processoTempo de Carregamento dos Tempo")
    start = timeit.default_timer()
    lista = TransformaTempo(engine)
        
    for item in lista :
        ins = dm_tempo.insert().values(id_tempo = item.id_tempo, nu_ano = item.nu_ano,
                                          nu_mes = item.nu_mes, nu_anomes = item.nu_anomes,
                                          sg_mes=item.sg_mes, nm_mesano=item.nm_mesano,
                                          nm_mes=item.nm_mes, nu_dia= item.nu_dia)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")


def LoadDmFornecedores():
    LimparBase(engine,dm_fornecedores)

    print("Iniciando processoTempo de Carregamento dos Fornecedores")
    start = timeit.default_timer()
    lista = TransformaFornecedores(engine)
        
    for item in lista :
        ins = dm_fornecedores.insert().values(id_forn = item.cod_forn, nom_forn = item.nom_forn,
                                          regiao_forn = item.regiao_forn)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")


def LoadDmProdutos():
    LimparBase(engine,dm_produtos)

    print("Iniciando processoTempo de Carregamento dos Produtos")
    start = timeit.default_timer()
    lista = TransformaProdutos(engine)
        
    for item in lista :
        ins = dm_produtos.insert().values(id_prod = item.id_prod, dsc_prod = item.dsc_prod,
                                          classe_prod = item.classe_prod)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")


def LoadDmTiposVendas():
    LimparBase(engine,dm_tipos_vendas)

    print("Iniciando processoTempo de Carregamento dos TiposVendas")
    start = timeit.default_timer()
    lista = TransformaTiposVendas(engine)
        
    for item in lista :
        ins = dm_tipos_vendas.insert().values(id_tipo_venda = item.id_tipo_venda, desc_tipo_venda = item.desc_tipo_venda)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")


def LoadFTVendas():
    LimparBase(engine,ft_vendas)
    print("Iniciando processoTempo de Carregamento dos FTVendas")
    start = timeit.default_timer()
    lista = TransformaFTVendas(engine)
        
    for item in lista :
        ins = ft_vendas.insert().values(id_prod = item.id_prod, id_tempo = item.id_tempo,
                                        id_tipo_venda=item.id_tipo_venda, id_forn=item.id_forn,
                                        valor_venda=item.valor_venda)
        engine.execute(ins)
                
    end = timeit.default_timer()
    exec_time = end - start
    print(f"empo: {exec_time:.2f}s")