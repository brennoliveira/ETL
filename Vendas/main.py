from ETL.extract import ExtractClientes,ExtractFornecedores,ExtractItensDeNota,ExtractItensDePedido,ExtractNotasFiscais,ExtractParcelas,ExtractPedidos,ExtractProdutos
from ETL.transform import TransformaClientes,TransformaFornecedores,TransformaFTImpontualidade,TransformaFTVendas,TransformaProdutos,TransformaTempo,TransformaTiposVendas
from ETL.load import LoadDmCliente,LoadDmTempo,LoadDmFornecedores,LoadDmProdutos,LoadDmTiposVendas,LoadFTVendas
from connection.connect import engine


# ExtractClientes(engine)
# ExtractFornecedores(engine)
# ExtractItensDeNota(engine)
# ExtractItensDePedido(engine)
# ExtractNotasFiscais(engine)
# ExtractParcelas(engine)
# ExtractPedidos(engine)
# ExtractProdutos(engine)

# TransformaClientes(engine)
# TransformaFornecedores(engine)
# TransformaFTImpontualidade(engine)
# TransformaFTImpontualidade(engine)
# TransformaFTVendas(engine)
# TransformaProdutos(engine)
# TransformaTempo(engine)
# TransformaTiposVendas(engine)

LoadDmCliente()
LoadDmTempo()
LoadDmFornecedores()
LoadDmProdutos()
LoadDmTiposVendas()
LoadFTVendas()