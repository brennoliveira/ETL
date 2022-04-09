class Fornecedores():
  def to_string(self):
    return {
    "uf": self.uf_forn
  }
  def __init__(self, cod_forn, uf_forn, sld_credor, nom_forn) -> None:
      self.cod_forn = cod_forn
      self.uf_forn = uf_forn
      self.sld_credor = sld_credor
      self.nom_forn = nom_forn