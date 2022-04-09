class Clientes():
  def to_string(self):
    return {
      "cod": self.cod_cli,
      "credito": self.lim_credito,
      "devedor": self.sld_devedor,
      "nome": self.nom_cli,
      "fone": self.fones
  }
  def __init__(self,cod_cli, lim_credito, sld_devedor , nom_cli , fones) -> None:
    self.cod_cli = cod_cli
    self.lim_credito = lim_credito
    self.sld_devedor = sld_devedor
    self.nom_cli = nom_cli
    self.fones = fones
