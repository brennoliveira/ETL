class Parcelas():

  def to_string(self):
    return {
      "val": self.val_parc,
      "pago": self.parc_paga
    }
  def __init__(self,num_ped, dat_venc, val_parc , parc_paga ) -> None:
    self.num_ped = num_ped
    self.dat_venc = dat_venc
    self.val_parc = val_parc
    self.parc_paga = parc_paga
