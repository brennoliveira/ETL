from sqlalchemy import text

def LimparBase(engine, table):
  try:
    engine.execute(text(f'DELETE FROM {table}'))
    print("Sucesso!")
    
  except Exception as e:
    print(f"erro: {e}")
