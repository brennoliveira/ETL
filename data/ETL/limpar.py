from sqlalchemy import text

def LimparBase(engine, dimensional):
  try:
    for table in dimensional:
      engine.execute(text(f'DELETE {table}'))
    print("Sucesso!")
    
  except Exception as e:
    print(f"erro: {e}")
