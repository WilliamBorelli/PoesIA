import pandas as pd
from pymongo import MongoClient
import time

# --- 1. CONFIGURAÇÃO DA CONEXÃO ---
# Conecta ao servidor MongoDB (assume que está rodando localmente)
client = MongoClient("mongodb://localhost:27017/")

# Seleciona (ou cria) seu banco de dados
db = client["projeto_poesia_db"]

# Seleciona (ou cria) sua coleção de poemas
collection = db["poems"]

# Limpa a coleção para evitar duplicatas se rodarmos o script várias vezes
# Comente esta linha se quiser adicionar a um banco já existente
collection.delete_many({})
print("Coleção 'poems' limpa.")

# --- 2. LEITURA DO ARQUIVO CSV ---
csv_file_path = "portuguese-poems.csv"  # <-- nome do  arquivo
batch_size = 1000  # Número de poemas para inserir de uma vez
poemas_para_inserir = []
total_inseridos = 0

try:
    print(f"Iniciando a leitura de '{csv_file_path}'...")
    # Usamos 'chunksize' para ler o CSV em pedaços (lotes)
    # Isso economiza memória e é eficiente para arquivos grandes
    for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
        
        start_batch_time = time.time()
        
        for index, row in chunk.iterrows():
            # --- 3. TRANSFORMAÇÃO DO DADO ---
            # Aqui está a mágica: mapeamos as colunas do CSV
            # para o nosso esquema de documento JSON que projetamos.
            
            poema_documento = {
                "title": row["Title"],
                "author": row["Author"],
                "full_text": row["Content"],
                
                # Criamos os campos do nosso modelo, mesmo que vazios
                "sentiment_analysis": {
                    "primary_sentiment": None, # Será preenchido depois
                    "secondary_sentiment": None,
                    "score": None,
                    "keywords": []
                },
                
                "recommendation_tags": {
                    "evokes": [],             # Será preenchido depois
                    "good_for_feeling": [], # Será preenchido depois
                    "intensity": "media"      # Um valor padrão
                },
                
                "metadata": {
                    "views_csv": int(row["Views"]), # Pegamos as views do CSV
                    "times_recommended": 0,
                    "average_rating": 0
                }
            }
            
            poemas_para_inserir.append(poema_documento)

        # --- 4. INSERÇÃO EM LOTE ---
        if poemas_para_inserir:
            collection.insert_many(poemas_para_inserir)
            total_inseridos += len(poemas_para_inserir)
            
            end_batch_time = time.time()
            print(f"  > Lote de {len(poemas_para_inserir)} poemas inserido. "
                  f"Total: {total_inseridos} / 15000. "
                  f"Tempo: {end_batch_time - start_batch_time:.2f}s")
            
            poemas_para_inserir = [] # Limpa a lista para o próximo lote

    print("\n--- Processo Concluído! ---")
    print(f"Total de {total_inseridos} poemas importados para o banco 'projeto_poesia_db', coleção 'poems'.")

except FileNotFoundError:
    print(f"ERRO: Arquivo '{csv_file_path}' não encontrado.")
    print("Por favor, verifique se o nome do arquivo está correto e no mesmo diretório do script.")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")

finally:
    client.close() # Sempre feche a conexão