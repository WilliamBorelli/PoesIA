import time
from pymongo import MongoClient

# --- CONFIGURAÇÃO ---
client = MongoClient("mongodb://localhost:27017/")
db = client["projeto_poesia_db"]
collection = db["poems"]

# Vamos pegar TODOS os poemas que tenham pelo menos alguma análise feita
query = {} 

total_docs = collection.count_documents(query)
print(f"Iniciando atualização forçada de 'evokes' em {total_docs} poemas...")

start_time = time.time()
count = 0
updated_count = 0

cursor = collection.find(query)

for poem in cursor:
    poem_id = poem["_id"]
    
    # 1. Recupera os dados existentes (com segurança, caso sejam None)
    analysis = poem.get("sentiment_analysis", {}) or {}
    
    # Pega keywords (garante que seja lista)
    keywords = analysis.get("keywords", [])
    if keywords is None: keywords = []
    
    # Pega sentimento secundário (garante que seja lista)
    sec_sentiment = analysis.get("secondary_sentiment", [])
    if sec_sentiment is None: sec_sentiment = []
    # Se por acaso o secondary_sentiment for string (versão antiga), converte pra lista
    if isinstance(sec_sentiment, str):
        sec_sentiment = [sec_sentiment]

    # 2. Cria o conjunto de Evokes
    # Set evita duplicatas (ex: se "amor" estiver nos dois, aparece uma vez só)
    tags_set = set()
    
    # Adiciona tudo e normaliza para minúsculo
    for k in keywords:
        if k: tags_set.add(k.lower())
    for s in sec_sentiment:
        if s: tags_set.add(s.lower())
        
    # Remove tags inúteis
    tags_inuteis = {"indefinido", "não-identificado", "vazio", "subjetivo", "objetivo", "reflexivo"}
    tags_finais = [t for t in tags_set if t not in tags_inuteis]
    
    # 3. Atualiza o banco APENAS se houver tags para salvar
    if tags_finais:
        collection.update_one(
            {"_id": poem_id},
            {"$set": {"recommendation_tags.evokes": tags_finais}}
        )
        updated_count += 1
        
    count += 1
    if count % 1000 == 0:
        print(f"  > Processados {count}/{total_docs}...")

end_time = time.time()
print("\n--- ATUALIZAÇÃO CONCLUÍDA ---")
print(f"Total processado: {count}")
print(f"Total atualizado com tags 'evokes': {updated_count}")
print(f"Tempo: {end_time - start_time:.2f}s")