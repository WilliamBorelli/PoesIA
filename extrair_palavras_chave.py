import time
from pymongo import MongoClient
import spacy
from collections import Counter # Usaremos isso para contar as palavras

# --- 1. CONFIGURAÇÃO DO spaCy ---
print("Carregando o modelo de PLN (spaCy)...")
# Carrega o modelo de português 'médio' que acabamos de baixar
nlp = spacy.load("pt_core_news_md")
print("Modelo carregado com sucesso!")


# --- 2. CONFIGURAÇÃO DO MONGODB ---
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string
db = client["projeto_poesia_db"]
collection = db["poems"]


# --- 3. DEFINIÇÃO DA CONSULTA ---
# Desta vez, vamos procurar poemas onde a análise de sentimento FOI feita,
# mas o campo 'keywords' ainda é um array vazio [].
query = {"sentiment_analysis.primary_sentiment": {"$ne": None},
         "sentiment_analysis.keywords": []} # <-- Apenas onde 'keywords' está vazio

# Vamos limitar a 10 poemas para este teste.
poemas_para_analisar = collection.find(query)

total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("Nenhum poema novo para extrair palavras-chave.")
else:
    print(f"Encontrados {total_para_analisar} poemas para analisar.")
    print("Iniciando a extração das 10 primeiras...")


# --- 4. O LOOP DE ENRIQUECIMENTO ---
start_time = time.time()
count = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    try:
        # 4a. A Análise de PLN (spaCy)
        # Processa o texto completo com o modelo
        doc = nlp(poem_text)
        
        keywords = []
        # Itera em cada "token" (palavra) que o spaCy encontrou
        for token in doc:
            # A MÁGICA:
            # Queremos a palavra se ela NÃO for "stopword" (e, de, que...)
            # E NÃO for pontuação (., !, ?)
            # E for um Substantivo (NOUN), Nome Próprio (PROPN) ou Adjetivo (ADJ)
            if (not token.is_stop and
                not token.is_punct and
                token.pos_ in ["NOUN", "PROPN", "ADJ"]):
                
                # Usamos .lemma_ para pegar a raiz da palavra
                # (ex: "tristes" -> "triste", "poemas" -> "poema")
                keywords.append(token.lemma_.lower())
        
        # 4b. Contar e pegar as 5 mais comuns
        # Counter({'amor': 5, 'tristeza': 3, 'noite': 2, ...})
        keyword_counts = Counter(keywords)
        
        # .most_common(5) pega as 5 mais frequentes
        # (ex: [('amor', 5), ('tristeza', 3), ...])
        # E extraímos apenas a palavra
        top_5_keywords = [word for word, freq in keyword_counts.most_common(5)]

        # 4c. Prepara a atualização para o MongoDB
        update_data = {
            "$set": {
                "sentiment_analysis.keywords": top_5_keywords
            }
        }
        
        # 4d. Atualiza o documento no banco
        collection.update_one({"_id": poem_id}, update_data)
        
        print(f"  > Poema '{poem['title']}' analisado. Keywords: {top_5_keywords}")
        count += 1

    except Exception as e:
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")

# --- 5. RESULTADOS ---
end_time = time.time()
print("\n--- Extração de Teste Concluída! ---")
if count > 0:
    print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
else:
    print("Nenhum poema foi processado nesta execução.")

client.close()