import time
from pymongo import MongoClient
from textblob import TextBlob
# Não precisamos mais do 'nltk' ou 'vadersentiment' diretamente

# --- 1. CONFIGURAÇÃO DA ANÁLISE DE SENTIMENTO (TextBlob) ---
print("Carregando o analisador de sentimento (TextBlob-pt)...")
# O TextBlob é mais simples, não há inicialização complexa.
print("Analisador carregado com sucesso!")


# --- 2. CONFIGURAÇÃO DO MONGODB ---
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string de conexão
db = client["projeto_poesia_db"]
collection = db["poems"]


# --- 3. DEFINIÇÃO DA CONSULTA ---
query = {"sentiment_analysis.primary_sentiment": None}
poemas_para_analisar = collection.find(query)
total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("Nenhum poema novo para analisar. O script de enriquecimento já foi concluído.")
else:
    print(f"Encontrados {total_para_analisar} poemas para analisar.")
    print("Iniciando a análise dos 100 primeiros...")


# --- 4. O LOOP DE ENRIQUECIMENTO ---
start_time = time.time()
count = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    try:
        # 4a. A Análise de IA (TextBlob)
        # 1. Criamos um objeto TextBlob com o texto
        analysis = TextBlob(poem_text)
        
        # 2. Acessamos a propriedade .sentiment
        #    Isso retorna: Sentiment(polarity=-0.1, subjectivity=0.4)
        #    A polaridade vai de -1.0 (negativo) a +1.0 (positivo)
        sentimento_score = analysis.sentiment.polarity

        # 4b. Prepara a atualização para o MongoDB
        # Usamos a mesma lógica de antes
        if sentimento_score >= 0.05:
            sentimento_label = "POSITIVE"
        elif sentimento_score <= -0.05:
            sentimento_label = "NEGATIVE"
        else:
            sentimento_label = "NEUTRAL"

        update_data = {
            "$set": {
                "sentiment_analysis.primary_sentiment": sentimento_label,
                "sentiment_analysis.score": sentimento_score,
                "recommendation_tags.good_for_feeling": [sentimento_label.lower()]
            }
        }
        
        # 4c. Atualiza o documento no banco
        collection.update_one({"_id": poem_id}, update_data)
        
        print(f"  > Poema '{poem['title']}' analisado: {sentimento_label} ({sentimento_score:.4f})")
        count += 1

    except Exception as e:
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")

# --- 5. RESULTADOS ---
end_time = time.time()
print("\n--- Análise de Teste Concluída! ---")
if count > 0:
    print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
else:
    print("Nenhum poema foi processado nesta execução.")

client.close()