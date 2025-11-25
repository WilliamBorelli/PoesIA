import time
from pymongo import MongoClient
from textblob import TextBlob
# Não precisamos mais do 'nltk' ou 'vadersentiment' diretamente

# --- 1. CONFIGURAÇÃO DA ANÁLISE DE SENTIMENTO (TextBlob) ---
print("Carregando o analisador de sentimento (TextBlob-pt)...")
print("Analisador carregado com sucesso!")


# --- 2. CONFIGURAÇÃO DO MONGODB ---
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string de conexão
db = client["projeto_poesia_db"]
collection = db["poems"]


# --- 3. FUNÇÃO AUXILIAR PARA SUBJETIVIDADE ---
def traduzir_subjetividade(score):
    """Converte o score numérico de subjetividade em um label descritivo."""
    if score > 0.66:
        return "Subjetivo" # Muito emocional / opinativo
    elif score > 0.33:
        return "Reflexivo" # Um balanço entre fato e opinião
    else:
        return "Objetivo"  # Muito factual / descritivo

# --- 4. DEFINIÇÃO DA CONSULTA ---
# ==========================================================
# MUDANÇA IMPORTANTE NA QUERY:
# Agora, procuramos por poemas onde o campo 'secondary_sentiment'
# AINDA NÃO EXISTE. Isso permite rodar o script em cima
# dos dados que já analisamos, sem reprocessar tudo.
# ==========================================================
query = {"sentiment_analysis.secondary_sentiment": None}

poemas_para_analisar = collection.find(query) # <--- REMOVA O .limit() para rodar em tudo

total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("Nenhum poema novo para analisar. O campo 'secondary_sentiment' já foi preenchido.")
else:
    print(f"Encontrados {total_para_analisar} poemas para preencher o 'secondary_sentiment'.")
    print("Iniciando o processamento...")


# --- 5. O LOOP DE ENRIQUECIMENTO ---
start_time = time.time()
count = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    try:
        # 5a. A Análise de IA (TextBlob)
        analysis = TextBlob(poem_text)
        
        # 5b. Pegamos os DOIS scores
        sentimento_score = analysis.sentiment.polarity
        subjectivity_score = analysis.sentiment.subjectivity # <-- NOVO
        
        # 5c. Traduzimos os DOIS scores
        if sentimento_score >= 0.05:
            sentimento_label = "POSITIVE"
        elif sentimento_score <= -0.05:
            sentimento_label = "NEGATIVE"
        else:
            sentimento_label = "NEUTRAL"
        
        secondary_label = traduzir_subjetividade(subjectivity_score) # <-- NOVO

        # 5d. Prepara a atualização para o MongoDB
        # Usamos $set para ATUALIZAR os campos.
        # Isso vai adicionar os campos novos sem apagar os antigos.
        update_data = {
            "$set": {
                "sentiment_analysis.primary_sentiment": sentimento_label,
                "sentiment_analysis.score": sentimento_score,
                "sentiment_analysis.secondary_sentiment": secondary_label, # <-- NOVO
                "sentiment_analysis.subjectivity_score": subjectivity_score, # <-- NOVO (Bônus)
                "recommendation_tags.good_for_feeling": [sentimento_label.lower()]
            }
        }
        
        # 5e. Atualiza o documento no banco
        collection.update_one({"_id": poem_id}, update_data)
        
        if (count + 1) % 100 == 0: # Imprime um status a cada 100 poemas
             print(f"  > Processados {count+1} / {total_para_analisar} poemas...")
        count += 1

    except Exception as e:
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")

# --- 6. RESULTADOS ---
end_time = time.time()
print("\n--- Análise de Subjetividade Concluída! ---")
if count > 0:
    print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
else:
    print("Nenhum poema foi processado nesta execução.")

client.close()