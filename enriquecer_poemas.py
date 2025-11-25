import time
from pymongo import MongoClient
from transformers import pipeline

# --- 1. CONFIGURAÇÃO DA ANÁLISE DE SENTIMENTO ---
print("Carregando o modelo de IA... (Pode demorar um pouco na primeira vez)")
# Este é um modelo de IA treinado especificamente para sentimento em Português.
# Ele classifica textos como "positivo", "negativo" ou "neutro".
# A biblioteca 'transformers' cuida de todo o download e setup.
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="pysentimiento/bertweet-pt-sentiment"
)
print("Modelo carregado com sucesso!")


# --- 2. CONFIGURAÇÃO DO MONGODB ---
# Use a MESMA string de conexão e nomes do script anterior!
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string de conexão
db = client["projeto_poesia_db"]
collection = db["poems"]


# --- 3. DEFINIÇÃO DA CONSULTA ---
# Esta é a parte importante:
# Vamos buscar apenas documentos onde o campo 'primary_sentiment'
# ainda esteja com o valor 'None' (que definimos na importação).
query = {"sentiment_analysis.primary_sentiment": None}

# Vamos limitar a 10 poemas para este teste.
# Depois de confirmar que funciona, você pode remover o .limit(10)
# para rodar em todos os 15.000 (mas vá com calma!)
poemas_para_analisar = collection.find(query)

total_para_analisar = collection.count_documents(query)
print(f"Encontrados {total_para_analisar} poemas para analisar.")
print("Iniciando a análise dos 10 primeiros...")


# --- 4. O LOOP DE ENRIQUECIMENTO ---
start_time = time.time()
count = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    try:
        # ** IMPORTANTE: LIMITAÇÃO DE TAMANHO **
        # Modelos de IA têm um limite de quantos caracteres/palavras
        # eles conseguem ler de uma vez (geralmente 512 "tokens").
        # Para evitar erros, vamos analisar apenas os primeiros 500 caracteres.
        # (Discutiremos isso melhor abaixo)
        texto_para_analise = poem_text[:500]

        # 4a. A Análise de IA
        # O resultado será algo como: [{'label': 'positive', 'score': 0.98}]
        resultado = sentiment_analyzer(texto_para_analise)[0]
        
        # 4b. Prepara a atualização para o MongoDB
        sentimento_label = resultado["label"].upper() # ex: "POSITIVE"
        sentimento_score = round(resultado["score"], 4) # ex: 0.9812

        update_data = {
            "$set": {
                "sentiment_analysis.primary_sentiment": sentimento_label,
                "sentiment_analysis.score": sentimento_score,
                # Podemos usar o label como uma tag inicial
                "recommendation_tags.good_for_feeling": [sentimento_label.lower()]
            }
        }
        
        # 4c. Atualiza o documento no banco
        collection.update_one({"_id": poem_id}, update_data)
        
        print(f"  > Poema '{poem['title']}' analisado: {sentimento_label} ({sentimento_score})")
        count += 1

    except Exception as e:
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")

# --- 5. RESULTADOS ---
end_time = time.time()
print("\n--- Análise de Teste Concluída! ---")
print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
print("Verifique seu MongoDB Compass para ver os campos atualizados!")

client.close()