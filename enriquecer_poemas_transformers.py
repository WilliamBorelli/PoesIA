import time
import os
import warnings
from pymongo import MongoClient
from transformers import pipeline
from dotenv import load_dotenv

# --- 1. CONFIGURAÇÕES E INICIALIZAÇÃO ---

# Suprime avisos que não são críticos (o transformers pode ser barulhento)
warnings.filterwarnings("ignore")

# Carrega as variáveis do arquivo .env (ex: HF_TOKEN)
load_dotenv() 

# Pega o token do ambiente
MEU_TOKEN_API = os.getenv("HF_TOKEN")

# Constantes do Projeto
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "projeto_poesia_db"
COLLECTION_NAME = "poems"
MODEL_ID = "neuralmind/bert-large-portuguese-cased"

# Validação do Token
if not MEU_TOKEN_API:
    print("="*50)
    print("ERRO CRÍTICO: Token HF_TOKEN não encontrado.")
    print("Por favor, crie um arquivo .env na mesma pasta do script")
    print("com a linha: HF_TOKEN='hf_seu_token_aqui'")
    print("="*50)
    exit() # Encerra o script se não houver token

# --- 2. CARREGAR O MODELO DE IA (PIPELINE) ---

print(f"Carregando o modelo '{MODEL_ID}'...")
print("Isso pode demorar vários minutos na primeira vez (download).")

try:
    # AQUI ESTÁ A MÁGICA:
    # model=MODEL_ID -> O modelo que queremos
    # token=MEU_TOKEN_API -> Autentica seu download
    # device=0 -> USA SUA PLACA DE VÍDEO (RTX 2060)!
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model=MODEL_ID,
        token=MEU_TOKEN_API,
        device=0  
    )
    print("\n[SUCESSO] Modelo carregado e rodando na sua GPU (RTX 2060).")

except Exception as e:
    print(f"\n[ERRO CRÍTICO] Não foi possível carregar o modelo: {e}")
    print("Verifique seu token, conexão com a internet e se os drivers da NVIDIA estão instalados.")
    exit()

# --- 3. FUNÇÃO AUXILIAR PARA TRADUZIR O RESULTADO ---

def traduzir_estrelas_para_sentimento(label, score):
    """
    Converte os labels '1 star'...'5 stars' do modelo neuralmind
    para 'NEGATIVE', 'NEUTRAL', 'POSITIVE' e ajusta o score.
    """
    if label == "1 star" or label == "2 stars":
        # Sentimento negativo, score de -1.0 a 0.0
        return "NEGATIVE", (score * -1)
    elif label == "3 stars":
        # Sentimento neutro, score próximo de 0
        return "NEUTRAL", (score * 0.1) # Reduz o score para perto de 0
    elif label == "4 stars" or label == "5 stars":
        # Sentimento positivo, score de 0.0 a +1.0
        return "POSITIVE", score
    
    return "NEUTRAL", 0 # Caso padrão

# --- 4. CONEXÃO COM O MONGODB ---

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    # Testa a conexão
    client.server_info()
    print(f"Conectado ao MongoDB: '{DB_NAME}' > '{COLLECTION_NAME}'")
except Exception as e:
    print(f"\n[ERRO CRÍTICO] Não foi possível conectar ao MongoDB: {e}")
    print("Verifique se o MongoDB está rodando no endereço '{MONGO_URI}'")
    exit()

# --- 5. DEFINIÇÃO DA CONSULTA ---

# Query: Encontrar todos os poemas onde a análise básica ainda não foi feita
query = {"sentiment_analysis.primary_sentiment": None}

# IMPORTANTE: Começamos com um limite de 10 para testar
# Depois de confirmar, remova o ".limit(10)" para rodar em tudo
poemas_para_analisar = collection.find(query)

total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("\nNenhum poema novo para analisar. O enriquecimento já foi concluído.")
    client.close()
    exit()
else:
    print(f"Encontrados {total_para_analisar} poemas para analisar.")
    print("Iniciando o processamento dos 10 primeiros...")

# --- 6. O LOOP DE ENRIQUECIMENTO ---

start_time = time.time()
count = 0
erros = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    try:
        # Modelos BERT têm um limite de tokens (palavras)
        # Vamos truncar o texto nos primeiros 512 caracteres para garantir
        texto_para_analise = poem_text[:512]

        # 6a. A Análise de IA (Rodando na sua GPU)
        resultado = sentiment_analyzer(texto_para_analise)[0]
        
        # 6b. Traduzir o resultado (ex: '5 stars' -> 'POSITIVE')
        sentimento_label, sentimento_score = traduzir_estrelas_para_sentimento(
            resultado["label"], 
            resultado["score"]
        )

        # 6c. Preparar a atualização para o MongoDB
        update_data = {
            "$set": {
                "sentiment_analysis.primary_sentiment": sentimento_label,
                "sentiment_analysis.score": round(sentimento_score, 4),
                # Usamos o label como tag inicial de recomendação
                "recommendation_tags.good_for_feeling": [sentimento_label.lower()]
            }
        }
        
        # 6d. Atualizar o documento no banco
        collection.update_one({"_id": poem_id}, update_data)
        
        print(f"  > (Poema {count+1}/{total_para_analisar}) '{poem['title']}' analisado: {sentimento_label}")
        count += 1

    except Exception as e:
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")
        erros += 1

# --- 7. RESULTADOS FINAIS ---

end_time = time.time()
tempo_total = end_time - start_time

print("\n--- Análise de Teste Concluída! ---")
print(f"Total de poemas processados: {count}")
print(f"Total de erros: {erros}")
print(f"Tempo total: {tempo_total:.2f} segundos.")

if count > 0:
    print(f"Velocidade Média: {count / tempo_total:.2f} poemas/segundo.")
    print("\n[AÇÃO] Para analisar todos os poemas, remova o '.limit(10)' do script e rode novamente.")

client.close()