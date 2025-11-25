import time
from pymongo import MongoClient
from textblob import TextBlob

# --- 1. CONFIGURAÇÃO DO MONGODB ---
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string de conexão
db = client["projeto_poesia_db"]
collection = db["poems"]

# --- 2. LÓGICA DE NOVAS TAGS (Do script de refinamento) ---

def get_subjectivity_tag(score):
    """Converte o score numérico de subjetividade (0.0 a 1.0) em um label."""
    if score > 0.66:
        return "Subjetivo" # Muito emocional / opinativo
    elif score > 0.33:
        return "Reflexivo" # Um balanço entre fato e opinião
    else:
        return "Objetivo"  # Muito factual / descritivo

def get_combined_emotion_tag(primary_sentiment, subjectivity_score):
    """Cria a tag de emoção combinada (ex: "Apaixonado")."""
    
    is_subjetivo = subjectivity_score > 0.66
    is_reflexivo = subjectivity_score > 0.33

    if primary_sentiment == "POSITIVE":
        if is_subjetivo:
            return "Apaixonado" # (Positivo + Muito Emocional)
        if is_reflexivo:
            return "Esperançoso" # (Positivo + Meio Emocional)
        return "Sereno"       # (Positivo + Objetivo)
        
    elif primary_sentiment == "NEGATIVE":
        if is_subjetivo:
            return "Melancólico" # (Negativo + Muito Emocional)
        if is_reflexivo:
            return "Sombrio"     # (Negativo + Meio Emocional)
        return "Crítico"      # (Negativo + Objetivo)
        
    else: # NEUTRAL
        if is_subjetivo:
            return "Introspectivo" # (Neutro + Muito Emocional)
        return "Contemplativo" # (Neutro + Objetivo/Reflexivo)

# --- 3. DEFINIÇÃO DA CONSULTA ---
# Esta é a consulta "do zero":
# Encontrar poemas onde o sentimento primário ainda é 'None'.
query = {"sentiment_analysis.primary_sentiment": None}

# IMPORTANTE: Remova o '.limit(10)' abaixo para rodar em TUDO
poemas_para_analisar = collection.find(query).limit(10) 

total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("Nenhum poema novo para analisar. O enriquecimento já foi concluído.")
else:
    print(f"Encontrados {total_para_analisar} poemas para analisar (execução 'do zero').")
    print("Iniciando o processamento completo...")


# --- 4. O LOOP DE ENRIQUECIMENTO E REFINAMENTO ---
start_time = time.time()
count = 0
erros = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    poem_text = poem["full_text"]
    
    # Prepara o documento de atualização
    update_data = {}

    try:
        # --- Verificação de Segurança (para poemas vazios) ---
        if not poem_text or not poem_text.strip():
            print(f"  > [AVISO] Poema '{poem['title']}' está VAZIO. Marcando como 'NEUTRAL'.")
            primary_label = "NEUTRAL"
            sentimento_score = 0.0
            subjectivity_score = 0.0
            final_secondary_tags = ["Indefinido"]
        
        else:
            # --- PASSO 1: ENRIQUECER (Rodar TextBlob) ---
            analysis = TextBlob(poem_text)
            sentimento_score = analysis.sentiment.polarity
            subjectivity_score = analysis.sentiment.subjectivity

            # --- PASSO 2: REFINAR (Traduzir os scores) ---
            
            # 2a. Traduzir Sentimento Primário
            if sentimento_score >= 0.05:
                primary_label = "POSITIVE"
            elif sentimento_score <= -0.05:
                primary_label = "NEGATIVE"
            else:
                primary_label = "NEUTRAL"
            
            # 2b. Traduzir Sentimento Secundário (Array)
            tag_subjetividade = get_subjectivity_tag(subjectivity_score)
            tag_emocao = get_combined_emotion_tag(primary_label, subjectivity_score)
            
            # Cria o array final
            final_secondary_tags = list(set([tag_subjetividade, tag_emocao]))

        # --- PASSO 3: Preparar o Documento Final ---
        update_data = {
            "sentiment_analysis.primary_sentiment": primary_label,
            "sentiment_analysis.score": sentimento_score,
            "sentiment_analysis.subjectivity_score": subjectivity_score,
            "sentiment_analysis.secondary_sentiment": final_secondary_tags, # <-- O ARRAY!
            "recommendation_tags.good_for_feeling": [primary_label.lower()]
        }
        
    except Exception as e:
        # Se o TextBlob falhar por um motivo inesperado
        print(f"ERRO ao analisar o poema '{poem['title']}' (ID: {poem_id}): {e}")
        erros += 1
        # Marca como "Indefinido" para não tentar de novo
        update_data = {
            "sentiment_analysis.primary_sentiment": "NEUTRAL",
            "sentiment_analysis.score": 0.0,
            "sentiment_analysis.subjectivity_score": 0.0,
            "sentiment_analysis.secondary_sentiment": ["Indefinido"],
            "recommendation_tags.good_for_feeling": ["neutral"]
        }
    
    finally:
        # --- PASSO 4: Salvar no Banco de Dados ---
        # Este 'finally' garante que o poema seja atualizado
        # mesmo se houver um erro, evitando que 'None' permaneça.
        if update_data:
            collection.update_one(
                {"_id": poem_id},
                {"$set": update_data}
            )
        count += 1
        
        if count % 100 == 0: # Imprime um status a cada 100 poemas
             print(f"  > Processados {count} / {total_para_analisar} poemas...")

# --- 5. RESULTADOS ---
end_time = time.time()
print("\n--- Processamento Completo Concluído! ---")
print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
print(f"Total de erros de análise: {erros}")
if total_para_analisar > 0 and count == 10: # (Assumindo o limite de 10)
    print("\n[AÇÃO] Para analisar todos os poemas, remova o '.limit(10)' do script e rode novamente.")

client.close()