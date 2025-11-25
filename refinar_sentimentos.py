import time
from pymongo import MongoClient

# --- 1. CONFIGURAÇÃO DO MONGODB ---
client = MongoClient("mongodb://localhost:27017/") # <-- Verifique sua string de conexão
db = client["projeto_poesia_db"]
collection = db["poems"]

# --- 2. LÓGICA DE NOVAS TAGS ---

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
# Vamos rodar em TODOS os poemas para garantir que
# todos sejam atualizados para o novo formato de Array.
query = {} 

poemas_para_analisar = collection.find(query)
total_para_analisar = collection.count_documents(query)

if total_para_analisar == 0:
    print("Banco de dados está vazio.")
else:
    print(f"Encontrados {total_para_analisar} poemas para refinar.")
    print("Iniciando o processamento...")

# --- 4. O LOOP DE REFINAMENTO ---
start_time = time.time()
count = 0
erros_corrigidos = 0

for poem in poemas_para_analisar:
    poem_id = poem["_id"]
    new_tags_array = []
    
    try:
        # Usamos .get() para acessar os dados com segurança,
        # caso a estrutura 'sentiment_analysis' não exista
        analysis_data = poem.get("sentiment_analysis", {})
        
        primary = analysis_data.get("primary_sentiment")
        subj_score = analysis_data.get("subjectivity_score")

        # ===============================================
        #  AQUI CORRIGIMOS OS NULOS (Sua Pergunta 1)
        # ===============================================
        if primary is None or subj_score is None:
            new_tags_array = ["Indefinido"]
            erros_corrigidos += 1
        
        # ===============================================
        #  AQUI FAZEMOS O UPGRADE (Sua Pergunta 2)
        # ===============================================
        else:
            tag_subjetividade = get_subjectivity_tag(subj_score)
            tag_emocao = get_combined_emotion_tag(primary, subj_score)
            
            # Adicionamos as tags ao array (usando set para evitar duplicatas)
            new_tags_array = list(set([tag_subjetividade, tag_emocao]))

        # 4b. Atualiza o documento no banco
        # $set vai SOBRESCREVER o campo 'secondary_sentiment'
        # com o nosso novo array.
        collection.update_one(
            {"_id": poem_id},
            {"$set": {"sentiment_analysis.secondary_sentiment": new_tags_array}}
        )
        
        count += 1
        if count % 1000 == 0: # Imprime um status a cada 1000 poemas
             print(f"  > Processados {count} / {total_para_analisar} poemas...")

    except Exception as e:
        print(f"ERRO ao refinar o poema (ID: {poem_id}): {e}")

# --- 5. RESULTADOS ---
end_time = time.time()
print("\n--- Refinamento Concluído! ---")
print(f"Total de {count} poemas atualizados em {end_time - start_time:.2f} segundos.")
print(f"Total de {erros_corrigidos} poemas que estavam nulos foram corrigidos para ['Indefinido'].")

client.close()