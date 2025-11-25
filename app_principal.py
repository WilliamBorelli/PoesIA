import os
import sys
import subprocess
import json
import random
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from textblob import TextBlob
from deep_translator import GoogleTranslator

# Bibliotecas Web
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Bibliotecas de Dados e IA
from pymongo import MongoClient
from textblob import TextBlob

# --- CONFIGURAÇÃO ---
app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app) # Permite que o front-end converse com o back-end sem erros de segurança

# Caminhos (Adaptados para rodar na sua pasta local)
BASE_DIR = Path(__file__).resolve().parent
# Assume que os scripts estão na mesma pasta
IMPORT_SCRIPT = BASE_DIR / "importar_poemas.py"
ENRICH_SCRIPT = BASE_DIR / "enriquecer_completo.py"
EXTRACT_SCRIPT = BASE_DIR / "extrair_palavras_chave.py"
CSV_PATH = BASE_DIR / "portuguese-poems.csv" # Certifique-se que o CSV está aqui

# Conexão MongoDB
# Carrega as variáveis do arquivo .env (para quando rodar localmente)
load_dotenv()

# Pega a URI da variável de ambiente OU usa o localhost como padrão (fallback)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# Conexão MongoDB
try:
    # Agora passamos a variável MONGO_URI, não o texto fixo
    client = MongoClient(MONGO_URI)
    
    # Opcional: Se sua string de conexão do Atlas tiver um nome de banco diferente,
    # você pode garantir que pegamos o banco certo aqui:
    db = client.get_database("projeto_poesia_db") 
    
    print(f"✅ Conectado ao MongoDB em: {MONGO_URI.split('@')[-1]}") # Mostra só o final por segurança
except Exception as e:
    print(f"❌ Erro ao conectar no MongoDB: {e}")
    db = None

# --- LÓGICA DE RECOMENDAÇÃO (DO NOSSO PROJETO ANTERIOR) ---
def recomendar_poema_mongo(sentimento_usuario, keyword_usuario=None):
    if db is None: return None
    
    poemas_collection = db["poems"]
    query_filter = {}

    # 1. Filtro por Sentimento
    if sentimento_usuario:
        query_filter["recommendation_tags.good_for_feeling"] = sentimento_usuario.lower()

    # 2. Filtro por Keyword (Opcional)
    if keyword_usuario:
        # Busca flexível (regex) ou exata no array
        query_filter["sentiment_analysis.keywords"] = keyword_usuario.lower()

    # Pipeline de busca
    pipeline = []
    if query_filter:
        pipeline.append({"$match": query_filter})
    
    pipeline.append({"$sample": {"size": 1}}) # Pega um aleatório

    resultado = list(poemas_collection.aggregate(pipeline))
    
    if resultado:
        return resultado[0]
    else:
        # Fallback: Se não achou com keyword, tenta só com sentimento
        if keyword_usuario and sentimento_usuario:
            return recomendar_poema_mongo(sentimento_usuario, None)
        # Fallback Final: Qualquer poema aleatório
        pipeline_random = [{"$sample": {"size": 1}}]
        res_random = list(poemas_collection.aggregate(pipeline_random))
        return res_random[0] if res_random else None

# --- ROTAS DA API ---

@app.route("/", methods=["GET"])
def index():
    # Cria a pasta static se não existir, para não dar erro
    if not os.path.exists(app.static_folder):
        os.makedirs(app.static_folder)
    # Tenta servir o index.html, ou cria um aviso se não existir
    try:
        return send_from_directory("static", "index.html")
    except:
        return "<h1>API de Poesia Rodando!</h1><p>Coloque seu 'index.html' na pasta 'static'.</p>"

@app.route("/api/recommend", methods=["POST"])
def recommend():
    """
    Recebe: JSON { "description": "Estou me sentindo triste e pensando no amor" }
    Retorna: JSON com o poema e a análise.
    """
    data = request.get_json() or {}
    user_desc = data.get("description", "").strip()

    if not user_desc:
        return jsonify({"ok": False, "error": "Descrição vazia"}), 400

    # 1. ANALISAR O INPUT DO USUÁRIO (Mini-NLP na hora)
    # Usamos o TextBlob para entender se a frase do usuário é positiva ou negativa
    blob = TextBlob(user_desc)
    polarity = blob.sentiment.polarity
    
    if polarity >= 0.1:
        detected_sentiment = "positive"
        sentiment_display = "Positivo"
    elif polarity <= -0.1:
        detected_sentiment = "negative"
        sentiment_display = "Negativo"
    else:
        detected_sentiment = "neutral"
        sentiment_display = "Neutro"

    # (Opcional) Tentar extrair uma palavra-chave simples da frase
    # Aqui pegamos a última palavra maior que 4 letras como "chute" de keyword
    # Ou passamos None para focar no sentimento
    words = [w for w in user_desc.split() if len(w) > 4]
    detected_keyword = words[-1].lower() if words else None

    # 2. BUSCAR NO MONGODB
    poema = recomendar_poema_mongo(detected_sentiment, detected_keyword)

    if poema:
        # Monta a resposta bonita
        resposta = {
            "ok": True,
            "sentiment": f"Detectamos um tom {sentiment_display}. Recomendação:",
            "poem": f"{poema['title'].upper()}\n\n{poema['full_text']}\n\n-- {poema['author']}",
            "details": {
                "tags": poema.get("recommendation_tags", {}).get("evokes", []),
                "match_sentiment": detected_sentiment
            }
        }
        
        # (Opcional) Salvar Interação
        try:
            db["user_interactions"].insert_one({
                "user_input": user_desc,
                "detected_sentiment": detected_sentiment,
                "recommended_poem_id": poema["_id"],
                "timestamp": datetime.utcnow()
            })
        except:
            pass # Não falha se não conseguir salvar log

        return jsonify(resposta)
    else:
        return jsonify({"ok": False, "error": "Banco de dados vazio ou erro de conexão"}), 500


# --- ROTAS DE ADMINISTRAÇÃO (Scripts) ---

@app.route("/api/import_poems", methods=["POST"])
def import_poems():
    return run_script(IMPORT_SCRIPT)

@app.route("/api/enrich", methods=["POST"])
def enrich():
    return run_script(ENRICH_SCRIPT)

@app.route("/api/extract_keywords", methods=["POST"])
def extract_keywords():
    return run_script(EXTRACT_SCRIPT)

def run_script(script_path):
    """Função auxiliar para rodar scripts Python"""
    if not os.path.exists(script_path):
        return jsonify({"ok": False, "error": f"Script não encontrado: {script_path}"}), 404
    
    try:
        # Roda o script e captura a saída
        proc = subprocess.run(
            [sys.executable, str(script_path)], # Usa o mesmo python que está rodando o Flask
            capture_output=True, 
            text=True, 
            timeout=300 # 5 minutos limite
        )
        return jsonify({
            "ok": True, 
            "stdout": proc.stdout, 
            "stderr": proc.stderr
        })
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "Tempo limite excedido"}), 504
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    print(f"🚀 Iniciando servidor Flask...")
    print(f"📂 Diretório Base: {BASE_DIR}")
    app.run(host="0.0.0.0", port=5000, debug=True)