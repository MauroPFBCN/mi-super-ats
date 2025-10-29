import os
import logging
import io
import re
import json
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Body, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, EmailStr, HttpUrl, validator
from motor.motor_asyncio import AsyncIOMotorClient
import PyPDF2
from docx import Document
from bs4 import BeautifulSoup
import httpx
from openai import AsyncOpenAI

# --- Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Carga de Secretos (Variables de Entorno) ---
# Se asume que estas variables están configuradas en el entorno de Render
MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017') # Default local para evitar error al inicio si falta
DB_NAME = os.environ.get('DB_NAME', 'ats_babel_db')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_DATABASE_ID = os.environ.get('NOTION_DATABASE_ID')
CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',') # Default a '*' si no está, Render lo debe limitar

if not all([MONGO_URL, DB_NAME, OPENAI_API_KEY, NOTION_API_TOKEN, NOTION_DATABASE_ID]):
    logger.warning("¡Advertencia! Una o más variables de entorno críticas (Mongo, OpenAI, Notion) no están configuradas.")

# --- Clientes ---
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client[DB_NAME]
# Inicializar cliente OpenAI solo si la clave existe
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# --- Opciones de Notion ---
NOTION_OPTIONS = {
    "STAGE": ["Lead", "InMail", "Application", "Waiting CV / ITV", "Babel Screening", "Interview Babel", "Submitted", "In Process Client", "HR Client's ITV", "Technical Client's ITV", "Meet the Team", "2nd Client's ITV", "Offer", "Hired"],
    "RESOLUTION": ["On Hold", "Waiting CV", "Procesando", "Pending Reply", "ITV Scheduled", "Babel Rejected", "Client Rejected in Submition", "Client Rejected", "Withdrawn Application", "TO REJECT", "TO PRESENT", "TO CONTACT", "Closed Job", "Hired"],
    # STATUS no estaba en tu código original, lo omito. Si es necesario, añádelo aquí y en los modelos.
    "REJECTION_REASON": ["Academic Background", "Already interviewed by client", "Cultural Fit", "Failed Languaje Test", "Failed Technical Screening Questions", "Failed Technical Test", "Freelance/Contractor", "Job Jumper", "Need VISA/PAC/Sponsorship", "No response", "No Show", "Not Interested", "Otro", "Salary Expectation", "Sobrecalificado", "Technical Skills / Not right experience", "Work Model"],
    "LANGUAJE": ["English A1", "English A2", "English B1", "English B2", "English C1", "English C2 Native", "Spanish B2", "Spanish C1", "Spanish C2 Native", "Catalan B1", "Catalan B2", "Catalan C1", "Catalan C2 Native", "French B1", "French B2", "French C1", "French C2 Native", "German A2", "German B2", "German C1", "German C2 Native", "Arabic C2 Native", "Bulgaro C2 Nativo", "Chinese B1", "Chinese B2", "Chinese C2 Native", "Dutch B2", "Dutch C2 Native", "Greek C2 Native", "Hebrew C2 Native", "Italian C2 Native", "Italiano B2", "Italiano C1", "Italian C2 Nativo", "Lithuanian C2 Native", "Polish C2 Native", "Portugues B1", "Portugues B2", "Portugues C1", "Portuguese C2 Native", "Russian C2 Native", "Turco C2 Nativo", "Turkish C2 Nativo", "Ukranian C2 Native", "Romanian C2 Native"],
    "SOURCE": ["Clay", "Linkedin JOB POST", "JOIN Multiposting", "Linkedin Personal", "PitchMe", "Recruitly", "Referral", "Sourced on LinkedIn", "People GPT", "Greenhouse", "SoftGarden"],
    "GENDER": ["Male", "Female"],
}

# --- Modelos Pydantic ---
class ExtractedData(BaseModel):
    # Campos extraídos por IA
    nombre_apellido: Optional[str] = Field(default="", description="Nombre completo sin títulos")
    email: Optional[EmailStr] = Field(default=None, description="Email principal")
    phone: Optional[str] = Field(default="", description="Teléfono limpio con prefijo internacional")
    location: Optional[str] = Field(default="", description="Ubicación en formato 'Ciudad, País'")
    linkedin_url: Optional[HttpUrl] = Field(default=None, description="URL de LinkedIn validada")
    current_company: Optional[str] = Field(default="", description="Empresa actual o 'Freelance'")
    skills: List[str] = Field(default_factory=list, description="Lista de hasta 8 skills clave")
    languages: List[str] = Field(default_factory=list, description="Lista de hasta 5 idiomas con nivel (si es posible)")
    gender: Optional[str] = Field(default="", description="'Male' o 'Female'")

    @validator('phone', pre=True)
    def clean_and_format_phone(cls, v):
        if not v or not isinstance(v, str): return ""
        # Eliminar todo excepto dígitos y el '+' inicial
        clean_phone = re.sub(r'[^\d+]', '', v)
        digits_only = re.sub(r'\D', '', clean_phone)

        if clean_phone.startswith('+') and len(digits_only) >= 9: # Asumir prefijo válido si tiene + y suficientes dígitos
             return f"+{digits_only}"
        elif len(digits_only) == 9 and digits_only[0] in '6789': # Heurística España
             return f"+34{digits_only}"
        elif len(digits_only) == 10 and digits_only[0] in '23456789': # Heurística USA/Canadá (simplificada)
             return f"+1{digits_only}"
        elif len(digits_only) >= 9: # Si tiene suficientes dígitos pero no prefijo claro
             return f"+{digits_only}" # Añadir '+' como intento genérico
        else:
            return digits_only # Devolver solo dígitos si es corto o irreconocible

    @validator('linkedin_url', pre=True)
    def validate_and_clean_linkedin(cls, v):
        if not v or not isinstance(v, str): return None
        # Normalizar URL
        if not v.startswith(('http://', 'https://')):
            v = f"https://{v}"
        # Asegurar que sea perfil /in/ y limpiar parámetros
        match = re.search(r'(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_]+/?)(?:\?.*)?', v, re.IGNORECASE)
        if match:
            clean_url = match.group(1)
            if not clean_url.endswith('/'): clean_url += '/'
            try:
                # Usar Pydantic para validación final
                validated_url = HttpUrl(clean_url, scheme="https")
                return validated_url
            except Exception:
                logger.warning(f"URL LinkedIn limpiada ({clean_url}) falló validación Pydantic.")
                return None
        logger.warning(f"URL proporcionada ({v}) no parece ser un perfil de LinkedIn válido.")
        return None

    @validator('nombre_apellido', pre=True)
    def clean_name(cls, v):
         if not v or not isinstance(v, str): return ""
         # Eliminar títulos comunes (lista ampliable)
         titles = ['Dr.', 'Dra.', 'Mr.', 'Mrs.', 'Ms.', 'PhD', 'Lic.', 'Ing.', 'Prof.','Sr.', 'Sra.']
         for title in titles:
              v = v.replace(title, '')
         return v.strip()

    @validator('current_company', pre=True)
    def clean_company(cls, v):
         if not v or not isinstance(v, str): return ""
         # Capitalizar si es Freelance/Contractor para consistencia
         if v.lower() in ["freelance", "contractor", "autonomo", "autónomo"]:
              return "Freelance"
         return v.strip()


class CandidateDataInput(BaseModel): # Datos completos enviados desde Frontend
    # --- Campos extraídos y editables ---
    nombre_apellido: str = Field(..., min_length=1, description="Nombre es obligatorio")
    email: Optional[EmailStr] = None
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[HttpUrl] = None
    current_company: Optional[str] = ""
    skills: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list) # Recibe la lista final seleccionada
    gender: Optional[str] = ""
    salary_expectation: Optional[str] = ""
    # --- Campos opcionales añadidos por usuario ---
    stage: Optional[str] = ""
    resolution: Optional[str] = ""
    rejection_reason: Optional[str] = ""
    source: Optional[str] = ""
    short_notes: Optional[str] = ""
    # --- Metadatos ---
    file_info: Optional[Dict[str, Any]] = None # filename, content_base64, size
    source_type: str # 'cv' or 'linkedin'

class ConfirmCreateRequest(BaseModel):
    candidate_data: CandidateDataInput
    force_create_duplicate: bool = False

class ConfirmCreateResponse(BaseModel):
    id: str # ID de MongoDB
    notion_record_id: str
    notion_url: HttpUrl
    message: str

class DuplicateCheckResponse(BaseModel):
    exists: bool
    candidate_id: Optional[str] = None
    notion_url: Optional[HttpUrl] = None

# --- Funciones de Extracción y Auxiliares ---

async def extract_text(file_content: bytes, filename: str) -> str:
    """Extrae texto de PDF o DOCX, manejo de errores mejorado."""
    text = ""
    try:
        if filename.lower().endswith('.pdf'):
            reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text: text += page_text + "\n"
        elif filename.lower().endswith(('.docx', '.doc')):
            # Añadir manejo básico de .doc si python-docx lo soporta indirectamente
            # o si se añade una librería específica como 'antiword' (requiere binario externo)
            doc = Document(io.BytesIO(file_content))
            text = "\n".join(para.text for para in doc.paragraphs if para.text)
        else:
            raise ValueError("Formato de archivo no soportado")
        logger.info(f"Texto extraído de {filename} ({len(text)} caracteres)")
        return text.strip()
    except PyPDF2.errors.PdfReadError:
        logger.error(f"Error leyendo PDF (posiblemente corrupto o protegido): {filename}")
        raise HTTPException(status_code=400, detail="El archivo PDF está corrupto o protegido con contraseña.")
    except Exception as e:
        logger.error(f"Error extrayendo texto de {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"No se pudo extraer texto del archivo: {e}")

async def extract_linkedin_from_text_robust(text_content: str) -> str:
    """Intenta extraer URL de LinkedIn con Regex, BeautifulSoup."""
    patterns = [
        r'(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_%/]+)', # Patrón más permisivo
        r'linkedin\.com/in/[a-zA-Z0-9\-_%/]+' # Sin http
    ]
    urls_found = set() # Usar set para evitar duplicados iniciales

    # 1. Regex directo
    for pattern in patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            url = match
            if not url.startswith('http'): url = f"https://{url}"
            # Limpiar parámetros y asegurar /in/
            if "linkedin.com/in/" in url:
                clean_url = url.split('?')[0].split('#')[0]
                if not clean_url.endswith('/'): clean_url += '/'
                urls_found.add(clean_url)

    # 2. BeautifulSoup para links incrustados
    try:
        soup = BeautifulSoup(text_content, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if "linkedin.com/in/" in href:
                 url = href
                 if not url.startswith('http'): url = f"https://{url}"
                 clean_url = url.split('?')[0].split('#')[0]
                 if not clean_url.endswith('/'): clean_url += '/'
                 urls_found.add(clean_url)
    except Exception: pass # Ignorar errores de parsing

    # Devolver la primera URL válida encontrada (o la más común si hay varias)
    if urls_found:
        # Podríamos añadir lógica para elegir la "mejor" si hay varias
        best_url = list(urls_found)[0]
        logger.info(f"LinkedIn encontrado por Regex/BS: {best_url}")
        return best_url
    logger.info("LinkedIn no encontrado por Regex/BS.")
    return ""

async def extract_data_with_ai(text_content: str) -> ExtractedData:
    """Realiza UNA llamada a OpenAI con prompt mejorado para extraer todos los datos."""
    if not openai_client:
        raise HTTPException(status_code=503, detail="Servicio de IA no configurado (falta OPENAI_API_KEY).")

    logger.info("Iniciando extracción con IA (Prompt v5)...")
    linkedin_url_direct = await extract_linkedin_from_text_robust(text_content)

    prompt = f"""
    Eres un asistente experto en Recursos Humanos especializado en extraer información clave de CVs y perfiles de LinkedIn. Analiza el texto proporcionado y devuelve un objeto JSON con los siguientes campos:

    TEXTO A ANALIZAR:
    ```{text_content[:6000]}```

    CAMPOS A EXTRAER (rellena con "" si no encuentras información):
    - nombre_apellido: El nombre completo del candidato. MUY IMPORTANTE: Omite cualquier título (Sr., Sra., Dr., Lic., PhD, Ing., Prof., Mr., Mrs., Ms.) o tratamiento. Solo nombre y apellidos.
    - email: La dirección de correo electrónico principal.
    - phone: El número de teléfono principal. Límpialo de espacios, puntos o guiones. Intenta deducir y añadir el prefijo internacional (ej: +34, +1) si es posible basándote en el texto o la ubicación. Formato final: +{prefijo}{número}.
    - location: La ciudad y país de residencia actual. Formato: "Ciudad, País".
    - linkedin_url: La URL COMPLETA del perfil de LinkedIn (https://linkedin.com/in/...). Busca activamente URLs incrustadas en texto (ej. la palabra 'LinkedIn' podría ser un link). Si encuentras varias, elige la más probable. Si ya te proporciono una URL pre-detectada, priorízala si parece correcta, pero verifica si encuentras una mejor en el texto principal. Limpia parámetros de tracking.
    - current_company: El nombre de la empresa donde trabaja actualmente (busca términos como "Presente", "Actualidad", "Current", la fecha más reciente sin fecha de fin). Si indica ser autónomo, responde "Freelance". Omite el puesto de trabajo.
    - skills: Una lista de MÁXIMO 8 habilidades técnicas, herramientas o roles principales mencionados. Generaliza los puestos (ej: "Software Engineer" en lugar de "Senior Software Engineer II"). ["Java", "React", "AWS", "Project Management", "SQL"].
    - languages: Una lista de MÁXIMO 5 idiomas indicados, incluyendo el nivel si se especifica claramente. Intenta usar los niveles del MCER (A1, A2, B1, B2, C1, C2 Native). Ej: ["Spanish C2 Native", "English B2", "Catalan B1"].
    - gender: Infiere el género ("Male" o "Female") basándote principalmente en el nombre. Si el nombre es ambiguo o no puedes determinarlo, pon "".

    URL DE LINKEDIN PRE-DETECTADA (Considera esta URL si no encuentras otra): {linkedin_url_direct or "Ninguna"}

    RESPONDE ÚNICAMENTE CON EL OBJETO JSON VÁLIDO. NO incluyas explicaciones, comentarios, ni la palabra 'json' o ``` al principio o final. Ejemplo:
    {{ "nombre_apellido": "...", "email": "...", "phone": "...", ... }}
    """
    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Extrae datos de CV/LinkedIn en formato JSON solicitado."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0, # Mayor precisión
            max_tokens=1000 # Aumentar por si acaso
        )
        json_response = response.choices[0].message.content
        logger.info(f"Respuesta JSON de IA: {json_response}")
        # Parsear con manejo de errores robusto
        try:
             data = json.loads(json_response)
        except json.JSONDecodeError as json_err:
             logger.error(f"Error crítico: IA devolvió JSON inválido: {json_err}. Respuesta: {json_response}")
             # Intentar limpiar si está envuelto en ```json ... ```
             match = re.search(r'```json\s*(\{.*\})\s*```', json_response, re.DOTALL)
             if match:
                  try:
                      data = json.loads(match.group(1))
                      logger.info("JSON recuperado tras limpiar ```json")
                  except Exception as inner_err:
                      logger.error(f"Fallo al recuperar JSON incluso después de limpiar ```: {inner_err}")
                      raise HTTPException(status_code=500, detail="La IA devolvió una respuesta inválida (JSON mal formado).") from inner_err
             else:
                 raise HTTPException(status_code=500, detail="La IA devolvió una respuesta inválida (JSON no encontrado).") from json_err


        # Validar y limpiar datos con Pydantic (esto aplicará los @validator)
        extracted = ExtractedData(**data)

        # Lógica final LinkedIn: Priorizar IA si validó, si no, usar pre-detectada si validó
        if not extracted.linkedin_url and linkedin_url_direct:
            validated_direct_url = ExtractedData(linkedin_url=linkedin_url_direct).linkedin_url
            if validated_direct_url:
                extracted.linkedin_url = validated_direct_url
                logger.info("Usando URL LinkedIn pre-detectada tras validación.")

        # Validar Idiomas contra Notion Options (mejor esfuerzo)
        valid_languages = []
        if extracted.languages:
            lang_options_lower = {opt.lower(): opt for opt in NOTION_OPTIONS["LANGUAJE"]}
            for lang in extracted.languages:
                lang_lower = lang.lower()
                if lang_lower in lang_options_lower:
                    valid_languages.append(lang_options_lower[lang_lower])
                else: # Intentar matching parcial (ej. "English Advanced" -> "English C1")
                    parts = lang.split()
                    if len(parts) > 0:
                        base_lang_lower = parts[0].lower()
                        # Buscar la opción más cercana en Notion para ese idioma base
                        best_match = None
                        for option in NOTION_OPTIONS["LANGUAJE"]:
                             if option.lower().startswith(base_lang_lower):
                                 # Aquí podríamos añadir lógica para mapear niveles (B2, C1, Advanced, etc.)
                                 best_match = option
                                 break # Tomar la primera opción de Notion para ese idioma
                        if best_match and best_match not in valid_languages:
                             valid_languages.append(best_match)

        extracted.languages = list(set(valid_languages))[:5] # Únicos, max 5

        logger.info("Extracción con IA completada y validada.")
        return extracted

    except HTTPException: raise # Re-lanzar excepciones HTTP ya manejadas
    except Exception as e:
        logger.exception("Error inesperado en extracción con IA")
        raise HTTPException(status_code=500, detail=f"Error inesperado durante la extracción IA: {e}")

# (Las funciones check_email_duplicate y upload_cv_to_external_service se mantienen igual)
async def check_email_duplicate(email: Optional[EmailStr]) -> DuplicateCheckResponse:
    # ... (igual que antes)
    if not email: return DuplicateCheckResponse(exists=False)
    try:
        # Pydantic ya validó el email, convertir a string para Mongo
        email_str = str(email).strip().lower()
        existing = await db.candidates.find_one({"email": email_str}, {"_id": 1, "notion_url": 1})
        if existing:
            # Validar URL de Notion antes de devolverla
            notion_url = None
            try:
                if existing.get("notion_url"): notion_url = HttpUrl(existing["notion_url"], scheme="https")
            except Exception: pass
            return DuplicateCheckResponse(exists=True, candidate_id=str(existing["_id"]), notion_url=notion_url)
        return DuplicateCheckResponse(exists=False)
    except Exception as e:
        logger.error(f"Error comprobando duplicado de email {email}: {e}")
        return DuplicateCheckResponse(exists=False)

async def upload_cv_to_external_service(file_content: bytes, filename: str) -> str:
    # ... (igual que antes, usando file.io)
    try:
        files = {'file': (filename, file_content)}
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post('[https://file.io?expires=1d](https://file.io?expires=1d)', files=files) # Expira en 1 día
            if response.status_code == 200:
                data = response.json()
                file_url = data.get('link')
                if file_url:
                    logger.info(f"Archivo subido a file.io: {file_url}")
                    return file_url
            logger.error(f"Error subiendo a file.io ({response.status_code}): {response.text}")
            return ""
    except Exception as e:
        logger.error(f"Error en upload_cv_to_external_service: {e}")
        return ""


async def create_notion_page(candidate_data: CandidateDataInput) -> tuple[str, str]:
    """Crea la página en Notion, incluyendo el adjunto."""
    if not NOTION_API_TOKEN or not NOTION_DATABASE_ID:
         raise HTTPException(status_code=503, detail="Configuración de Notion incompleta en el servidor.")

    logger.info(f"Creando registro en Notion para: {candidate_data.nombre_apellido}")
    headers = {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    properties = { # Construcción cuidadosa de propiedades
        "Nombre y Apellido": {"title": [{"text": {"content": candidate_data.nombre_apellido or "Nombre no extraído"}}]},
    }
    # Usar .dict() para manejar opcionales y conversiones
    data_dict = candidate_data.dict(exclude_unset=True) # Excluir campos no enviados

    if data_dict.get("email"): properties["Email"] = {"email": str(data_dict["email"])} # Convertir EmailStr a str
    if data_dict.get("phone"): properties["Phone"] = {"phone_number": data_dict["phone"]}
    if data_dict.get("location"): properties["LOCATION"] = {"rich_text": [{"text": {"content": data_dict["location"]}}]}
    if data_dict.get("linkedin_url"): properties["LINKEDIN URL"] = {"url": str(data_dict["linkedin_url"])} # Convertir HttpUrl a str
    if data_dict.get("current_company"): properties["CURRENT COMPANY"] = {"rich_text": [{"text": {"content": data_dict["current_company"]}}]}
    if data_dict.get("salary_expectation"): properties["SALARY EXPECTATION"] = {"rich_text": [{"text": {"content": data_dict["salary_expectation"]}}]}
    if data_dict.get("short_notes"): properties["SHORT NOTES"] = {"rich_text": [{"text": {"content": data_dict["short_notes"]}}]}

    # Selects: Validar contra opciones
    if data_dict.get("stage") in NOTION_OPTIONS["STAGE"]: properties["STAGE"] = {"select": {"name": data_dict["stage"]}}
    if data_dict.get("resolution") in NOTION_OPTIONS["RESOLUTION"]: properties["RESOLUTION"] = {"select": {"name": data_dict["resolution"]}}
    if data_dict.get("rejection_reason") in NOTION_OPTIONS["REJECTION_REASON"]: properties["REJECTION REASON"] = {"select": {"name": data_dict["rejection_reason"]}}
    if data_dict.get("source") in NOTION_OPTIONS["SOURCE"]: properties["SOURCE"] = {"select": {"name": data_dict["source"]}}
    if data_dict.get("gender") in NOTION_OPTIONS["GENDER"]: properties["Gender"] = {"select": {"name": data_dict["gender"]}}

    # Multi-Selects: Validar y limitar
    if data_dict.get("skills"):
        valid_skills = [s for s in data_dict["skills"] if s][:10] # Tomar solo los 10 primeros no vacíos
        if valid_skills: properties["Skills"] = {"multi_select": [{"name": s} for s in valid_skills]}
    if data_dict.get("languages"):
        valid_langs = [lang for lang in data_dict["languages"] if lang in NOTION_OPTIONS["LANGUAJE"]]
        if valid_langs: properties["LANGUAJE"] = {"multi_select": [{"name": lang} for lang in valid_langs]}

    # --- LÓGICA DE ADJUNTO ---
    if candidate_data.file_info and candidate_data.file_info.get("content_base64"):
        logger.info("Procesando adjunto para Notion...")
        try:
            file_content = base64.b64decode(candidate_data.file_info["content_base64"])
            filename = candidate_data.file_info["filename"]
            logger.info(f"Adjunto: {filename}, Tamaño: {len(file_content)} bytes")
            file_url = await upload_cv_to_external_service(file_content, filename)
            if file_url:
                properties["ATTACHMENT"] = { "files": [{"type": "external", "name": filename, "external": {"url": file_url}}] }
                logger.info(f"Propiedad ATTACHMENT añadida para {filename}")
            else:
                logger.warning(f"No se pudo obtener URL para el adjunto {filename}, se creará sin él.")
        except Exception as e:
            logger.error(f"Error procesando/subiendo adjunto para Notion: {e}")
    # --- FIN: LÓGICA DE ADJUNTO ---

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    logger.debug(f"Payload final para Notion API: {json.dumps(payload, indent=2)}")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post("[https://api.notion.com/v1/pages](https://api.notion.com/v1/pages)", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Registro creado en Notion exitosamente: ID={data['id']}")
            return data['id'], data['url']
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        logger.error(f"Error API Notion ({e.response.status_code}): {error_body}")
        # Intentar extraer mensaje de error de Notion si es JSON
        detail = f"Error de API Notion ({e.response.status_code})"
        try: detail += f": {e.response.json().get('message', error_body)}"
        except Exception: detail += f": {error_body}"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except Exception as e:
        logger.exception("Error inesperado creando página en Notion")
        raise HTTPException(status_code=500, detail=f"Error interno al crear registro en Notion: {e}")

# --- API Endpoints ---
api_router = APIRouter(prefix="/api")

@api_router.get("/options", response_model=Dict[str, List[str]])
async def get_notion_options_endpoint_v2():
    """Devuelve las opciones para los dropdowns y multi-selects."""
    # Añadir opción vacía solo a los selects simples
    options = {}
    selects = ["STAGE", "RESOLUTION", "REJECTION_REASON", "SOURCE", "GENDER"]
    for key, values in NOTION_OPTIONS.items():
        if key in selects:
            options[key] = [""] + values # Añadir "" al principio
        else: # LANGUAJE (multi-select)
            options[key] = sorted(values) # Devolver ordenado
    return options

@api_router.post("/process")
async def process_source_endpoint_v2(file: Optional[UploadFile] = File(None), linkedin_url: Optional[HttpUrl] = Form(None)):
    """Procesa CV o LinkedIn, extrae datos con IA, devuelve JSON validado."""
    if not file and not linkedin_url:
        raise HTTPException(status_code=400, detail="Debe proporcionar un archivo CV o una URL de LinkedIn.")
    if file and linkedin_url:
        raise HTTPException(status_code=400, detail="Proporcione solo un archivo CV o una URL, no ambos.")

    text_content = ""
    source_type = ""
    file_info = None

    try:
        if file:
            logger.info(f"Procesando archivo CV: {file.filename}")
            if not file.filename or not file.filename.lower().endswith(('.pdf', '.docx', '.doc')):
                 raise HTTPException(status_code=400, detail="Formato de archivo no permitido o nombre inválido.")
            file_content = await file.read()
            text_content = await extract_text(file_content, file.filename)
            source_type = "cv"
            file_info = {
                 "filename": file.filename,
                 "content_base64": base64.b64encode(file_content).decode('utf-8'),
                 "size": len(file_content)
            }
        elif linkedin_url:
            logger.info(f"Procesando URL LinkedIn: {linkedin_url}")
            text_content = await extract_linkedin_data(str(linkedin_url)) # Simple scraping
            source_type = "linkedin"

        if not text_content or not text_content.strip():
             # Añadir log específico si el scraping falló
             if source_type == "linkedin": logger.warning(f"Scraping de LinkedIn ({linkedin_url}) devolvió texto vacío.")
             raise HTTPException(status_code=400, detail="No se pudo obtener contenido textual de la fuente.")

        extracted_data = await extract_data_with_ai(text_content)

        # Forzar URL original si es de LinkedIn y no se extrajo/validó
        if source_type == "linkedin" and not extracted_data.linkedin_url:
             validated_original_url = ExtractedData(linkedin_url=str(linkedin_url)).linkedin_url
             if validated_original_url:
                 extracted_data.linkedin_url = validated_original_url

        duplicate_info = await check_email_duplicate(extracted_data.email)

        # Convertir Pydantic a dict, asegurando que HttpUrl sea string
        extracted_dict = extracted_data.dict()
        if extracted_dict.get("linkedin_url"): extracted_dict["linkedin_url"] = str(extracted_dict["linkedin_url"])
        if duplicate_info.notion_url: duplicate_info.notion_url = str(duplicate_info.notion_url)

        return {
            "extracted_data": extracted_dict,
            "file_info": file_info,
            "source_type": source_type,
            "duplicate_info": duplicate_info.dict()
        }
    except HTTPException as e:
        logger.error(f"HTTPException en /process: {e.detail}")
        raise e
    except Exception as e:
        logger.exception("Error inesperado en /process")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {e}")


@api_router.post("/candidates/confirm-create", response_model=ConfirmCreateResponse)
async def confirm_create_endpoint_v3(request: ConfirmCreateRequest):
    """Confirma y crea el candidato en Notion (con adjunto) y MongoDB."""
    candidate_input = request.candidate_data
    logger.info(f"Confirmando creación para: {candidate_input.nombre_apellido} (Email: {candidate_input.email}) Force: {request.force_create_duplicate}")

    # Re-validar duplicado si no se fuerza
    if not request.force_create_duplicate and candidate_input.email:
        duplicate_check = await check_email_duplicate(candidate_input.email)
        if duplicate_check.exists:
            logger.warning(f"Intento de crear duplicado (email: {candidate_input.email}) sin forzar.")
            raise HTTPException(
                status_code=409, # Conflict
                detail=f"Email duplicado detectado ({candidate_input.email}). Confirma si quieres crearlo igualmente."
            )

    # 1. Crear en Notion (incluye subida de adjunto)
    try:
        notion_id, notion_url_str = await create_notion_page(candidate_input)
        try:
            notion_url = HttpUrl(notion_url_str, scheme="https")
        except Exception:
             logger.error(f"URL de Notion inválida recibida: {notion_url_str}, usando fallback.")
             notion_url = HttpUrl("[https://www.notion.so](https://www.notion.so)", scheme="https") # Fallback
    except HTTPException as e:
        raise e # Re-lanzar errores de Notion o de validación
    except Exception as e:
        logger.exception("Error inesperado al llamar a create_notion_page")
        raise HTTPException(status_code=500, detail=f"Error interno al crear en Notion: {e}")

    # 2. Guardar en MongoDB
    mongo_id = str(uuid.uuid4())
    candidate_doc = {
        "_id": mongo_id,
        "notion_record_id": notion_id,
        "notion_url": str(notion_url),
        "created_at": datetime.now(timezone.utc),
        **candidate_input.dict(exclude={'file_info'}) # Excluir file_info de Mongo
    }
    # Convertir tipos complejos a string para MongoDB
    if candidate_doc.get("email"): candidate_doc["email"] = str(candidate_doc["email"]).lower()
    if candidate_doc.get("linkedin_url"): candidate_doc["linkedin_url"] = str(candidate_doc["linkedin_url"])

    try:
        await db.candidates.insert_one(candidate_doc)
        logger.info(f"Candidato guardado en MongoDB con ID: {mongo_id}")
    except Exception as e:
        logger.error(f"Error guardando candidato en MongoDB (Notion ID: {notion_id}): {e}") # No crítico

    return ConfirmCreateResponse(
        id=mongo_id,
        notion_record_id=notion_id,
        notion_url=notion_url,
        message="Candidato creado exitosamente en Notion y MongoDB."
    )

# --- Inicialización de la App ---
app = FastAPI(
    title="ATS Babel - CV Processor v5.1",
    description="API optimizada para procesar CVs/LinkedIn, extraer datos con IA, adjuntar archivos y enviar a Notion.",
    version="5.1.0" # Incremento de versión patch
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/", include_in_schema=False)
async def root_v5_1():
    # Endpoint simple para verificar que el servidor está corriendo
    return {"message": "ATS API v5.1 is running."}

@app.on_event("shutdown")
async def shutdown_db_client_v5_1():
    mongo_client.close()
    logger.info("Conexión MongoDB cerrada.")
