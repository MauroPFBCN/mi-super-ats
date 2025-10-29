# === INICIO main.py (Versión 5.1 - Incluye adjuntos, sin 'prefijo') ===
import os
import logging
import io
import re
import json
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Body, Form # Asegurar que Form está aquí
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
MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
DB_NAME = os.environ.get('DB_NAME', 'ats_babel_db')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_DATABASE_ID = os.environ.get('NOTION_DATABASE_ID')
CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',')

if not all([MONGO_URL, DB_NAME, OPENAI_API_KEY, NOTION_API_TOKEN, NOTION_DATABASE_ID]):
    logger.warning("¡Advertencia! Variables críticas (Mongo, OpenAI, Notion) no configuradas.")

# --- Clientes ---
try:
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    # Ping a la DB para verificar conexión al inicio (opcional pero bueno para debug)
    # Intenta obtener el nombre de la DB para forzar conexión/error temprano
    db = mongo_client.get_database(DB_NAME)
    # Considerar: await db.command('ping') dentro de una función async startup si es necesario
    logger.info("Conexión inicial a MongoDB establecida (o diferida).")
except Exception as mongo_err:
    logger.error(f"Error CRÍTICO conectando a MongoDB al inicio: {mongo_err}")
    # Podríamos decidir salir si la DB es esencial al inicio
    # raise SystemExit(f"MongoDB connection failed: {mongo_err}") from mongo_err
    db = None # Marcar DB como no disponible

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
if not openai_client:
    logger.warning("Cliente OpenAI no inicializado (falta OPENAI_API_KEY).")

# --- Opciones de Notion ---
NOTION_OPTIONS = {
    "STAGE": ["Lead", "InMail", "Application", "Waiting CV / ITV", "Babel Screening", "Interview Babel", "Submitted", "In Process Client", "HR Client's ITV", "Technical Client's ITV", "Meet the Team", "2nd Client's ITV", "Offer", "Hired"],
    "RESOLUTION": ["On Hold", "Waiting CV", "Procesando", "Pending Reply", "ITV Scheduled", "Babel Rejected", "Client Rejected in Submition", "Client Rejected", "Withdrawn Application", "TO REJECT", "TO PRESENT", "TO CONTACT", "Closed Job", "Hired"],
    "REJECTION_REASON": ["Academic Background", "Already interviewed by client", "Cultural Fit", "Failed Languaje Test", "Failed Technical Screening Questions", "Failed Technical Test", "Freelance/Contractor", "Job Jumper", "Need VISA/PAC/Sponsorship", "No response", "No Show", "Not Interested", "Otro", "Salary Expectation", "Sobrecalificado", "Technical Skills / Not right experience", "Work Model"],
    "LANGUAJE": sorted(["English A1", "English A2", "English B1", "English B2", "English C1", "English C2 Native", "Spanish B2", "Spanish C1", "Spanish C2 Native", "Catalan B1", "Catalan B2", "Catalan C1", "Catalan C2 Native", "French B1", "French B2", "French C1", "French C2 Native", "German A2", "German B2", "German C1", "German C2 Native", "Arabic C2 Native", "Bulgaro C2 Nativo", "Chinese B1", "Chinese B2", "Chinese C2 Native", "Dutch B2", "Dutch C2 Native", "Greek C2 Native", "Hebrew C2 Native", "Italian C2 Native", "Italiano B2", "Italiano C1", "Italian C2 Nativo", "Lithuanian C2 Native", "Polish C2 Native", "Portugues B1", "Portugues B2", "Portugues C1", "Portuguese C2 Native", "Russian C2 Native", "Turco C2 Nativo", "Turkish C2 Nativo", "Ukranian C2 Native", "Romanian C2 Native"]),
    "SOURCE": ["Clay", "Linkedin JOB POST", "JOIN Multiposting", "Linkedin Personal", "PitchMe", "Recruitly", "Referral", "Sourced on LinkedIn", "People GPT", "Greenhouse", "SoftGarden"],
    "GENDER": ["Male", "Female"],
}

# --- Modelos Pydantic ---
class ExtractedData(BaseModel):
    nombre_apellido: Optional[str] = Field(default="", description="Nombre completo sin títulos")
    email: Optional[EmailStr] = Field(default=None)
    phone: Optional[str] = Field(default="")
    location: Optional[str] = Field(default="")
    linkedin_url: Optional[HttpUrl] = Field(default=None)
    current_company: Optional[str] = Field(default="")
    skills: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    gender: Optional[str] = Field(default="")

    @validator('phone', pre=True)
    def clean_and_format_phone_v2(cls, v): # Renombrado por claridad
        if not v or not isinstance(v, str): return ""
        clean_phone = re.sub(r'[^\d+]', '', v)
        digits_only = re.sub(r'\D', '', clean_phone)

        if clean_phone.startswith('+') and len(digits_only) >= 9:
             # Ya tiene prefijo, solo limpiar dígitos después del +
             return f"+{digits_only}"
        elif len(digits_only) == 9 and digits_only[0] in '6789': # Heurística España
             return f"+34{digits_only}"
        elif len(digits_only) == 10 and digits_only[0] in '23456789': # Heurística USA/Canadá
             return f"+1{digits_only}"
        elif len(digits_only) >= 9: # Otros casos con suficientes dígitos
             return f"+{digits_only}" # Añadir '+' genérico
        else:
            return digits_only # Devolver solo dígitos si es corto

    @validator('linkedin_url', pre=True)
    def validate_and_clean_linkedin_v2(cls, v): # Renombrado por claridad
        if not v or not isinstance(v, str): return None
        if not v.startswith(('http://', 'https://')): v = f"https://{v}"
        match = re.search(r'(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_%/]+/?)(?:[\?#].*)?$', v, re.IGNORECASE)
        if match:
            clean_url = match.group(1)
            if not clean_url.endswith('/'): clean_url += '/'
            try: return HttpUrl(clean_url, scheme="https")
            except Exception: return None
        return None

    @validator('nombre_apellido', pre=True)
    def clean_name_v2(cls, v): # Renombrado por claridad
         if not v or not isinstance(v, str): return ""
         titles = ['Dr.', 'Dra.', 'Mr.', 'Mrs.', 'Ms.', 'PhD', 'Lic.', 'Ing.', 'Prof.','Sr.', 'Sra.']
         # Usar regex para eliminar títulos al inicio/final o con espacios
         cleaned = re.sub(r'(^|\s+)(' + '|'.join(re.escape(t) for t in titles) + r')(\s+|$)', ' ', v, flags=re.IGNORECASE).strip()
         # Quitar múltiples espacios
         return re.sub(r'\s+', ' ', cleaned).strip()


    @validator('current_company', pre=True)
    def clean_company_v2(cls, v): # Renombrado por claridad
         if not v or not isinstance(v, str): return ""
         if v.lower() in ["freelance", "contractor", "autonomo", "autónomo", "self-employed"]: return "Freelance"
         return v.strip()

# (Resto de modelos: CandidateDataInput, ConfirmCreateRequest, etc. se mantienen igual)
class CandidateDataInput(BaseModel):
    nombre_apellido: str = Field(..., min_length=1)
    email: Optional[EmailStr] = None
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[HttpUrl] = None
    current_company: Optional[str] = ""
    skills: List[str] = Field(default_factory=list)
    languages: List[str] = Field(default_factory=list)
    gender: Optional[str] = ""
    salary_expectation: Optional[str] = ""
    stage: Optional[str] = ""
    resolution: Optional[str] = ""
    rejection_reason: Optional[str] = ""
    source: Optional[str] = ""
    short_notes: Optional[str] = ""
    file_info: Optional[Dict[str, Any]] = None
    source_type: str

class ConfirmCreateRequest(BaseModel):
    candidate_data: CandidateDataInput
    force_create_duplicate: bool = False

class ConfirmCreateResponse(BaseModel):
    id: str
    notion_record_id: str
    notion_url: HttpUrl
    message: str

class DuplicateCheckResponse(BaseModel):
    exists: bool
    candidate_id: Optional[str] = None
    notion_url: Optional[HttpUrl] = None

# --- Funciones de Extracción y Auxiliares ---

async def extract_text(file_content: bytes, filename: str) -> str:
    # ... (igual que antes) ...
    text = ""
    try:
        if filename.lower().endswith('.pdf'):
            reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text: text += page_text + "\n"
        elif filename.lower().endswith(('.docx')): # .doc puede requerir unoconv o similar
            doc = Document(io.BytesIO(file_content))
            text = "\n".join(para.text for para in doc.paragraphs if para.text)
        else:
             logger.warning(f"Formato de archivo no oficialmente soportado para extracción de texto: {filename}")
             # Intentar leer como texto plano como último recurso?
             try: text = file_content.decode('utf-8', errors='ignore')
             except: text = ""
             # raise ValueError("Formato de archivo no soportado")
        logger.info(f"Texto extraído de {filename} ({len(text)} caracteres)")
        return text.strip()
    except PyPDF2.errors.PdfReadError as pdf_err:
        logger.error(f"Error leyendo PDF: {filename}. Detalles: {pdf_err}")
        raise HTTPException(status_code=400, detail=f"El archivo PDF está corrupto o protegido. {pdf_err}")
    except Exception as e:
        logger.exception(f"Error inesperado extrayendo texto de {filename}") # Loggear traceback
        raise HTTPException(status_code=500, detail=f"Error interno extrayendo texto: {e}")


async def extract_linkedin_from_text_robust(text_content: str) -> str:
    # ... (igual que antes) ...
    patterns = [
        r'(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_%/]+)',
        r'linkedin\.com/in/[a-zA-Z0-9\-_%/]+'
    ]
    urls_found = set()
    for pattern in patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            url = match
            if not url.startswith('http'): url = f"https://{url}"
            if "linkedin.com/in/" in url:
                clean_url = url.split('?')[0].split('#')[0]
                if not clean_url.endswith('/'): clean_url += '/'
                urls_found.add(clean_url)
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
    except Exception: pass
    if urls_found:
        best_url = list(urls_found)[0]
        logger.info(f"LinkedIn encontrado por Regex/BS: {best_url}")
        return best_url
    logger.info("LinkedIn no encontrado por Regex/BS.")
    return ""

async def extract_data_with_ai(text_content: str) -> ExtractedData:
    """Realiza UNA llamada a OpenAI con prompt mejorado (v5.1)."""
    if not openai_client:
        raise HTTPException(status_code=503, detail="Servicio de IA no configurado.")

    logger.info("Iniciando extracción con IA (Prompt v5.1)...")
    linkedin_url_direct = await extract_linkedin_from_text_robust(text_content)

    # Prompt Refinado (v5.1)
    prompt = f"""
    Eres un asistente experto en RRHH para extraer información clave de CVs y perfiles de LinkedIn. Devuelve un objeto JSON con los siguientes campos:

    TEXTO:
    ```{text_content[:7000]}```

    CAMPOS A EXTRAER (rellena con "" o [] si no encuentras información):
    - nombre_apellido: Nombre completo. IMPORTANTE: EXCLUYE títulos (Sr., Sra., Dr., Lic., PhD, Ing., Prof., Mr., Mrs., Ms.). Solo Nombre Apellido1 Apellido2.
    - email: Email principal.
    - phone: Teléfono principal. Límpialo (solo dígitos) y añade prefijo internacional si es posible (ej: +34, +1). Formato final: +[prefijo][número]. Si no hay prefijo claro, solo dígitos.
    - location: Ciudad y País actual. Formato: "Ciudad, País".
    - linkedin_url: URL COMPLETA del perfil (https://linkedin.com/in/...). Busca links INCUSTADOS en texto (ej. la palabra 'LinkedIn'). Prioriza la URL pre-detectada si es válida, pero busca en el texto por si hay una mejor. Limpia parámetros (?trk=...).
    - current_company: Empresa más reciente (busca "Presente", "Actual", "Current"). Si es autónomo, responde "Freelance". Solo nombre de empresa, SIN PUESTO.
    - skills: Lista MÁXIMO 8 skills/roles clave (ej: ["Java", "React", "AWS", "Project Management", "SQL"]). Generaliza puestos.
    - languages: Lista MÁXIMO 5 idiomas con nivel si se indica claramente (ej: ["Spanish C2 Native", "English B2"]). Usa niveles MCER (A1-C2 Native) si puedes.
    - gender: Infiere "Male" o "Female" del nombre. Si es ambiguo, pon "".

    URL PRE-DETECTADA (Considera esta): {linkedin_url_direct or "Ninguna"}

    RESPONDE SOLO CON EL JSON. SIN ```json, SIN comentarios.
    {{ "nombre_apellido": "...", "email": "...", "phone": "...", ... }}
    """
    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini", # Modelo eficiente
            messages=[
                {"role": "system", "content": "Extrae datos de CV/LinkedIn en formato JSON solicitado."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=1500 # Aumentado ligeramente por si acaso
        )
        json_response = response.choices[0].message.content
        logger.info(f"Respuesta JSON de IA: {json_response}")
        try: data = json.loads(json_response)
        except json.JSONDecodeError as json_err:
             logger.error(f"IA devolvió JSON inválido: {json_err}. Respuesta: {json_response}")
             raise HTTPException(status_code=500, detail="La IA devolvió una respuesta JSON inválida.") from json_err

        # Validar y limpiar con Pydantic (aplica validadores como clean_name, etc.)
        extracted = ExtractedData(**data)

        # Lógica final LinkedIn
        if not extracted.linkedin_url and linkedin_url_direct:
            validated_direct_url = ExtractedData(linkedin_url=linkedin_url_direct).linkedin_url
            if validated_direct_url:
                extracted.linkedin_url = validated_direct_url
                logger.info("Usando URL LinkedIn pre-detectada validada.")

        # Validar Idiomas contra Notion Options
        valid_languages = []
        if extracted.languages:
            lang_options_lower = {opt.lower(): opt for opt in NOTION_OPTIONS["LANGUAJE"]}
            for lang in extracted.languages:
                lang_lower = lang.lower()
                if lang_lower in lang_options_lower: valid_languages.append(lang_options_lower[lang_lower])
                else:
                     parts = lang.split(); base_lang_lower = parts[0].lower() if parts else ""
                     if base_lang_lower:
                         best_match = next((opt for opt in NOTION_OPTIONS["LANGUAJE"] if opt.lower().startswith(base_lang_lower)), None)
                         if best_match and best_match not in valid_languages: valid_languages.append(best_match)
        extracted.languages = list(set(valid_languages))[:5]

        logger.info("Extracción con IA completada y validada.")
        return extracted

    except HTTPException: raise
    except Exception as e:
        logger.exception("Error inesperado en extracción con IA")
        raise HTTPException(status_code=500, detail=f"Error interno durante extracción IA: {e}")

async def check_email_duplicate(email: Optional[EmailStr]) -> DuplicateCheckResponse:
    if not email: return DuplicateCheckResponse(exists=False)
    if not db: # Verificar si la conexión a DB falló al inicio
        logger.warning("No se puede comprobar duplicado, conexión a DB no disponible.")
        return DuplicateCheckResponse(exists=False) # Asumir no existencia
    try:
        email_str = str(email).strip().lower()
        existing = await db.candidates.find_one({"email": email_str}, {"_id": 1, "notion_url": 1})
        if existing:
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
    # ... (igual que antes, usando file.io) ...
     try:
        # Añadir header para prevenir análisis de contenido que puede dar errores
        headers = {'User-Agent': 'ATS Uploader/1.0'}
        files = {'file': (filename, file_content)}
        # Usar file.io con expiración corta (ej. 1 día)
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post('[https://file.io?expires=1d](https://file.io?expires=1d)', files=files, headers=headers)

            if response.status_code == 200:
                data = response.json()
                file_url = data.get('link')
                if file_url:
                    logger.info(f"Archivo subido a file.io: {file_url}")
                    return file_url
                else:
                    logger.error(f"file.io devolvió 200 pero sin link: {data}")
                    return ""
            else:
                 logger.error(f"Error subiendo a file.io ({response.status_code}): {response.text}")
                 return ""
     except Exception as e:
        logger.error(f"Excepción en upload_cv_to_external_service: {e}")
        return ""


async def create_notion_page(candidate_data: CandidateDataInput) -> tuple[str, str]:
    """Crea la página en Notion, incluyendo el adjunto."""
    if not NOTION_API_TOKEN or not NOTION_DATABASE_ID:
         raise HTTPException(status_code=503, detail="Configuración de Notion incompleta.")

    logger.info(f"Creando registro en Notion para: {candidate_data.nombre_apellido}")
    headers = {"Authorization": f"Bearer {NOTION_API_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    properties = {"Nombre y Apellido": {"title": [{"text": {"content": candidate_data.nombre_apellido or "Nombre no extraído"}}]}}
    data_dict = candidate_data.dict(exclude_unset=True)

    # Mapeo robusto
    if data_dict.get("email"): properties["Email"] = {"email": str(data_dict["email"])}
    if data_dict.get("phone"): properties["Phone"] = {"phone_number": data_dict["phone"]}
    if data_dict.get("location"): properties["LOCATION"] = {"rich_text": [{"text": {"content": data_dict["location"]}}]}
    if data_dict.get("linkedin_url"): properties["LINKEDIN URL"] = {"url": str(data_dict["linkedin_url"])}
    if data_dict.get("current_company"): properties["CURRENT COMPANY"] = {"rich_text": [{"text": {"content": data_dict["current_company"]}}]}
    if data_dict.get("salary_expectation"): properties["SALARY EXPECTATION"] = {"rich_text": [{"text": {"content": data_dict["salary_expectation"]}}]}
    if data_dict.get("short_notes"): properties["SHORT NOTES"] = {"rich_text": [{"text": {"content": data_dict["short_notes"]}}]}
    if data_dict.get("stage") in NOTION_OPTIONS["STAGE"]: properties["STAGE"] = {"select": {"name": data_dict["stage"]}}
    if data_dict.get("resolution") in NOTION_OPTIONS["RESOLUTION"]: properties["RESOLUTION"] = {"select": {"name": data_dict["resolution"]}}
    if data_dict.get("rejection_reason") in NOTION_OPTIONS["REJECTION_REASON"]: properties["REJECTION REASON"] = {"select": {"name": data_dict["rejection_reason"]}}
    if data_dict.get("source") in NOTION_OPTIONS["SOURCE"]: properties["SOURCE"] = {"select": {"name": data_dict["source"]}}
    if data_dict.get("gender") in NOTION_OPTIONS["GENDER"]: properties["Gender"] = {"select": {"name": data_dict["gender"]}}
    if data_dict.get("skills"): valid_skills = [s for s in data_dict["skills"] if s][:10]; properties["Skills"] = {"multi_select": [{"name": s} for s in valid_skills]}
    if data_dict.get("languages"): valid_langs = [l for l in data_dict["languages"] if l in NOTION_OPTIONS["LANGUAJE"]]; properties["LANGUAJE"] = {"multi_select": [{"name": l} for l in valid_langs]}

    # Adjunto
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
            else: logger.warning(f"No se pudo obtener URL para adjunto {filename}.")
        except Exception as e: logger.error(f"Error procesando/subiendo adjunto: {e}")

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    # logger.debug(f"Payload Notion: {json.dumps(payload, indent=2)}") # Descomentar para debug extremo

    try:
        async with httpx.AsyncClient(timeout=45.0) as client: # Timeout aumentado
            response = await client.post("[https://api.notion.com/v1/pages](https://api.notion.com/v1/pages)", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Notion OK: ID={data['id']}")
            return data['id'], data['url']
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        logger.error(f"Error API Notion ({e.response.status_code}): {error_body}")
        detail = f"Error Notion ({e.response.status_code})"
        try: detail += f": {e.response.json().get('message', error_body)}"
        except Exception: detail += f": {error_body}"
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except Exception as e:
        logger.exception("Error inesperado creando página Notion")
        raise HTTPException(status_code=500, detail=f"Error interno Notion: {e}")

# --- API Endpoints ---
api_router = APIRouter(prefix="/api")

@api_router.get("/options", response_model=Dict[str, List[str]])
async def get_notion_options_endpoint_v3(): # Renombrado por claridad
    options = {}
    selects = ["STAGE", "RESOLUTION", "REJECTION_REASON", "SOURCE", "GENDER"]
    for key, values in NOTION_OPTIONS.items():
        if key in selects: options[key] = [""] + values
        else: options[key] = sorted(values) # LANGUAJE ordenado
    return options

@api_router.post("/process", response_model=Dict) # Ajustar response model si es necesario
async def process_source_endpoint_v3(file: Optional[UploadFile] = File(None), linkedin_url: Optional[HttpUrl] = Form(None)): # Renombrado por claridad
    # ... (lógica igual que antes, solo asegurar que devuelve dict como espera el frontend) ...
    if not file and not linkedin_url: raise HTTPException(status_code=400, detail="Proporciona CV o URL.")
    if file and linkedin_url: raise HTTPException(status_code=400, detail="Proporciona solo CV o URL.")

    text_content = ""; source_type = ""; file_info = None

    try:
        if file:
            logger.info(f"Procesando CV: {file.filename}")
            if not file.filename or not file.filename.lower().endswith(('.pdf', '.docx', '.doc')): raise HTTPException(status_code=400, detail="Formato inválido.")
            file_content = await file.read()
            text_content = await extract_text(file_content, file.filename)
            source_type = "cv"
            file_info = {"filename": file.filename, "content_base64": base64.b64encode(file_content).decode('utf-8'), "size": len(file_content)}
        elif linkedin_url:
            logger.info(f"Procesando LinkedIn: {linkedin_url}")
            text_content = await extract_linkedin_data(str(linkedin_url))
            source_type = "linkedin"

        if not text_content or not text_content.strip(): raise HTTPException(status_code=400, detail="No se pudo obtener texto.")

        extracted_data = await extract_data_with_ai(text_content)

        if source_type == "linkedin" and not extracted_data.linkedin_url:
             validated_original_url = ExtractedData(linkedin_url=str(linkedin_url)).linkedin_url
             if validated_original_url: extracted_data.linkedin_url = validated_original_url

        duplicate_info = await check_email_duplicate(extracted_data.email)

        extracted_dict = extracted_data.dict()
        if extracted_dict.get("linkedin_url"): extracted_dict["linkedin_url"] = str(extracted_dict["linkedin_url"])
        duplicate_dict = duplicate_info.dict()
        if duplicate_dict.get("notion_url"): duplicate_dict["notion_url"] = str(duplicate_dict["notion_url"])

        return {
            "extracted_data": extracted_dict,
            "file_info": file_info,
            "source_type": source_type,
            "duplicate_info": duplicate_dict
        }
    # ... (Manejo de errores igual que antes) ...
    except HTTPException as e: logger.error(f"HTTPException en /process: {e.detail}"); raise e
    except Exception as e: logger.exception("Error inesperado en /process"); raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@api_router.post("/candidates/confirm-create", response_model=ConfirmCreateResponse)
async def confirm_create_endpoint_v4(request: ConfirmCreateRequest): # Renombrado por claridad
    # ... (lógica igual que antes, solo asegurar que usa HttpUrl correctas) ...
    candidate_input = request.candidate_data
    logger.info(f"Confirmando: {candidate_input.nombre_apellido} Email: {candidate_input.email} Force: {request.force_create_duplicate}")

    if not request.force_create_duplicate and candidate_input.email:
        duplicate_check = await check_email_duplicate(candidate_input.email)
        if duplicate_check.exists:
            logger.warning(f"Intento crear duplicado: {candidate_input.email}")
            raise HTTPException(status_code=409, detail=f"Email duplicado: {candidate_input.email}. Confirma para crearlo.")

    try:
        notion_id, notion_url_str = await create_notion_page(candidate_input)
        try: notion_url = HttpUrl(notion_url_str, scheme="https")
        except Exception: notion_url = HttpUrl("[https://www.notion.so](https://www.notion.so)", scheme="https") # Fallback
    except HTTPException as e: raise e
    except Exception as e: logger.exception("Error llamando a create_notion_page"); raise HTTPException(status_code=500, detail=f"Error interno Notion: {e}")

    mongo_id = str(uuid.uuid4())
    candidate_doc = {"_id": mongo_id, "notion_record_id": notion_id, "notion_url": str(notion_url), "created_at": datetime.now(timezone.utc), **candidate_input.dict(exclude={'file_info'})}
    if candidate_doc.get("email"): candidate_doc["email"] = str(candidate_doc["email"]).lower()
    if candidate_doc.get("linkedin_url"): candidate_doc["linkedin_url"] = str(candidate_doc["linkedin_url"])

    if db: # Guardar solo si la conexión a DB está activa
        try: await db.candidates.insert_one(candidate_doc) ; logger.info(f"Guardado en MongoDB: {mongo_id}")
        except Exception as e: logger.error(f"Error guardando en MongoDB (NotionID: {notion_id}): {e}")
    else: logger.warning("No se guardó en MongoDB, conexión no disponible.")


    return ConfirmCreateResponse(id=mongo_id, notion_record_id=notion_id, notion_url=notion_url, message="Candidato creado con éxito.")

# --- Inicialización de la App ---
app = FastAPI( title="ATS Babel - CV Processor v5.2", version="5.2.0") # Incremento versión

app.add_middleware( CORSMiddleware, allow_origins=CORS_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(api_router)

@app.get("/", include_in_schema=False)
async def root_v5_2(): return {"message": "ATS API v5.2 running."}

@app.on_event("startup")
async def startup_event():
     # Verificar conexión a MongoDB al inicio de forma asíncrona
     if db:
         try:
             await db.command('ping')
             logger.info("Conexión a MongoDB verificada con éxito al inicio.")
         except Exception as e:
             logger.error(f"Ping a MongoDB falló al inicio: {e}. La comprobación de duplicados puede fallar.")
             # No marcamos db = None aquí, podría recuperarse. Los endpoints lo verificarán.
     else:
         logger.error("No se intentará ping a MongoDB, la conexión inicial falló.")


@app.on_event("shutdown")
async def shutdown_db_client_v5_2(): mongo_client.close(); logger.info("Conexión MongoDB cerrada.")

# === FIN main.py ===
