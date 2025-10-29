import os
import logging
import io
import re
import json
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Body
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
try:
    MONGO_URL = os.environ['MONGO_URL']
    DB_NAME = os.environ['DB_NAME']
    OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
    NOTION_API_TOKEN = os.environ['NOTION_API_TOKEN']
    NOTION_DATABASE_ID = os.environ['NOTION_DATABASE_ID']
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', 'http://localhost:3000').split(',')
except KeyError as e:
    logger.error(f"Error crítico: Falta la variable de entorno {e}. La aplicación no puede iniciar.")
    # Permite que Render inicie, pero fallará si se usan las claves
    pass

# --- Clientes ---
mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client[DB_NAME]
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# --- Opciones de Notion ---
NOTION_OPTIONS = {
    "STAGE": ["Lead", "InMail", "Application", "Waiting CV / ITV", "Babel Screening", "Interview Babel", "Submitted", "In Process Client", "HR Client's ITV", "Technical Client's ITV", "Meet the Team", "2nd Client's ITV", "Offer", "Hired"],
    "RESOLUTION": ["On Hold", "Waiting CV", "Procesando", "Pending Reply", "ITV Scheduled", "Babel Rejected", "Client Rejected in Submition", "Client Rejected", "Withdrawn Application", "TO REJECT", "TO PRESENT", "TO CONTACT", "Closed Job", "Hired"],
    "REJECTION_REASON": ["Academic Background", "Already interviewed by client", "Cultural Fit", "Failed Languaje Test", "Failed Technical Screening Questions", "Failed Technical Test", "Freelance/Contractor", "Job Jumper", "Need VISA/PAC/Sponsorship", "No response", "No Show", "Not Interested", "Otro", "Salary Expectation", "Sobrecalificado", "Technical Skills / Not right experience", "Work Model"],
    "LANGUAJE": ["English A1", "English A2", "English B1", "English B2", "English C1", "English C2 Native", "Spanish B2", "Spanish C1", "Spanish C2 Native", "Catalan B1", "Catalan B2", "Catalan C1", "Catalan C2 Native", "French B1", "French B2", "French C1", "French C2 Native", "German A2", "German B2", "German C1", "German C2 Native", "Arabic C2 Native", "Bulgaro C2 Nativo", "Chinese B1", "Chinese B2", "Chinese C2 Native", "Dutch B2", "Dutch C2 Native", "Greek C2 Native", "Hebrew C2 Native", "Italian C2 Native", "Italiano B2", "Italiano C1", "Italian C2 Nativo", "Lithuanian C2 Native", "Polish C2 Native", "Portugues B1", "Portugues B2", "Portugues C1", "Portuguese C2 Native", "Russian C2 Native", "Turco C2 Nativo", "Turkish C2 Nativo", "Ukranian C2 Native", "Romanian C2 Native"],
    "SOURCE": ["Clay", "Linkedin JOB POST", "JOIN Multiposting", "Linkedin Personal", "PitchMe", "Recruitly", "Referral", "Sourced on LinkedIn", "People GPT", "Greenhouse", "SoftGarden"],
    "GENDER": ["Male", "Female"],
}

# --- Modelos Pydantic ---
class ExtractedData(BaseModel):
    nombre_apellido: Optional[str] = ""
    email: Optional[EmailStr] = ""
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[HttpUrl] = None # Permitir None
    current_company: Optional[str] = ""
    skills: List[str] = []
    languages: List[str] = []
    gender: Optional[str] = ""

    @validator('phone')
    def format_phone(cls, v):
        if not v: return ""
        clean_phone = re.sub(r'[^\d+]', '', v)
        if clean_phone.startswith('+') and len(clean_phone) > 1:
             digits_only = re.sub(r'\D', '', clean_phone[1:])
             return f"+{digits_only}"
        else:
            digits_only = re.sub(r'\D', '', clean_phone)
            if len(digits_only) == 9 and digits_only[0] in '6789':
                 return f"+34{digits_only}"
            return digits_only

    @validator('linkedin_url', pre=True)
    def format_linkedin(cls, v):
        if not v: return None
        if isinstance(v, str):
            if not v.startswith(('http://', 'https://')):
                v = f"https://{v}"
            if "linkedin.com/in/" in v:
                v = v.split('?')[0]
                if not v.endswith('/'):
                     v += '/'
                try:
                    # Validar formalmente como HttpUrl
                    return HttpUrl(v, scheme="https")
                except Exception:
                    logger.warning(f"URL LinkedIn formateada ({v}) no es válida.")
                    return None # Retornar None si no valida
        return None # Retornar None si no es string o no es de LinkedIn


class ProcessRequest(BaseModel):
    linkedin_url: Optional[HttpUrl] = None

class CandidateDataInput(BaseModel):
    nombre_apellido: str
    email: Optional[EmailStr] = ""
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[HttpUrl] = None # Permitir None
    current_company: Optional[str] = ""
    skills: List[str] = []
    languages: List[str] = []
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

# --- Funciones de Extracción ---

async def extract_text(file_content: bytes, filename: str) -> str:
    try:
        if filename.lower().endswith('.pdf'):
            reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            return "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
        elif filename.lower().endswith(('.docx', '.doc')):
            doc = Document(io.BytesIO(file_content))
            return "\n".join(para.text for para in doc.paragraphs if para.text)
        else:
            raise ValueError("Formato de archivo no soportado")
    except Exception as e:
        logger.error(f"Error extrayendo texto de {filename}: {e}")
        raise HTTPException(status_code=400, detail=f"No se pudo extraer texto del archivo: {e}")

async def extract_linkedin_from_text(text_content: str) -> str:
    patterns = [
        r'(https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_]{3,}/?)',
        r'(?:LinkedIn|linkedin)[:\s]*((?:https?://)?(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_]{3,}/?)'
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            url = match[0] if isinstance(match, tuple) else match
            if url and "linkedin.com/in/" in url:
                if not url.startswith('http'): url = f"https://{url}"
                return url.split('?')[0] # Limpiar parámetros
    try:
        soup = BeautifulSoup(text_content, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if "linkedin.com/in/" in href:
                 if not href.startswith('http'): href = f"https://{href}"
                 return href.split('?')[0] # Limpiar parámetros
    except Exception: pass
    return ""

async def extract_data_with_ai(text_content: str) -> ExtractedData:
    logger.info("Iniciando extracción con IA...")
    linkedin_url_direct = await extract_linkedin_from_text(text_content)
    prompt = f"""
    Analiza el siguiente texto (puede ser un CV o un perfil de LinkedIn) y extrae la información solicitada en formato JSON.
    TEXTO:
    ```{text_content[:4000]}```
    EXTRAE LOS SIGUIENTES CAMPOS:
    - nombre_apellido: Nombre completo. SIN títulos (Sr., Sra., Dr., Lic.), SIN tratamientos. Si no lo encuentras, pon "".
    - email: Email principal. Si no lo encuentras, pon "".
    - phone: Número de teléfono principal. Intenta incluir el prefijo internacional si es obvio. Limpia espacios/puntos. Si no lo encuentras, pon "".
    - location: Ciudad y País (ej: "Madrid, España"). Si no lo encuentras, pon "".
    - linkedin_url: URL del perfil de LinkedIn (formato https://linkedin.com/in/...). Si encuentras varias, la más probable. Si ya te proporciono una abajo, usa esa. Si no encuentras ninguna, pon "".
    - current_company: Nombre de la empresa más reciente o "Freelance"/"Contractor" si aplica. SIN puesto. Si no lo encuentras, pon "".
    - skills: Lista de MÁXIMO 8 skills técnicas o roles clave (ej: ["Java", "Python", "React", "Project Manager", "AWS", "SQL Server"]). Generaliza roles (ej: "Software Engineer").
    - languages: Lista de idiomas mencionados y su nivel si se especifica (ej: ["Spanish C2 Native", "English B2"]). Usa los niveles A1, A2, B1, B2, C1, C2 Native si es posible. Máximo 5.
    - gender: "Male" o "Female", inferido principalmente del nombre. Si es ambiguo o no se puede determinar, pon "".
    URL DE LINKEDIN PRE-DETECTADA (si existe, úsala): {linkedin_url_direct or "No pre-detectada"}
    RESPONDE ÚNICAMENTE CON EL OBJETO JSON, SIN EXPLICACIONES ADICIONALES.
    {{
        "nombre_apellido": "...", "email": "...", "phone": "...", "location": "...", "linkedin_url": "...",
        "current_company": "...", "skills": [...], "languages": [...], "gender": "..."
    }}
    """
    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto extrayendo datos estructurados de CVs y perfiles."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        json_response = response.choices[0].message.content
        logger.info(f"Respuesta JSON de IA: {json_response}")
        data = json.loads(json_response)
        extracted = ExtractedData(**data) # Validar/Limpiar con Pydantic

        # Lógica LinkedIn mejorada
        if not extracted.linkedin_url and linkedin_url_direct:
             extracted.linkedin_url = ExtractedData(linkedin_url=linkedin_url_direct).linkedin_url # Validar URL directa
        elif extracted.linkedin_url: # Si la IA encontró una, re-validarla
             extracted.linkedin_url = ExtractedData(linkedin_url=str(extracted.linkedin_url)).linkedin_url


        # Validar Idiomas contra Notion Options
        valid_languages = []
        if extracted.languages:
            for lang in extracted.languages:
                found = False
                for option in NOTION_OPTIONS["LANGUAJE"]:
                    if lang.lower() == option.lower():
                        valid_languages.append(option)
                        found = True
                        break
                if not found:
                     parts = lang.split()
                     if len(parts) > 0:
                          base_lang = parts[0]
                          for option in NOTION_OPTIONS["LANGUAJE"]:
                              if option.lower().startswith(base_lang.lower()):
                                   valid_languages.append(option)
                                   break
        extracted.languages = list(set(valid_languages))[:5]

        logger.info("Extracción con IA completada y validada.")
        return extracted

    except json.JSONDecodeError as e:
        logger.error(f"Error decodificando JSON de IA: {e}. Respuesta: {json_response}")
        raise HTTPException(status_code=500, detail="Error procesando respuesta de la IA.")
    except Exception as e:
        logger.error(f"Error inesperado en extracción con IA: {e}")
        raise HTTPException(status_code=500, detail=f"Error inesperado durante la extracción IA: {e}")


async def check_email_duplicate(email: Optional[str]) -> DuplicateCheckResponse:
    if not email: return DuplicateCheckResponse(exists=False)
    try:
        existing = await db.candidates.find_one({"email": email.strip().lower()}, {"_id": 1, "notion_url": 1})
        if existing:
            return DuplicateCheckResponse(exists=True, candidate_id=str(existing["_id"]), notion_url=existing.get("notion_url"))
        return DuplicateCheckResponse(exists=False)
    except Exception as e:
        logger.error(f"Error comprobando duplicado de email {email}: {e}")
        return DuplicateCheckResponse(exists=False)

async def upload_cv_to_external_service(file_content: bytes, filename: str) -> str:
    """Sube el CV a un servicio externo temporal (file.io) y retorna la URL."""
    try:
        files = {'file': (filename, file_content)}
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post('https://file.io', files=files)
            if response.status_code == 200:
                data = response.json()
                file_url = data.get('link')
                if file_url:
                    logger.info(f"Archivo subido a servicio externo: {file_url}")
                    return file_url
            logger.error(f"Error subiendo a file.io ({response.status_code}): {response.text}")
            return "" # Retornar vacío para no bloquear
    except Exception as e:
        logger.error(f"Error en upload_cv_to_external_service: {e}")
        return ""

async def create_notion_page(candidate_data: CandidateDataInput) -> tuple[str, str]:
    """Crea la página en Notion, incluyendo el adjunto si existe."""
    logger.info(f"Creando registro en Notion para: {candidate_data.nombre_apellido}")
    headers = {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    properties = {
        "Nombre y Apellido": {"title": [{"text": {"content": candidate_data.nombre_apellido}}]},
    }
    if candidate_data.email: properties["Email"] = {"email": candidate_data.email}
    if candidate_data.phone: properties["Phone"] = {"phone_number": candidate_data.phone}
    if candidate_data.location: properties["LOCATION"] = {"rich_text": [{"text": {"content": candidate_data.location}}]}
    # Asegurar que la URL se envía como string
    if candidate_data.linkedin_url: properties["LINKEDIN URL"] = {"url": str(candidate_data.linkedin_url)}
    if candidate_data.current_company: properties["CURRENT COMPANY"] = {"rich_text": [{"text": {"content": candidate_data.current_company}}]}
    if candidate_data.salary_expectation: properties["SALARY EXPECTATION"] = {"rich_text": [{"text": {"content": candidate_data.salary_expectation}}]}
    if candidate_data.short_notes: properties["SHORT NOTES"] = {"rich_text": [{"text": {"content": candidate_data.short_notes}}]}
    if candidate_data.stage and candidate_data.stage in NOTION_OPTIONS["STAGE"]: properties["STAGE"] = {"select": {"name": candidate_data.stage}}
    if candidate_data.resolution and candidate_data.resolution in NOTION_OPTIONS["RESOLUTION"]: properties["RESOLUTION"] = {"select": {"name": candidate_data.resolution}}
    if candidate_data.rejection_reason and candidate_data.rejection_reason in NOTION_OPTIONS["REJECTION_REASON"]: properties["REJECTION REASON"] = {"select": {"name": candidate_data.rejection_reason}}
    if candidate_data.source and candidate_data.source in NOTION_OPTIONS["SOURCE"]: properties["SOURCE"] = {"select": {"name": candidate_data.source}}
    if candidate_data.gender and candidate_data.gender in NOTION_OPTIONS["GENDER"]: properties["Gender"] = {"select": {"name": candidate_data.gender}}
    if candidate_data.skills: properties["Skills"] = {"multi_select": [{"name": skill} for skill in candidate_data.skills[:10]]}
    if candidate_data.languages:
        valid_langs = [lang for lang in candidate_data.languages if lang in NOTION_OPTIONS["LANGUAJE"]]
        if valid_langs: properties["LANGUAJE"] = {"multi_select": [{"name": lang} for lang in valid_langs]}

    # --- INICIO: LÓGICA DE ADJUNTO ---
    if candidate_data.file_info and candidate_data.file_info.get("content_base64"):
        logger.info("Procesando adjunto para Notion...")
        try:
            # Decodificar el archivo que viene del frontend
            file_content = base64.b64decode(candidate_data.file_info["content_base64"])
            filename = candidate_data.file_info["filename"]
            logger.info(f"Adjunto: {filename}, Tamaño: {len(file_content)} bytes")

            # Subirlo a un servicio externo para obtener URL
            file_url = await upload_cv_to_external_service(file_content, filename)

            if file_url:
                # Añadir la propiedad ATTACHMENT al payload de Notion
                properties["ATTACHMENT"] = {
                    "files": [{
                        "type": "external",
                        "name": filename,
                        "external": {"url": file_url}
                    }]
                }
                logger.info(f"Propiedad ATTACHMENT añadida para {filename}")
            else:
                logger.warning(f"No se pudo obtener URL para el adjunto {filename}, se creará sin él.")

        except base64.binascii.Error as e:
             logger.error(f"Error decodificando base64 del adjunto: {e}")
        except Exception as e:
            logger.error(f"Error procesando/subiendo adjunto para Notion: {e}")
            # Continuar sin adjunto si falla la subida/decodificación
    # --- FIN: LÓGICA DE ADJUNTO ---

    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    logger.debug(f"Payload final para Notion API: {json.dumps(payload, indent=2)}") # Log detallado (cuidado con datos sensibles)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post("https://api.notion.com/v1/pages", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Registro creado en Notion exitosamente: ID={data['id']}")
            return data['id'], data['url']
    except httpx.HTTPStatusError as e:
        logger.error(f"Error API Notion ({e.response.status_code}): {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"Error de API Notion: {e.response.text}")
    except Exception as e:
        logger.error(f"Error inesperado creando página en Notion: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno al crear registro en Notion: {e}")

# --- API Endpoints ---
api_router = APIRouter(prefix="/api")

@api_router.get("/options")
async def get_notion_options_endpoint():
    return {k: [""] + v if k != "LANGUAJE" else v for k, v in NOTION_OPTIONS.items()}

@api_router.post("/process")
async def process_source_endpoint(file: Optional[UploadFile] = File(None), linkedin_url: Optional[HttpUrl] = Form(None)):
    if not file and not linkedin_url:
        raise HTTPException(status_code=400, detail="Debe proporcionar un archivo CV o una URL de LinkedIn.")
    if file and linkedin_url:
        raise HTTPException(status_code=400, detail="Proporcione solo un archivo CV o una URL de LinkedIn, no ambos.")

    text_content = ""
    source_type = ""
    file_info = None

    try:
        if file:
            logger.info(f"Procesando archivo CV: {file.filename}")
            if not file.filename.lower().endswith(('.pdf', '.docx', '.doc')):
                 raise HTTPException(status_code=400, detail="Formato de archivo no permitido. Usar PDF, DOCX o DOC.")
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
            text_content = await extract_linkedin_data(str(linkedin_url))
            source_type = "linkedin"

        if not text_content or not text_content.strip():
             raise HTTPException(status_code=400, detail="No se pudo obtener contenido textual de la fuente.")

        extracted_data = await extract_data_with_ai(text_content)

        if source_type == "linkedin" and not extracted_data.linkedin_url:
             extracted_data.linkedin_url = linkedin_url

        duplicate_info = await check_email_duplicate(extracted_data.email)

        # Convertir HttpUrl a string antes de devolver JSON si no es None
        extracted_dict = extracted_data.dict()
        if extracted_dict.get("linkedin_url"):
             extracted_dict["linkedin_url"] = str(extracted_dict["linkedin_url"])

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
async def confirm_create_endpoint_v2(request: ConfirmCreateRequest):
    candidate_input = request.candidate_data
    logger.info(f"Confirmando creación para: {candidate_input.nombre_apellido} (Email: {candidate_input.email})")

    if not request.force_create_duplicate:
        duplicate_check = await check_email_duplicate(candidate_input.email)
        if duplicate_check.exists:
            logger.warning(f"Intento de crear duplicado (email: {candidate_input.email}) sin forzar.")
            raise HTTPException(
                status_code=409,
                detail=f"Email duplicado detectado ({candidate_input.email}). Confirma si quieres crearlo igualmente."
            )

    try:
        notion_id, notion_url_str = await create_notion_page(candidate_input)
        # Validar la URL de Notion devuelta
        try:
            notion_url = HttpUrl(notion_url_str, scheme="https")
        except Exception:
             logger.error(f"URL de Notion inválida recibida: {notion_url_str}")
             notion_url = "https://www.notion.so" # URL por defecto o manejo de error


    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception("Error inesperado al llamar a create_notion_page")
        raise HTTPException(status_code=500, detail=f"Error interno al crear en Notion: {e}")

    # Guardar en MongoDB
    candidate_doc = candidate_input.dict()
    candidate_doc["_id"] = str(uuid.uuid4())
    candidate_doc["notion_record_id"] = notion_id
    candidate_doc["notion_url"] = str(notion_url)
    candidate_doc["created_at"] = datetime.now(timezone.utc)
    if candidate_doc.get("linkedin_url"): candidate_doc["linkedin_url"] = str(candidate_doc["linkedin_url"])
    if candidate_doc.get("email"): candidate_doc["email"] = str(candidate_doc["email"]).lower()
    candidate_doc.pop("file_info", None)

    try:
        insert_result = await db.candidates.insert_one(candidate_doc)
        logger.info(f"Candidato guardado en MongoDB con ID: {insert_result.inserted_id}")
    except Exception as e:
        logger.error(f"Error guardando candidato en MongoDB (Notion ID: {notion_id}): {e}")

    return ConfirmCreateResponse(
        id=candidate_doc["_id"],
        notion_record_id=notion_id,
        notion_url=notion_url, # Devolver HttpUrl validada
        message="Candidato creado exitosamente en Notion y MongoDB."
    )

# --- Inicialización de la App ---
app = FastAPI(
    title="ATS Babel - CV Processor v5",
    description="API para procesar CVs/LinkedIn, extraer datos con IA, adjuntar archivos y enviar a Notion.",
    version="5.0.0" # Incremento de versión
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
async def root_v5():
    return {"message": "ATS API v5 is running."}

@app.on_event("shutdown")
async def shutdown_db_client_v5():
    mongo_client.close()
    logger.info("Conexión MongoDB cerrada.")
