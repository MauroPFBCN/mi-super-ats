from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr, HttpUrl
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone
import json
import httpx
import PyPDF2
from docx import Document
import io
import re
from bs4 import BeautifulSoup
import base64
from openai import AsyncOpenAI # <--- CAMBIO IMPORTANTE

# Carga las variables de entorno (claves secretas)
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# --- CAMBIO ARQUITECTÓNICO IMPORTANTE ---
# Reemplazamos el cliente 'emergent' por el cliente oficial de 'openai'
# Esto requiere una clave de API de OpenAI
def get_ai_client():
    return AsyncOpenAI(
        api_key=os.environ.get("OPENAI_API_KEY")
    )
# --- FIN DEL CAMBIO ---

# Create the main app
app = FastAPI(
    title="ATS - Sistema de Gestión de CVs",
    description="Sistema ATS para procesar CVs y perfiles de LinkedIn con integración a Notion",
    version="3.0.0"
)

# Create API router
api_router = APIRouter(prefix="/api")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', 'http://localhost:3000').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Notion Database Options
NOTION_OPTIONS = {
    "STAGE": [
        "Lead", "InMail", "Application", "Waiting CV / ITV", "Babel Screening",
        "Interview Babel", "Submitted", "In Process Client", "HR Client's ITV",
        "Technical Client's ITV", "Meet the Team", "2nd Client's ITV", "Offer", "Hired"
    ],
    "RESOLUTION": [
        "On Hold", "Waiting CV", "Procesando", "Pending Reply", "ITV Scheduled",
        "Babel Rejected", "Client Rejected in Submition", "Client Rejected",
        "Withdrawn Application", "TO REJECT", "TO PRESENT", "TO CONTACT",
        "Closed Job", "Hired"
    ],
    "REJECTION_REASON": [
        "Academic Background", "Already interviewed by client", "Cultural Fit",
        "Failed Languaje Test", "Failed Technical Screening Questions",
        "Failed Technical Test", "Freelance/Contractor", "Job Jumper",
        "Need VISA/PAC/Sponsorship", "No response", "No Show", "Not Interested",
        "Otro", "Salary Expectation", "Sobrecalificado", 
        "Technical Skills / Not right experience", "Work Model"
    ],
    "LANGUAJE": [
        "English A1", "English A2", "English B1", "English B2", "English C1", "English C2 Native",
        "Spanish B2", "Spanish C1", "Spanish C2 Native",
        "Catalan B1", "Catalan B2", "Catalan C1", "Catalan C2 Native",
        "French B1", "French B2", "French C1", "French C2 Native",
        "German A2", "German B2", "German C1", "German C2 Native",
        "Arabic C2 Native", "Bulgaro C2 Nativo", "Chinese B1", "Chinese B2", "Chinese C2 Native",
        "Dutch B2", "Dutch C2 Native", "Greek C2 Native", "Hebrew C2 Native",
        "Italian C2 Native", "Italiano B2", "Italiano C1", "Italian C2 Nativo",
        "Lithuanian C2 Native", "Polish C2 Native", "Portugues B1", "Portugues B2",
        "Portugues C1", "Portuguese C2 Native", "Russian C2 Native", "Turco C2 Nativo",
        "Turkish C2 Nativo", "Ukranian C2 Native", "Romanian C2 Native"
    ],
    "SOURCE": [
        "Clay", "Linkedin JOB POST", "JOIN Multiposting", "Linkedin Personal",
        "PitchMe", "Recruitly", "Referral", "Sourced on LinkedIn", "People GPT",
        "Greenhouse", "SoftGarden"
    ],
    "GENDER": [
        "Male", "Female"
    ],
}

# Pydantic Models
class CandidateCreate(BaseModel):
    nombre_apellido: str
    email: Optional[str] = ""
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[str] = ""
    current_company: Optional[str] = ""
    skills: List[str] = []
    salary_expectation: Optional[str] = ""
    stage: Optional[str] = ""
    resolution: Optional[str] = ""
    rejection_reason: Optional[str] = ""
    languages: List[str] = []
    source: Optional[str] = ""
    gender: Optional[str] = ""
    short_notes: Optional[str] = ""

class CandidateEdit(BaseModel):
    id: str
    nombre_apellido: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    location: Optional[str] = None
    linkedin_url: Optional[str] = None
    current_company: Optional[str] = None
    skills: Optional[List[str]] = None
    salary_expectation: Optional[str] = None
    stage: Optional[str] = None
    resolution: Optional[str] = None
    rejection_reason: Optional[str] = None
    languages: Optional[List[str]] = None
    source: Optional[str] = None
    gender: Optional[str] = None
    short_notes: Optional[str] = None

class CandidateResponse(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    nombre_apellido: str
    email: Optional[str] = ""
    phone: Optional[str] = ""
    location: Optional[str] = ""
    linkedin_url: Optional[str] = ""
    current_company: Optional[str] = ""
    skills: List[str] = []
    salary_expectation: Optional[str] = ""
    stage: Optional[str] = ""
    resolution: Optional[str] = ""
    rejection_reason: Optional[str] = ""
    languages: List[str] = []
    source: Optional[str] = ""
    gender: Optional[str] = ""
    short_notes: Optional[str] = ""
    notion_record_id: Optional[str] = None
    notion_url: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_type: str = "cv"  # "cv" or "linkedin"

class LinkedInProcessRequest(BaseModel):
    linkedin_url: str

class NotionOptionsResponse(BaseModel):
    stages: List[str]
    resolutions: List[str]
    rejection_reasons: List[str]
    languages: List[str]
    sources: List[str]
    genders: List[str]

class EmailCheckResponse(BaseModel):
    exists: bool
    existing_candidate: Optional[CandidateResponse] = None

class ConfirmCandidateRequest(BaseModel):
    candidate_data: dict
    file_info: Optional[dict] = None
    force_create_duplicate: bool = False

# --- FUNCIÓN INTERNA REFACTORIZADA ---
async def _get_ai_response(prompt: str) -> str:
    """Función helper para llamar a la API de OpenAI."""
    try:
        ai_client = get_ai_client()
        response = await ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert at extracting structured data from CVs and resumes."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Error llamando a OpenAI: {str(e)}")
        return "NO_ENCONTRADO"
# --- FIN DE LA FUNCIÓN ---

# Helper Functions
async def extract_text_from_pdf(file_content: bytes) -> str:
    try:
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        return text.strip()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al procesar PDF: {str(e)}")

async def extract_text_from_docx(file_content: bytes) -> str:
    try:
        doc = Document(io.BytesIO(file_content))
        text = ""
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"
        return text.strip()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al procesar DOCX: {str(e)}")

async def extract_linkedin_data(linkedin_url: str) -> str:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        async with httpx.AsyncClient(headers=headers, timeout=10.0) as client:
            response = await client.get(linkedin_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('title')
            title_text = title.text if title else ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            description = meta_desc.get('content', '') if meta_desc else ""
            return f"Título de LinkedIn: {title_text}\n\nDescripción: {description}\n\nURL: {linkedin_url}"
    except Exception as e:
        return f"Perfil de LinkedIn: {linkedin_url}\n\nNota: No se pudo acceder al contenido completo del perfil. Por favor, proporciona los datos manualmente."

async def extract_skills_with_ai(text_content: str) -> List[str]:
    try:
        prompt = f"""
Analiza el siguiente CV y extrae las skills en las categorías específicas con los límites indicados:

TEXTO DEL CV:
{text_content[:3000]}

Instrucciones:
1. **Puestos de Trabajo (máximo 3)**: Generalizar posiciones similares. Por ejemplo, "Manager of Software Development", "Team Lead", "Sr Software Engineer" se resume como "Software Developer"
2. **Lenguajes de Programación (máximo 3)**: Solo lenguajes de programación puros como Java, Python, JavaScript, etc.
3. **Frameworks y Librerías (máximo 3)**: Como React, Spring Boot, Django, etc.
4. **Bases de Datos (máximo 1)**: La base de datos más relevante mencionada

Responde SOLO con una lista JSON en este formato exacto:
{{"puestos": ["Software Developer"], "lenguajes": ["Java", "Python"], "frameworks": ["Spring Boot", "React"], "bases_datos": ["PostgreSQL"]}}
"""
        response = await _get_ai_response(prompt)
        content = response.strip()
        
        if content.startswith('```'):
            content = content.split('```')[1]
            if content.startswith('json'):
                content = content[4:]
        
        skills_data = json.loads(content)
        final_skills = []
        final_skills.extend(skills_data.get('puestos', [])[:3])
        final_skills.extend(skills_data.get('lenguajes', [])[:3])
        final_skills.extend(skills_data.get('frameworks', [])[:3])
        if skills_data.get('bases_datos'):
            final_skills.extend(skills_data['bases_datos'][:1])
        
        logging.info(f"Skills extraídas con IA: {final_skills}")
        return final_skills[:8]
        
    except Exception as e:
        logging.error(f"Error extrayendo skills con IA: {str(e)}")
        return await extract_skills_basic(text_content)

async def extract_skills_basic(text_content: str) -> List[str]:
    skill_keywords = [
        "Java", "Python", "JavaScript", "TypeScript", "React", "Spring Boot",
        "Node.js", "Angular", "Vue.js", "PostgreSQL", "MySQL", "MongoDB",
        "Docker", "Kubernetes", "AWS", "Git", "Scrum", "Agile"
    ]
    found_skills = []
    text_upper = text_content.upper()
    for skill in skill_keywords:
        if skill.upper() in text_upper and skill not in found_skills:
            found_skills.append(skill)
            if len(found_skills) >= 6:
                break
    return found_skills

async def extract_name_advanced(text_content: str) -> str:
    try:
        prompt = f"""
Extrae el nombre completo de la persona de este CV. 
REGLAS IMPORTANTES:
1. NO incluyas títulos académicos (Dr, PhD, Lic, Ing, Prof, etc.)
2. NO incluyas tratamientos (Sr, Sra, Mr, Mrs, etc.) 
3. NO incluyas texto como "Completar manualmente" o "Highly Confidential"
4. Solo el nombre y apellido de la persona
5. Si no puedes identificar un nombre válido, responde "NO_ENCONTRADO"
TEXTO (primeras líneas):
{text_content[:800]}
Responde SOLO con el nombre completo o "NO_ENCONTRADO":
"""
        name = await _get_ai_response(prompt)
        
        if (name == "NO_ENCONTRADO" or 
            len(name) < 3 or 
            any(word in name.lower() for word in ["completar", "confidential", "curriculum", "resume", "cv"])):
            return "Completar manualmente"
        return name
    except Exception as e:
        logging.error(f"Error extrayendo nombre con IA: {str(e)}")
        return "Completar manualmente"

async def extract_phone_advanced(text_content: str) -> str:
    phone_patterns = [
        r'(?:Teléfono|Telefono|Phone|Tel|Móvil|Mobile|Celular)[:.\s]*(\+?[0-9\s\-\(\)\.]{8,20})',
        r'(\+34\s*[6-9][0-9]{2}\s*[0-9]{3}\s*[0-9]{3})',
        r'(\+1\s*[2-9][0-9]{2}\s*[0-9]{3}\s*[0-9]{4})',
        r'(\+[1-9][0-9]{1,3}\s*[0-9]{6,12})',
        r'\b([6-9][0-9]{8})\b',
        r'\b(\([2-9][0-9]{2}\)\s*[0-9]{3}[-\s]*[0-9]{4})\b'
    ]
    for pattern in phone_patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        if matches:
            phone_raw = matches[0].strip()
            clean_phone = re.sub(r'[^\d+]', '', phone_raw)
            if len(clean_phone) >= 9:
                if not phone_raw.startswith('+') and len(clean_phone) == 9 and clean_phone[0] in '6789':
                    return f"+34{clean_phone}"
                elif not phone_raw.startswith('+') and len(clean_phone) == 10:
                    return f"+1{clean_phone}"
                elif clean_phone.startswith('+') and 10 <= len(clean_phone) <= 16:
                    return clean_phone
                elif 9 <= len(clean_phone) <= 15:
                    if clean_phone[0] in '6789' and len(clean_phone) == 9:
                        return f"+34{clean_phone}"
                    else:
                        return f"+{clean_phone}"
    return ""

async def extract_linkedin_advanced(text_content: str) -> str:
    linkedin_patterns = [
        r'(?:https?://)?(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_]{3,}/?',
        r'(?:LinkedIn|linkedin|LINKEDIN)[:.\s]*((?:https?://)?(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_]{3,}/?)',
    ]
    for pattern in linkedin_patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        if matches:
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match[0] else (match[1] if len(match) > 1 else '')
                if 'linkedin.com/in/' in match.lower():
                    url = match if match.startswith('http') else f'https://{match}'
                    return url
    try:
        soup = BeautifulSoup(text_content, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            text = link.get_text().lower()
            if 'linkedin' in text and 'linkedin.com/in/' in href.lower():
                return href if href.startswith('http') else f'https://{href}'
            elif 'linkedin.com/in/' in href.lower():
                return href if href.startswith('http') else f'https://{href}'
        all_elements = soup.find_all(string=re.compile(r'linkedin\.com/in/', re.IGNORECASE))
        for element in all_elements:
            url_match = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_]{3,}/?', str(element), re.IGNORECASE)
            if url_match:
                url = url_match.group(0)
                return url if url.startswith('http') else f'https://{url}'
    except Exception as e:
        logging.debug(f"Error parsing HTML for LinkedIn: {e}")
    
    lines = text_content.split('\n')
    for line in lines:
        line_lower = line.lower()
        if 'linkedin' in line_lower:
            url_match = re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/in/[A-Za-z0-9\-_]{3,}/?', line, re.IGNORECASE)
            if url_match:
                url = url_match.group(0)
                return url if url.startswith('http') else f'https://{url}'
            general_url_match = re.search(r'https?://[^\s]*linkedin[^\s]*', line, re.IGNORECASE)
            if general_url_match:
                return general_url_match.group(0)
    try:
        prompt = f"""
Busca y extrae SOLO la URL de LinkedIn de este texto. Muchas veces está incrustada en la palabra "LinkedIn" o en algún link.
TEXTO:
{text_content[:1500]}
Reglas:
1. Debe ser una URL que contenga "linkedin.com/in/"
2. Responde SOLO con la URL completa o "NO_ENCONTRADO"
3. Si encuentras la URL, asegúrate de que tenga el formato completo https://linkedin.com/in/usuario
"""
        linkedin_url = await _get_ai_response(prompt)
        if linkedin_url != "NO_ENCONTRADO" and 'linkedin.com/in/' in linkedin_url.lower():
            if not linkedin_url.startswith('http'):
                linkedin_url = f'https://{linkedin_url}'
            return linkedin_url
    except Exception as e:
        logging.error(f"Error extrayendo LinkedIn con IA: {str(e)}")
    return ""

async def extract_company_advanced(text_content: str) -> str:
    try:
        prompt = f"""
Analiza este CV y encuentra la empresa ACTUAL donde trabaja la persona.
REGLAS:
1. Busca la experiencia más reciente (actual, presente, current, freelance)
2. NO incluyas universidades, centros educativos o cursos
3. Si es freelance/contractor, responde "Freelance"
4. Solo el nombre de la empresa, sin títulos de puesto
5. Si no encuentras empresa actual, responde "NO_ENCONTRADO"
TEXTO DEL CV:
{text_content[:2000]}
Responde SOLO con el nombre de la empresa actual o "NO_ENCONTRADO":
"""
        company = await _get_ai_response(prompt)
        if company == "NO_ENCONTRADO" or len(company) < 2:
            return ""
        return company
    except Exception as e:
        logging.error(f"Error extrayendo empresa con IA: {str(e)}")
        return ""

async def extract_languages_advanced(text_content: str) -> List[str]:
    detected_languages = []
    text_lower = text_content.lower()
    language_patterns = {
        "English C2 Native": [r'english.*(?:native|c2|nativo|mother\s+tongue)', r'native.*english'],
        "English C1": [r'english.*(?:advanced|c1|fluent|avanzado)', r'(?:advanced|fluent).*english'],
        "English B2": [r'english.*(?:upper.*intermediate|b2|intermediate.*high)', r'intermediate.*english'],
        "English B1": [r'english.*(?:intermediate|b1)', r'basic.*english'],
        "Spanish C2 Native": [r'(?:spanish|español|castellano).*(?:native|nativo|c2|mother\s+tongue)', r'native.*(?:spanish|español)'],
        "Spanish C1": [r'(?:spanish|español|castellano).*(?:advanced|avanzado|c1|fluent)'],
        "Spanish B2": [r'(?:spanish|español|castellano).*(?:intermediate|b2|intermedio)'],
        "Catalan C2 Native": [r'(?:catalan|catalán).*(?:native|nativo|c2)', r'native.*(?:catalan|catalán)'],
        "Catalan C1": [r'(?:catalan|catalán).*(?:advanced|avanzado|c1)'],
        "French C2 Native": [r'(?:french|francés).*(?:native|nativo|c2)', r'native.*(?:french|francés)'],
        "French C1": [r'(?:french|francés).*(?:advanced|avanzado|c1)'],
        "French B2": [r'(?:french|francés).*(?:intermediate|intermedio|b2)'],
        "German C2 Native": [r'(?:german|alemán).*(?:native|nativo|c2)', r'native.*(?:german|alemán)'],
        "German C1": [r'(?:german|alemán).*(?:advanced|avanzado|c1)'],
        "German B2": [r'(?:german|alemán).*(?:intermediate|intermedio|b2)'],
        "Italian C2 Native": [r'(?:italian|italiano).*(?:native|nativo|c2)', r'native.*(?:italian|italiano)'],
        "Portuguese C2 Native": [r'(?:portuguese|portugués).*(?:native|nativo|c2)', r'native.*(?:portuguese|portugués)'],
    }
    for language, patterns in language_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                if language not in detected_languages:
                    detected_languages.append(language)
                break
    if not detected_languages:
        spanish_indicators = ['currículum', 'experiencia laboral', 'formación', 'estudios', 'educación']
        if any(indicator in text_lower for indicator in spanish_indicators):
            detected_languages.append("Spanish C2 Native")
        if re.search(r'\benglish\b|\binglés\b', text_lower):
            detected_languages.append("English B2")
    return detected_languages

async def extract_gender_advanced(text_content: str, nombre_apellido: str) -> str:
    try:
        prompt = f"""
Analiza el nombre y el contexto de este CV para determinar el género de la persona.
REGLAS:
1. Basa tu decisión principalmente en el nombre: "{nombre_apellido}"
2. Usa el texto del CV solo para buscar pronombres (he/she, él/ella) si el nombre es ambiguo.
3. Responde SOLAMENTE con una de estas opciones: "Male", "Female", o "" (vacío)
TEXTO DEL CV (para contexto):
{text_content[:1500]}
Responde "Male", "Female", o "":
"""
        gender = await _get_ai_response(prompt)
        if gender in ["Male", "Female"]:
            logging.info(f"Género extraído: {gender}")
            return gender
        else:
            logging.info("Género no encontrado")
            return ""
    except Exception as e:
        logging.error(f"Error extrayendo género con IA: {str(e)}")
        return ""

async def extract_candidate_data_ultra_improved(text_content: str, source: str = "cv") -> CandidateCreate:
    try:
        logging.info("=== INICIANDO EXTRACCIÓN ULTRA MEJORADA ===")
        nombre = await extract_name_advanced(text_content)
        logging.info(f"Nombre extraído: {nombre}")
        gender = await extract_gender_advanced(text_content, nombre)
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text_content, re.IGNORECASE)
        email = emails[0] if emails else ""
        logging.info(f"Email extraído: {email}")
        phone = await extract_phone_advanced(text_content)
        logging.info(f"Teléfono extraído: {phone}")
        if source == "linkedin":
            linkedin_url = text_content if 'linkedin.com/in/' in text_content else ""
        else:
            linkedin_url = await extract_linkedin_advanced(text_content)
        logging.info(f"LinkedIn URL extraída: {linkedin_url}")
        location = ""
        cities_dict = {
            "madrid": "Madrid, España", "barcelona": "Barcelona, España",
            "valencia": "Valencia, España", "sevilla": "Sevilla, España",
            "london": "London, UK", "paris": "Paris, France",
            "berlin": "Berlin, Germany", "amsterdam": "Amsterdam, Netherlands",
            "new york": "New York, USA", "san francisco": "San Francisco, USA"
        }
        text_lower = text_content.lower()
        for city_key, city_full in cities_dict.items():
            if city_key in text_lower:
                location = city_full
                break
        current_company = await extract_company_advanced(text_content)
        logging.info(f"Empresa actual extraída: {current_company}")
        skills = await extract_skills_with_ai(text_content)
        logging.info(f"Skills extraídas: {skills}")
        languages = await extract_languages_advanced(text_content)
        logging.info(f"Idiomas extraídos: {languages}")
        
        return CandidateCreate(
            nombre_apellido=nombre,
            email=email,
            phone=phone,
            location=location,
            linkedin_url=linkedin_url,
            current_company=current_company,
            skills=skills,
            languages=languages,
            gender=gender
        )
    except Exception as e:
        logging.error(f"Error en extracción ultra mejorada: {str(e)}")
        return CandidateCreate(
            nombre_apellido="Error en extracción",
            email="",
            phone="",
            skills=[],
            languages=[],
            gender=""
        )

async def upload_cv_to_external_service(file_content: bytes, filename: str) -> str:
    """
    Sube el CV a un servicio externo temporal y retorna la URL
    """
    try:
        files = {'file': (filename, file_content)}
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post('https://file.io', files=files)
            if response.status_code == 200:
                data = response.json()
                file_url = data.get('link')
                if file_url:
                    logging.info(f"Archivo subido a servicio externo: {file_url}")
                    return file_url
            logging.error(f"Error subiendo a file.io: {response.text}")
            return ""
    except Exception as e:
        logging.error(f"Error en upload_cv_to_external_service: {str(e)}")
        return ""

async def create_notion_record(candidate: CandidateCreate, file_content: bytes = None, filename: str = None) -> tuple[str, str]:
    try:
        headers = {
            "Authorization": f"Bearer {os.environ.get('NOTION_API_TOKEN')}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        notion_data = {
            "parent": {"database_id": os.environ.get('NOTION_DATABASE_ID')},
            "properties": {
                "Nombre y Apellido": {
                    "title": [{"text": {"content": candidate.nombre_apellido}}]
                }
            }
        }
        if candidate.email and candidate.email.strip():
            notion_data["properties"]["Email"] = {"email": candidate.email}
        if candidate.phone and candidate.phone.strip():
            notion_data["properties"]["Phone"] = {"phone_number": candidate.phone}
        if candidate.location and candidate.location.strip():
            notion_data["properties"]["LOCATION"] = {
                "rich_text": [{"text": {"content": candidate.location}}]
            }
        if candidate.linkedin_url and candidate.linkedin_url.strip():
            notion_data["properties"]["LINKEDIN URL"] = {"url": candidate.linkedin_url}
        if candidate.current_company and candidate.current_company.strip():
            notion_data["properties"]["CURRENT COMPANY"] = {
                "rich_text": [{"text": {"content": candidate.current_company}}]
            }
        if candidate.salary_expectation and candidate.salary_expectation.strip():
            notion_data["properties"]["SALARY EXPECTATION"] = {
                "rich_text": [{"text": {"content": candidate.salary_expectation}}]
            }
        if candidate.short_notes and candidate.short_notes.strip():
            notion_data["properties"]["SHORT NOTES"] = {
                "rich_text": [{"text": {"content": candidate.short_notes}}]
            }
        if candidate.skills and len(candidate.skills) > 0:
            valid_skills = [skill.strip() for skill in candidate.skills if skill and skill.strip()][:10]
            if valid_skills:
                notion_data["properties"]["Skills"] = {
                    "multi_select": [{"name": skill} for skill in valid_skills]
                }
        if candidate.languages and len(candidate.languages) > 0:
            valid_languages = [lang for lang in candidate.languages if lang in NOTION_OPTIONS["LANGUAJE"]]
            if valid_languages:
                notion_data["properties"]["LANGUAJE"] = {
                    "multi_select": [{"name": lang} for lang in valid_languages]
                }
        if candidate.stage and candidate.stage in NOTION_OPTIONS["STAGE"]:
            notion_data["properties"]["STAGE"] = {"select": {"name": candidate.stage}}
        if candidate.resolution and candidate.resolution in NOTION_OPTIONS["RESOLUTION"]:
            notion_data["properties"]["RESOLUTION"] = {"select": {"name": candidate.resolution}}
        if candidate.rejection_reason and candidate.rejection_reason in NOTION_OPTIONS["REJECTION_REASON"]:
            notion_data["properties"]["REJECTION REASON"] = {"select": {"name": candidate.rejection_reason}}
        if candidate.source and candidate.source in NOTION_OPTIONS["SOURCE"]:
            notion_data["properties"]["SOURCE"] = {"select": {"name": candidate.source}}
        if candidate.gender and candidate.gender in NOTION_OPTIONS["GENDER"]:
            notion_data["properties"]["Gender"] = {"select": {"name": candidate.gender}}
        
        if file_content and filename:
            try:
                file_url = await upload_cv_to_external_service(file_content, filename)
                if file_url:
                    notion_data["properties"]["ATTACHMENT"] = {
                        "files": [
                            {
                                "type": "external",
                                "name": filename,
                                "external": {"url": file_url}
                            }
                        ]
                    }
                    logging.info(f"CV {filename} agregado a propiedad ATTACHMENT")
            except Exception as e:
                logging.error(f"Error agregando CV a ATTACHMENT: {str(e)}")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "https://api.notion.com/v1/pages",
                json=notion_data,
                headers=headers
            )
            if response.status_code != 200:
                error_detail = f"Status: {response.status_code}, Response: {response.text}"
                logging.error(f"Notion API Error: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Error de API de Notion: {error_detail}"
                )
            notion_response = response.json()
            notion_id = notion_response["id"]
            notion_url = notion_response["url"]
            logging.info(f"Registro creado en Notion: {notion_id}")
            return notion_id, notion_url
            
    except httpx.HTTPStatusError as e:
        error_detail = "Error desconocido de Notion"
        try:
            error_response = e.response.json()
            error_detail = error_response.get("message", str(error_response))
        except:
            error_detail = e.response.text
        logging.error(f"HTTPStatusError: {error_detail}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Error de API de Notion: {error_detail}"
        )
    except Exception as e:
        logging.error(f"Error creando registro Notion: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

async def check_email_exists(email: str) -> tuple[bool, Optional[CandidateResponse]]:
    try:
        if not email or email.strip() == "":
            return False, None
        existing = await db.candidates.find_one({"email": email.strip().lower()})
        if existing:
            if isinstance(existing["created_at"], str):
                existing["created_at"] = datetime.fromisoformat(existing["created_at"])
            return True, CandidateResponse(**existing)
        return False, None
    except Exception as e:
        logging.error(f"Error checking email existence: {str(e)}")
        return False, None

# API Endpoints
@api_router.get("/")
async def root():
    return {"message": "ATS API Ultra Mejorado - v3.0"}

@api_router.get("/options", response_model=NotionOptionsResponse)
async def get_notion_options():
    return NotionOptionsResponse(
        stages=[""] + NOTION_OPTIONS["STAGE"],
        resolutions=[""] + NOTION_OPTIONS["RESOLUTION"],
        rejection_reasons=[""] + NOTION_OPTIONS["REJECTION_REASON"],
        languages=NOTION_OPTIONS["LANGUAJE"],
        sources=[""] + NOTION_OPTIONS["SOURCE"],
        genders=[""] + NOTION_OPTIONS["GENDER"]
    )

@api_router.post("/check-email", response_model=EmailCheckResponse)
async def check_email_endpoint(email: str):
    exists, existing_candidate = await check_email_exists(email)
    return EmailCheckResponse(exists=exists, existing_candidate=existing_candidate)

@api_router.post("/candidates/upload-cv")
async def upload_cv(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No se proporcionó un archivo")
    if not file.filename.lower().endswith(('.pdf', '.docx', '.doc')):
        raise HTTPException(
            status_code=400, 
            detail="Solo se permiten archivos PDF, DOCX o DOC"
        )
    try:
        file_content = await file.read()
        if file.filename.lower().endswith('.pdf'):
            text_content = await extract_text_from_pdf(file_content)
        else:
            text_content = await extract_text_from_docx(file_content)
        if not text_content.strip():
            raise HTTPException(status_code=400, detail="No se pudo extraer texto del archivo")
        
        candidate_data = await extract_candidate_data_ultra_improved(text_content, "cv")
        email_duplicate_info = None
        if candidate_data.email and candidate_data.email.strip():
            exists, existing = await check_email_exists(candidate_data.email)
            if exists:
                email_duplicate_info = {
                    "exists": True,
                    "existing_candidate": existing.dict() if existing else None
                }
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        return {
            "status": "preview",
            "candidate_data": candidate_data.dict(),
            "email_duplicate_info": email_duplicate_info,
            "file_info": {
                "filename": file.filename,
                "content_base64": file_base64,
                "size": len(file_content)
            },
            "extracted_text": text_content[:500] + "..." if len(text_content) > 500 else text_content
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando CV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@api_router.post("/candidates/process-linkedin")
async def process_linkedin(request: LinkedInProcessRequest):
    try:
        if "linkedin.com" not in request.linkedin_url.lower():
            raise HTTPException(status_code=400, detail="Debe ser una URL válida de LinkedIn")
        linkedin_content = await extract_linkedin_data(request.linkedin_url)
        candidate_data = await extract_candidate_data_ultra_improved(linkedin_content, "linkedin")
        candidate_data.linkedin_url = request.linkedin_url
        email_duplicate_info = None
        if candidate_data.email and candidate_data.email.strip():
            exists, existing = await check_email_exists(candidate_data.email)
            if exists:
                email_duplicate_info = {
                    "exists": True,
                    "existing_candidate": existing.dict() if existing else None
                }
        return {
            "status": "preview",
            "candidate_data": candidate_data.dict(),
            "email_duplicate_info": email_duplicate_info,
            "file_info": None,
            "extracted_text": linkedin_content[:500] + "..." if len(linkedin_content) > 500 else linkedin_content
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error procesando LinkedIn: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@api_router.post("/candidates/confirm-create", response_model=CandidateResponse)
async def confirm_create_candidate(request: ConfirmCandidateRequest):
    try:
        candidate_data = CandidateCreate(**request.candidate_data)
        if candidate_data.email and candidate_data.email.strip():
            exists, existing = await check_email_exists(candidate_data.email)
            if exists and not request.force_create_duplicate:
                raise HTTPException(
                    status_code=409,
                    detail=f"El email {candidate_data.email} ya existe en la base de datos. Email duplicado detectado."
                )
        file_content = None
        filename = None
        if request.file_info and request.file_info.get("content_base64"):
            file_content = base64.b64decode(request.file_info["content_base64"])
            filename = request.file_info["filename"]
        
        notion_record_id, notion_url = await create_notion_record(
            candidate_data, 
            file_content, 
            filename
        )
        candidate_response = CandidateResponse(
            **candidate_data.dict(),
            notion_record_id=notion_record_id,
            notion_url=notion_url,
            source_type="cv" if request.file_info else "linkedin"
        )
        candidate_dict = candidate_response.dict()
        candidate_dict["created_at"] = candidate_dict["created_at"].isoformat()
        if candidate_dict.get("email"):
            candidate_dict["email"] = candidate_dict["email"].lower()
        await db.candidates.insert_one(candidate_dict)
        return candidate_response
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error confirmando candidato: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@api_router.get("/candidates", response_model=List[CandidateResponse])
async def get_candidates():
    try:
        candidates = await db.candidates.find().sort("created_at", -1).to_list(100)
        for candidate in candidates:
            if isinstance(candidate["created_at"], str):
                candidate["created_at"] = datetime.fromisoformat(candidate["created_at"])
        return [CandidateResponse(**candidate) for candidate in candidates]
    except Exception as e:
        logging.error(f"Error obteniendo candidatos: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

@api_router.get("/health/notion")
async def notion_health_check():
    try:
        headers = {
            "Authorization": f"Bearer {os.environ.get('NOTION_API_TOKEN')}",
            "Notion-Version": "2022-06-28"
        }
        database_id = os.environ.get('NOTION_DATABASE_ID')
        url = f"https://api.notion.com/v1/databases/{database_id}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
        return {"status": "connected", "service": "notion"}
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Notion no disponible: {str(e)}"
        )

# Include router
app.include_router(api_router)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client. Close()
