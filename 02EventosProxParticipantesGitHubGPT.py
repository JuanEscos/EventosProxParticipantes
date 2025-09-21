# -*- coding: utf-8 -*-
"""
Created on Wed Sep 17 12:06:59 2025

@author: Juan
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extractor de participantes de FlowAgility - Basado en Módulo 1
Extrae información detallada de participantes usando las URLs obtenidas del Módulo 1
"""

import os
import sys
import json
import re
import time
import argparse
import traceback
import unicodedata
import random
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from glob import glob

# Third-party imports
try:
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Error importando dependencias: {e}")
    sys.exit(1)

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError as e:
    print(f"❌ Error importando Selenium: {e}")
    HAS_SELENIUM = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# ============================== CONFIGURACIÓN GLOBAL ==============================

# Configuración base
BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
SCRIPT_DIR = Path(__file__).resolve().parent

# Cargar variables de entorno
try:
    load_dotenv(SCRIPT_DIR / ".env")
    print("✅ Variables de entorno cargadas")
except Exception as e:
    print(f"❌ Error cargando .env: {e}")

# Credenciales
FLOW_EMAIL = os.getenv("FLOW_EMAILRq", "raquelcort1134@gmail.com")
FLOW_PASS = os.getenv("FLOW_PASSRq", "Seattle$")

# Flags/tunables
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "15"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "3.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")
# Flags/tunables (añade esta línea)
LIMIT_EVENTS = int(os.getenv("LIMIT_EVENTS", "0"))  # 0 = sin límite


print(f"📋 Configuración: HEADLESS={HEADLESS}, OUT_DIR={OUT_DIR}")

# ============================== UTILIDADES GENERALES ==============================

def log(message):
    """Función de logging"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=1, max_s=2):
    """Pausa aleatoria"""
    time.sleep(random.uniform(min_s, max_s))

def _clean(s: str) -> str:
    """Limpia y normaliza texto"""
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

def _clean_output_directory():
    """Limpiar archivos antiguos del directorio de output"""
    try:
        files_to_keep = ['config.json', 'settings.ini']
        for file in os.listdir(OUT_DIR):
            if file not in files_to_keep:
                file_path = os.path.join(OUT_DIR, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    log(f"🧹 Eliminado archivo antiguo: {file}")
        log("✅ Directorio de output limpiado")
    except Exception as e:
        log(f"⚠️  Error limpiando directorio: {e}")


# B) Helpers de cortesía (pausas) + helpers de reanudación (añadir debajo de tus utilidades Selenium)
def polite_pause(min_s=None, max_s=None):
    """Pausa de cortesía con aleatoriedad."""
    a = THROTTLE_PAGE_MIN_S if min_s is None else float(min_s)
    b = THROTTLE_PAGE_MAX_S if max_s is None else float(max_s)
    time.sleep(random.uniform(a, b))

def _event_key(info: dict) -> str:
    """Clave única para mapear eventos guardados."""
    return (info.get('event_url_participantes')
            or info.get('event_id')
            or info.get('event_nombre')
            or "")

def _find_existing_event(data_list, key):
    """Devuelve (idx, ref) de evento por key, o (None, None)."""
    for i, e in enumerate(data_list):
        if _event_key(e.get('informacion_evento', {})) == key:
            return i, e
    return None, None

def _load_existing_output(today_str: str):
    """
    Carga el JSON de salida si existe (para RESUME). Devuelve (data_list, out_path, latest_path).
    Política:
      1) RESUME_FILE si está definido,
      2) participantes_detallados_{today}.json,
      3) participantes_detallados.json,
      4) lista vacía si no existe nada.
    """
    out_path_today = os.path.join(OUT_DIR, f'participantes_detallados_{today_str}.json')
    latest_path = os.path.join(OUT_DIR, 'participantes_detallados.json')

    candidates = []
    if RESUME_FILE:
        candidates.append(RESUME_FILE)
    candidates.extend([out_path_today, latest_path])

    for path in candidates:
        try:
            if path and os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data, out_path_today, latest_path
        except Exception:
            pass
    return [], out_path_today, latest_path

def _save_output_atomic(data_list, out_path, latest_path=None):
    """Escritura atómica: escribe a .tmp y luego reemplaza."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)
    if latest_path:
        tmp2 = latest_path + ".tmp"
        with open(tmp2, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, ensure_ascii=False, indent=2)
        os.replace(tmp2, latest_path)

# ============================== FUNCIONES DE NAVEGACIÓN ==============================

#  Aqui instalacion de Chomdriver para GitHub # 😊✨😊✨😊✨😊. Esto hay que quitarlo en Spyder y poner el siguiente
def _get_driver(headless=True):
    #Crea y configura el driver de Selenium
    if not HAS_SELENIUM:
        raise ImportError("Selenium no está instalado")
    
    opts = Options()
    
    # Configuración específica para GitHub Actions/entornos headless
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-browser-side-navigation")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    if headless:
        opts.add_argument("--headless=new")
    if INCOGNITO:
        opts.add_argument("--incognito")
    
    # Configuración adicional para evitar detección
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    try:
        # USAR CHROME Y CHROMEDRIVER INSTALADOS CORRECTAMENTE
        # Ruta correcta de Chrome en Ubuntu
        opts.binary_location = "/usr/bin/google-chrome-stable"
        
        # Buscar chromedriver en varias ubicaciones posibles
        chromedriver_paths = [
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "/snap/bin/chromedriver"
        ]
        
        chromedriver_path = None
        for path in chromedriver_paths:
            if os.path.exists(path):
                chromedriver_path = path
                break
        
        if not chromedriver_path:
            raise Exception("No se encontró chromedriver en las rutas esperadas")
        
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=opts)
        
        # Ejecutar script para evitar detección
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.set_page_load_timeout(90)
        driver.implicitly_wait(30)
        return driver
        
    except Exception as e:
        log(f"Error creando driver: {e}")
        log("Traceback completo:")
        import traceback
        traceback.print_exc()
        return None

def _login(driver):
    #Inicia sesión en FlowAgility
    if not driver:
        return False
        
    log("Iniciando login...")
    
    try:
        driver.get(f"{BASE}/user/login")
        
        # Esperar más tiempo en GitHub Actions
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        slow_pause(3, 5)
        
        # Verificar si ya estamos logueados (redirección)
        if "/user/login" not in driver.current_url:
            log("Ya autenticado (redirección detectada)")
            return True
        
        # Buscar campos de login con múltiples selectores
        email_selectors = [
            (By.NAME, "user[email]"),
            (By.ID, "user_email"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.XPATH, "//input[contains(@name, 'email')]")
        ]
        
        password_selectors = [
            (By.NAME, "user[password]"),
            (By.ID, "user_password"),
            (By.CSS_SELECTOR, "input[type='password']")
        ]
        
        submit_selectors = [
            (By.CSS_SELECTOR, 'button[type="submit"]'),
            (By.XPATH, "//button[contains(text(), 'Sign') or contains(text(), 'Log') or contains(text(), 'Iniciar')]")
        ]
        
        email_field = None
        for selector in email_selectors:
            try:
                email_field = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(selector)
                )
                break
            except:
                continue
        
        if not email_field:
            log("❌ No se pudo encontrar campo email")
            return False
        
        password_field = None
        for selector in password_selectors:
            try:
                password_field = driver.find_element(*selector)
                break
            except:
                continue
        
        if not password_field:
            log("❌ No se pudo encontrar campo password")
            return False
        
        submit_button = None
        for selector in submit_selectors:
            try:
                submit_button = driver.find_element(*selector)
                break
            except:
                continue
        
        if not submit_button:
            log("❌ No se pudo encontrar botón submit")
            return False
        
        # Llenar campos
        email_field.clear()
        email_field.send_keys(FLOW_EMAIL)
        slow_pause(1, 2)
        
        password_field.clear()
        password_field.send_keys(FLOW_PASS)
        slow_pause(1, 2)
        
        # Hacer clic
        submit_button.click()
        
        # Esperar a que se complete el login con timeout extendido
        try:
            WebDriverWait(driver, 45).until(
                lambda d: "/user/login" not in d.current_url or "dashboard" in d.current_url or "zone" in d.current_url
            )
            
            # Verificar login exitoso
            slow_pause(5, 8)  # Pausa más larga para GitHub Actions
            
            current_url = driver.current_url
            if "/user/login" in current_url:
                log("❌ Login falló - aún en página de login")
                # Verificar mensajes de error
                try:
                    error_elements = driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .text-red-600")
                    for error in error_elements:
                        log(f"Mensaje error: {error.text}")
                except:
                    pass
                return False
            else:
                log(f"✅ Login exitoso - Redirigido a: {current_url}")
                return True
                
        except TimeoutException:
            log("❌ Timeout esperando redirección de login")
            # Tomar screenshot para debugging
            try:
                driver.save_screenshot("/tmp/login_timeout.png")
                log("📸 Screenshot guardado en /tmp/login_timeout.png")
            except:
                pass
            return False
        
    except Exception as e:
        log(f"❌ Error en login: {e}")
        log(f"Traceback: {traceback.format_exc()}")
        return False

# ================= COOKIE HELPERS (reemplazo seguro) =================

def _accept_cookies(driver) -> bool:
    """Intenta aceptar cookies por selectores comunes; si falla, usa un fallback JS."""
    if not driver:
        return False
    try:
        cookie_selectors = [
            "[data-testid='uc-accept-all-button']",
            "button[aria-label='Accept all']",
            "button[aria-label='Aceptar todo']",
            "button[mode='primary']",
            "button:contains('Aceptar')",      # no estándar, por si hay lib que lo soporte
            "button:contains('Accept')",
        ]

        # Intento 1: selectores directos
        clicked = False
        for sel in cookie_selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", els[0])
                    try:
                        els[0].click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", els[0])
                    time.sleep(0.3)
                    clicked = True
                    break
            except Exception:
                # seguimos probando otros selectores
                pass

        if clicked:
            return True

        # Intento 2: fallback JS genérico
        return accept_cookies_fallback(driver)

    except Exception as e:
        log(f"Error manejando cookies: {e}")
        return False


def accept_cookies_fallback(driver) -> bool:
    """Fallback con JS: busca botones con texto típico y hace click."""
    try:
        driver.execute_script("""
          (function(){
            const labels = [
              /aceptar.*(todas|todo|cookies)/i,
              /accept.*(all|cookies)/i,
              /consent/i,
              /agree/i
            ];
            const btns = Array.from(document.querySelectorAll('button, [role="button"], input[type="button"], input[type="submit"]'));
            for (const b of btns) {
              const t = (b.textContent || b.value || "").trim();
              if (!t) continue;
              if (labels.some(rx => rx.test(t))) {
                b.click();
                return true;
              }
            }
            // también probamos enlaces con aspecto de botón
            const links = Array.from(document.querySelectorAll('a'));
            for (const a of links) {
              const t = (a.textContent || "").trim();
              if (labels.some(rx => rx.test(t))) {
                a.click();
                return true;
              }
            }
            return false;
          })();
        """)
        time.sleep(0.4)
        return True
    except Exception as e:
        log(f"Cookie JS fallback error: {e}")
        return False


def _full_scroll(driver):
    #Scroll completo para cargar todos los elementos
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
#  Hasta Aqui instalacion de Chomdriver para GitHub # 😊✨😊✨😊✨😊. Esto hay que quitarlo en Spyder y poner el siguiente


# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_events():
    """Función principal para extraer eventos básicos"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== MÓDULO 1: EXTRACCIÓN DE EVENTOS BÁSICOS ===")
    
    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        _accept_cookies(driver)
        
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        event_containers = soup.find_all('div', class_='group mb-6')
        log(f"Encontrados {len(event_containers)} contenedores de eventos")
        
        events = []
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = {}
                
                event_id = container.get('id', '')
                if event_id:
                    event_data['id'] = event_id.replace('event-card-', '')
                
                name_elem = container.find('div', class_='font-caption text-lg text-black truncate -mt-1')
                if name_elem:
                    event_data['nombre'] = _clean(name_elem.get_text())
                
                date_elem = container.find('div', class_='text-xs')
                if date_elem:
                    event_data['fechas'] = _clean(date_elem.get_text())
                
                org_elems = container.find_all('div', class_='text-xs')
                if len(org_elems) > 1:
                    event_data['organizacion'] = _clean(org_elems[1].get_text())
                
                club_elem = container.find('div', class_='text-xs mb-0.5 mt-0.5')
                if club_elem:
                    event_data['club'] = _clean(club_elem.get_text())
                else:
                    for div in container.find_all('div', class_='text-xs'):
                        text = _clean(div.get_text())
                        if text and not any(x in text for x in ['/', 'Spain', 'España']):
                            event_data['club'] = text
                            break
                
                location_divs = container.find_all('div', class_='text-xs')
                for div in location_divs:
                    text = _clean(div.get_text())
                    if '/' in text and any(x in text for x in ['Spain', 'España', 'Madrid', 'Barcelona']):
                        event_data['lugar'] = text
                        break
                
                if 'lugar' not in event_data:
                    for div in location_divs:
                        text = _clean(div.get_text())
                        if '/' in text and len(text) < 100:
                            event_data['lugar'] = text
                            break
                
                event_data['enlaces'] = {}
                
                info_link = container.find('a', href=lambda x: x and '/info/' in x)
                if info_link:
                    event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
                
                participant_links = container.find_all('a', href=lambda x: x and any(term in x for term in ['/participants', '/participantes']))
                for link in participant_links:
                    href = link.get('href', '')
                    if '/participants_list' in href or '/participantes' in href:
                        event_data['enlaces']['participantes'] = urljoin(BASE, href)
                        break
                
                if 'participantes' not in event_data['enlaces'] and 'id' in event_data:
                    event_data['enlaces']['participantes'] = f"{BASE}/zone/events/{event_data['id']}/participants_list"
                
                flag_elem = container.find('div', class_='text-md')
                if flag_elem:
                    event_data['pais_bandera'] = _clean(flag_elem.get_text())
                else:
                    event_data['pais_bandera'] = '🇪🇸'
                
                events.append(event_data)
                log(f"✅ Evento {i} procesado: {event_data.get('nombre', 'Sin nombre')}")
                # 👉 corta en caliente si hay límite
                if isinstance(limit, int) and limit > 0 and len(events) >= limit:
                    log(f"⏹️  Alcanzado límite de {limit} eventos en Módulo 1")
                    break
                # por si quieres cortar de nuevo por seguridad (no imprescindible)
                if isinstance(limit, int) and limit > 0:
                    events = events[:limit]  
              
            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                continue
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        os.makedirs(OUT_DIR, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        latest_file = os.path.join(OUT_DIR, '01events.json')
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        
        return events
        
    except Exception as e:
        log(f"❌ Error durante el scraping: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except:
            pass


# ============================== MÓDULO 2: EXTRACCIÓN DETALLADA DE PARTICIPANTES ==============================
# Requiere: BASE, OUT_DIR, HEADLESS, LIMIT_EVENTS, log, slow_pause, _get_driver, _login, _accept_cookies
# e imports ya cargados (re, json, time, random, os, glob, datetime, unicodedata, BeautifulSoup, Selenium)

# A) Config de “modo lento” + reanudación#
# --- Throttling y Resume (.env admite overrides) ---
THROTTLE_EVENT_S       = float(os.getenv("THROTTLE_EVENT_S", "3.0"))   # pausa entre eventos
THROTTLE_PAGE_MIN_S    = float(os.getenv("THROTTLE_PAGE_MIN_S", "1.2"))
THROTTLE_PAGE_MAX_S    = float(os.getenv("THROTTLE_PAGE_MAX_S", "2.5"))
THROTTLE_TOGGLE_MIN_S  = float(os.getenv("THROTTLE_TOGGLE_MIN_S", "0.9"))
THROTTLE_TOGGLE_MAX_S  = float(os.getenv("THROTTLE_TOGGLE_MAX_S", "2.2"))
AUTO_SAVE_EVERY        = int(os.getenv("AUTO_SAVE_EVERY", "10"))       # guarda cada N participantes nuevos

RESUME                 = os.getenv("RESUME", "true").lower() == "true" # lee destino y continúa
RESUME_FILE            = os.getenv("RESUME_FILE", "").strip()          # si vacío, autodetecta





# 1) _clean mejorado (reemplaza tu versión previa si quieres normalización/antiespacios más robusta)
EMOJI_RE = re.compile(
    "[\U0001F1E6-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+"
)
def _clean(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = EMOJI_RE.sub("", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

# 2) Utilidades Selenium para participantes
def _collect_booking_ids(driver):
    """Devuelve lista única de booking_id presentes en la página (toggles LiveView)."""
    try:
        ids = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("[phx-click='booking_details_show']")
            ).map(el => el.getAttribute("phx-value-booking_id")).filter(Boolean);
        """) or []
    except Exception:
        ids = []
    out, seen = [], set()
    for x in ids:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _click_toggle_by_pid(driver, pid, By, WebDriverWait, EC):
    """Hace click en el toggle del participante y espera a que aparezca el bloque con ese id."""
    sel = f"[phx-click='booking_details_show'][phx-value-booking_id='{pid}']"
    for _ in range(6):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, 8).until(lambda d: d.find_element(By.ID, pid))
            return driver.find_element(By.ID, pid)
        except Exception:
            time.sleep(0.4)
            driver.execute_script("window.scrollBy(0, 160);")
    return None

# 3) Mapper JS rico (extrae campos + hasta 6 bloques Día/Fecha/Mangas)
JS_MAP_PARTICIPANT_RICH = r"""
const pid = arguments[0];
const root = document.getElementById(pid);
if (!root) return null;

const txt = el => (el && el.textContent) ? el.textContent.trim() : null;

function classListArray(el){
  if (!el) return [];
  const cn = el.className;
  if (!cn) return [];
  if (typeof cn === 'string') return cn.trim().split(/\s+/);
  if (typeof cn === 'object' && 'baseVal' in cn) return String(cn.baseVal).trim().split(/\s+/);
  return String(cn).trim().split(/\s+/);
}
function isHeader(el){
  const arr = classListArray(el);
  return (arr.includes('border-b') && arr.includes('border-gray-400'))
      || (arr.includes('font-bold') && arr.includes('text-sm') && arr.some(c => /^mt-/.test(c)));
}
function isLabel(el){ return (classListArray(el).includes('text-gray-500') && classListArray(el).includes('text-sm')); }
function isStrong(el){
  const arr = classListArray(el);
  return (arr.includes('font-bold') && arr.includes('text-sm'));
}
function nextStrong(el){
  let cur = el;
  for (let i=0;i<8;i++){
    cur = cur && cur.nextElementSibling;
    if (!cur) break;
    if (isStrong(cur)) return cur;
  }
  return null;
}

const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, null);
let node = walker.currentNode;
let currentDay = null;
let tmpFecha = null;
let tmpMangas = null;

const fields = {};
const schedule = [];

const simpleFieldLabels = new Set([
  "Dorsal","Guía","Guia","Perro","Raza","Edad","Género","Genero",
  "Altura (cm)","Altura","Nombre de Pedigree","Nombre de Pedrigree",
  "País","Pais","Licencia","Equipo","Club","Federación","Federacion"
]);

while (node){
  if (isHeader(node)){
    const t = txt(node); if (t) currentDay = t;
  } else if (isLabel(node)){
    const label = (txt(node) || "");
    const valueEl = nextStrong(node);
    const value = txt(valueEl) || "";

    const l = label.toLowerCase();
    if (l.startsWith("fecha"))       { tmpFecha  = value; }
    else if (l.startsWith("mangas")) { tmpMangas = value; }
    else if (simpleFieldLabels.has(label) && value && (fields[label] == null || fields[label] === "")) {
      fields[label] = value;
    }

    if (tmpFecha !== null && tmpMangas !== null){
      schedule.push({ day: currentDay || "", fecha: tmpFecha, mangas: tmpMangas });
      tmpFecha = null; tmpMangas = null;
    }
  }
  node = walker.nextNode();
}
return { fields, schedule };
"""

# 4) Fallback XPath (si el DOM cambia y el JS no encuentra estructura)
def _fallback_map_participant(driver, pid, By):
    labels = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'text-gray-500') and contains(@class,'text-sm')]"
    )
    values = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
    )
    fields = {}
    for lab_el, val_el in zip(labels, values):
        lt = _clean(lab_el.text or "")
        vt = _clean(val_el.text or "")
        if lt and vt and lt not in fields:
            fields[lt] = vt

    headers = driver.find_elements(
        By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'border-b') and contains(@class,'border-gray-400')]"
    )
    schedule = []
    for h in headers:
        fecha = h.find_elements(
            By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][1]"
        )
        mangas = h.find_elements(
            By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][2]"
        )
        schedule.append({
            "day": _clean(h.text or ""),
            "fecha": _clean(fecha[0].text if fecha else ""),
            "mangas": _clean(mangas[0].text if mangas else "")
        })
    return {"fields": fields, "schedule": schedule}

# 5) Función principal del Módulo 2 (usa LIMIT_EVENTS si está definido)
def extract_participants_info():
    """Extrae info detallada de participantes con cortesía (lenta) y reanudación segura."""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None

    log("=== EXTRACCIÓN DETALLADA DE PARTICIPANTES (lenta + resume) ===")

    # 1) Cargar último 01events_*.json del Módulo 1
    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return None
    latest_event_file = max(event_files, key=os.path.getctime)
    with open(latest_event_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    log(f"✅ Cargados {len(events)} eventos desde {latest_event_file}")

    # Limitar nº de eventos si LIMIT_EVENTS > 0
    try:
        limit = int(LIMIT_EVENTS) if isinstance(LIMIT_EVENTS, (int, str)) else 0
    except Exception:
        limit = 0
    if limit and limit > 0:
        events = events[:limit]
        log(f"🔎 LIMIT_EVENTS activo en participantes: procesaré {len(events)} eventos")

    # 2) Cargar/crear salida (para RESUME)
    today_str = datetime.now().strftime("%Y-%m-%d")
    existing_list, out_path, latest_path = _load_existing_output(today_str)
    log(f"🗂️  Archivo de trabajo: {out_path}")
    if existing_list:
        log(f"♻️  Reanudación activada: {len(existing_list)} eventos ya guardados")

    # 3) Driver y login
    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None

    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")

        # 4) Procesar eventos
        for i, event in enumerate(events, 1):
            try:
                plist = (event.get('enlaces') or {}).get('participantes')
                if not plist:
                    eid = event.get('id') or ""
                    if eid:
                        plist = f"{BASE}/zone/events/{eid}/participants_list"
                if not plist:
                    log(f"⚠️  Evento {i} sin URL de participantes: {event.get('nombre','(sin nombre)')}")
                    polite_pause(1.2, 1.8)
                    continue

                # Preparar estructura en existing_list para RESUME
                target_info = {
                    'event_id': event.get('id', ''),
                    'event_nombre': event.get('nombre', ''),
                    'event_fechas': event.get('fechas', ''),
                    'event_club': event.get('club', ''),
                    'event_lugar': event.get('lugar', ''),
                    'event_url_participantes': plist,
                }
                ekey = _event_key(target_info)
                idx, existing_event = _find_existing_event(existing_list, ekey)
                if existing_event is None:
                    existing_event = {
                        'informacion_evento': {
                            **target_info,
                            'total_participantes': 0,
                            'timestamp_extraccion': datetime.now().isoformat()
                        },
                        'participantes': []
                    }
                    existing_list.append(existing_event)
                    idx = len(existing_list) - 1

                processed_bids = {p.get("BinomID") for p in existing_event.get('participantes', []) if p.get("BinomID")}
                already = len(processed_bids)

                log(f"Procesando participantes {i}/{len(events)}: {event.get('nombre','(sin nombre)')}")
                log(f"  URL: {plist}")
                if already:
                    log(f"  ♻️  Reanudación: {already} participantes ya guardados, continuaré desde el siguiente.")

                # 5) Abrir lista y esperar toggles
                driver.get(plist)
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                _accept_cookies(driver)
                polite_pause()  # cortesía tras cargar página

                # Detectar estado básico
                state = "timeout"
                t0 = time.time()
                while time.time() - t0 < 25:
                    if "/user/login" in (driver.current_url or ""):
                        state = "login"; break
                    if driver.find_elements(By.CSS_SELECTOR, "[phx-click='booking_details_show']"):
                        state = "ok"; break
                    if driver.find_elements(By.XPATH, "//p[contains(., 'No hay') or contains(., 'No results')]"):
                        state = "empty"; break
                    time.sleep(0.25)

                if state == "login":
                    log("Sesión caducada; reintentando login…")
                    if not _login(driver):
                        log("No se pudo relogar, salto evento.")
                        polite_pause()
                        continue
                    driver.get(plist)
                    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    polite_pause(0.8, 1.5)

                if state == "empty":
                    log("participants_list sin participantes.")
                    existing_event['informacion_evento'].update({
                        'total_participantes': len(existing_event['participantes']),
                        'timestamp_extraccion': datetime.now().isoformat()
                    })
                    _save_output_atomic(existing_list, out_path, latest_path)
                    polite_pause(THROTTLE_EVENT_S, THROTTLE_EVENT_S + 0.6)
                    continue

                # 6) Recoger booking_ids
                booking_ids = _collect_booking_ids(driver)
                total = len(booking_ids)
                log(f"  ✅ Detectados {total} participantes (toggles)")

                # Si ya teníamos todos, saltamos evento
                if total and len(processed_bids) >= total:
                    log("  ♻️  Evento completo previamente. Paso al siguiente.")
                    polite_pause(THROTTLE_EVENT_S, THROTTLE_EVENT_S + 0.6)
                    continue

                # 7) Iterar participantes con pausas y guardado incremental
                new_counter = 0
                for idxp, pid in enumerate(booking_ids, start=1):
                    if not pid or pid in processed_bids:
                        continue

                    # Desplegar bloque + esperar render
                    block = _click_toggle_by_pid(driver, pid, By, WebDriverWait, EC)
                    if not block:
                        polite_pause(0.5, 1.0)
                        continue

                    # Espera a STRONG pintados (valores)
                    painted = False
                    end = time.time() + 12.0
                    while time.time() < end:
                        strongs = block.find_elements(By.XPATH, ".//div[contains(@class,'font-bold') and contains(@class,'text-sm')]")
                        if strongs:
                            painted = True
                            break
                        time.sleep(0.25)
                    if not painted:
                        polite_pause(0.6, 1.1)
                        continue

                    # 1º intento: JS map
                    try:
                        payload = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
                    except Exception:
                        payload = None
                    # Fallback
                    if not payload or not isinstance(payload, dict):
                        payload = _fallback_map_participant(driver, pid, By)

                    fields = (payload.get("fields") or {})
                    schedule = (payload.get("schedule") or [])

                    def pick(keys, default=""):
                        for k in keys:
                            v = fields.get(k)
                            if v:
                                return _clean(v)
                        return default

                    row = {
                        "BinomID": pid,
                        "Dorsal": pick(["Dorsal"]),
                        "Guía": pick(["Guía","Guia"]),
                        "Perro": pick(["Perro"]),
                        "Raza": pick(["Raza"]),
                        "Edad": pick(["Edad"]),
                        "Género": pick(["Género","Genero"]),
                        "Altura (cm)": pick(["Altura (cm)","Altura"]),
                        "Nombre de Pedigree": pick(["Nombre de Pedigree","Nombre de Pedrigree"]),
                        "País": pick(["País","Pais"]),
                        "Licencia": pick(["Licencia"]),
                        "Club": pick(["Club"]),
                        "Federación": pick(["Federación","Federacion"]),
                        "Equipo": pick(["Equipo"]),
                    }
                    for j in range(1, 7):
                        day = schedule[j-1]["day"] if j-1 < len(schedule) else ""
                        fec = schedule[j-1]["fecha"] if j-1 < len(schedule) else ""
                        man = schedule[j-1]["mangas"] if j-1 < len(schedule) else ""
                        row[f"Día {j}"]    = _clean(day)
                        row[f"Fecha {j}"]  = _clean(fec)
                        row[f"Mangas {j}"] = _clean(man)

                    # Añadir y marcar como procesado
                    existing_event['participantes'].append(row)
                    processed_bids.add(pid)
                    new_counter += 1

                    # Guardado incremental
                    if new_counter % AUTO_SAVE_EVERY == 0:
                        existing_event['informacion_evento'].update({
                            'total_participantes': len(existing_event['participantes']),
                            'timestamp_extraccion': datetime.now().isoformat()
                        })
                        _save_output_atomic(existing_list, out_path, latest_path)

                    # Pausa entre toggles (más lenta que antes)
                    time.sleep(random.uniform(THROTTLE_TOGGLE_MIN_S, THROTTLE_TOGGLE_MAX_S))

                # 8) Cierre de evento + guardado
                existing_event['informacion_evento'].update({
                    'event_fechas': event.get('fechas', ''),
                    'event_club':  event.get('club', ''),
                    'event_lugar': event.get('lugar', ''),
                    'total_participantes': len(existing_event['participantes']),
                    'timestamp_extraccion': datetime.now().isoformat()
                })
                _save_output_atomic(existing_list, out_path, latest_path)
                log(f"  ✅ Evento OK: {len(existing_event['participantes'])} participantes acumulados")
                polite_pause(THROTTLE_EVENT_S, THROTTLE_EVENT_S + 0.8)  # cortesía entre eventos

            except Exception as e:
                log(f"❌ Error en evento {i}: {e}")
                traceback.print_exc()
                # guarda lo que haya antes de continuar
                try:
                    _save_output_atomic(existing_list, out_path, latest_path)
                except Exception:
                    pass
                polite_pause(THROTTLE_EVENT_S, THROTTLE_EVENT_S + 1.2)
                continue

        # 9) Resumen
        total_events = len(existing_list)
        total_people = sum(len(e.get('participantes', [])) for e in existing_list)
        log(f"✅ Guardado final en {out_path}")
        print("\n" + "="*80)
        print("RESUMEN FINAL PARTICIPANTES:")
        print("="*80)
        print(f"Eventos (presentes en archivo): {total_events}")
        print(f"Total participantes (acumulado): {total_people}")
        if total_events:
            top = sorted(existing_list, key=lambda x: len(x.get('participantes', [])), reverse=True)[:5]
            print("\n📊 Top eventos por nº de participantes:")
            for t in top:
                print(f"  {t['informacion_evento'].get('event_nombre','(sin nombre)')}: {len(t.get('participantes', []))}")
        print("\n" + "="*80 + "\n")

        return existing_list

    except Exception as e:
        log(f"❌ Error global en participantes: {e}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
        except:
            pass




# ============================== FUNCIÓN PRINCIPAL ==============================

def main():
    """Función principal"""
    print("🚀 EXTRACTOR DE PARTICIPANTES DE FLOWAGILITY")
    print("📋 Este proceso extrae eventos y luego información detallada de participantes")
    print(f"📂 Directorio de salida: {OUT_DIR}")
    print("=" * 80)
    
    os.makedirs(OUT_DIR, exist_ok=True)
    #_clean_output_directory()
    
    parser = argparse.ArgumentParser(description="Extractor de participantes de FlowAgility")
    
    parser.add_argument("--module",
        choices=["events","participants","all"],
        default="all",
        help="Qué módulo ejecutar")
    
    parser.add_argument("--limit-events",
        type=int,
        default=None,
        help="Límite de eventos a procesar en ambos módulos (si no se indica, usa LIMIT_EVENTS del entorno)")
    
    args = parser.parse_args()
    
    # justo después de parse_args:
    global LIMIT_EVENTS
    if args.limit_events is not None:
        LIMIT_EVENTS = int(args.limit_events)
    log(f"🔧 LIMIT_EVENTS efectivo: {LIMIT_EVENTS or 0} (0 = sin límite)")

    
    try:
        success = True
        events_data = None
        participants_data = None
        
        # Módulo 1: Eventos básicos
        if args.module in ["events", "all"]:
            log("🏁 INICIANDO EXTRACCIÓN DE EVENTOS BÁSICOS")
            events_data = extract_events(limit=LIMIT_EVENTS)
            if not events_data:
                log("❌ Falló la extracción de eventos")
                success = False
            else:
                log("✅ Eventos básicos extraídos correctamente")
        
        # Módulo 2: Participantes detallados
        if args.module in ["participants", "all"] and success:
            log("🏁 INICIANDO EXTRACCIÓN DE PARTICIPANTES DETALLADOS")
            participants_data = extract_participants_info()
            if not participants_data:
                log("⚠️  No se pudo extraer información de participantes")
            else:
                log("✅ Información de participantes extraída correctamente")
        
        if success:
            log("🎉 PROCESO COMPLETADO EXITOSAMENTE")
            
            print(f"\n📁 ARCHIVOS GENERADOS EN {OUT_DIR}:")
            output_files = glob(os.path.join(OUT_DIR, "*"))
            for file in sorted(output_files):
                if os.path.isfile(file):
                    size = os.path.getsize(file)
                    print(f"   {os.path.basename(file)} - {size} bytes")
                    
        else:
            log("❌ PROCESO COMPLETADO CON ERRORES")
        
        return success
        
    except Exception as e:
        log(f"❌ ERROR CRÍTICO DURANTE LA EJECUCIÓN: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
