#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - MÓDULO 2 (MEJORADO CON PAGINACIÓN): PARTICIPANTES DETALLADOS
- Lee ./output/01events.json
- Para cada evento abre participants_list, detecta booking_id (PID),
  abre el panel (sin colapsar el primero si ya está abierto) y mapea campos.
- Mapeo híbrido: JS (ES/EN) + BeautifulSoup (hermano fuerte) + fallback XPATH.
- Navegación por paginación para obtener TODOS los participantes.
- Salidas:
  * ./output/02participants.json
  * ./output/02participants_debug.json (si DEBUG_PARTICIPANTS=1)
  * ./output/participants/02p_<event_id>.json (por evento)
  * ./output/participants/raw_<event_id>_pageX.html (dump si no hay toggles)
"""

import os
import sys
import json
import re
import time
import random
import unicodedata
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException,
    StaleElementReferenceException, NoSuchElementException
)

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WDM = True
except ImportError:
    HAS_WDM = False

BASE = "https://www.flowagility.com"
SCRIPT_DIR = Path(__file__).resolve().parent

# ===================== ENV / CONFIG =====================

try: load_dotenv(SCRIPT_DIR / ".env")
except Exception: pass

FLOW_EMAIL = os.getenv("FLOW_EMAIL", "")
FLOW_PASS  = os.getenv("FLOW_PASS", "")

HEADLESS        = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO       = os.getenv("INCOGNITO", "true").lower() == "true"
OUT_DIR         = os.getenv("OUT_DIR", "./output")
LIMIT_EVENTS    = int(os.getenv("LIMIT_EVENTS", "0"))
PER_EVENT_MAX_S = int(os.getenv("PER_EVENT_MAX_S", "600"))  # Aumentado para paginación
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
DEBUG_PARTICIPANTS = os.getenv("DEBUG_PARTICIPANTS", "0") == "1"
MAX_PAGES       = int(os.getenv("MAX_PAGES", "0"))  # 0 = todas las páginas

# Etiquetas → claves canónicas (ES + EN)
ALIASES = {
    # ES
    "dorsal":"dorsal","guía":"guia","guia":"guia","perro":"perro","raza":"raza",
    "edad":"edad","género":"genero","genero":"genero","altura (cm)":"altura_cm","altura":"altura_cm",
    "club":"club","licencia":"licencia","federación":"federacion","federacion":"federacion",
    "nombre de pedigree":"nombre_pedigree","nombre de pedrigree":"nombre_pedigree",
    "país":"pais","pais":"pais","equipo":"equipo",
    # EN
    "handler":"guia","dog":"perro","breed":"raza","age":"edad","gender":"genero",
    "height (cm)":"altura_cm","license number":"licencia","federation":"federacion",
    "country":"pais","team":"equipo"
}

def log(s): print(f"[{datetime.now().strftime('%H:%M:%S')}] {s}")
def sleep(a=0.20,b=0.45): time.sleep(random.uniform(a,b))

def _clean(s):
    if s is None: return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[ \t]+", " ", s.strip())
    return s

def _now(): return time.time()
def _deadline(sec): return _now() + max(0, sec)
def _left(deadline): return max(0.0, deadline - _now())

# ===================== Driver / Login =====================

def _get_driver():
    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    for a in ["--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
              "--disable-extensions","--disable-infobars",
              "--disable-browser-side-navigation","--disable-features=VizDisplayCompositor",
              "--ignore-certificate-errors","--window-size=1920,1080"]:
        opts.add_argument(a)
    ua = os.getenv("CHROME_UA","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    exe = None
    for p in ["/usr/local/bin/chromedriver","/usr/bin/chromedriver","/snap/bin/chromedriver"]:
        if os.path.exists(p): exe = p; break
    if exe is None and HAS_WDM:
        exe = ChromeDriverManager().install()

    service = Service(executable_path=exe) if exe else None
    driver = webdriver.Chrome(service=service, options=opts) if service else webdriver.Chrome(options=opts)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    driver.set_page_load_timeout(75)
    driver.implicitly_wait(2)
    return driver

def _login(driver):
    log("Login…")
    driver.get(f"{BASE}/user/login")
    WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME,"body")))
    sleep()
    if "/user/login" not in driver.current_url:
        return True
    if not FLOW_EMAIL or not FLOW_PASS:
        log("❌ Falta FLOW_EMAIL / FLOW_PASS")
        return False

    def _find_any(cands):
        for sel in cands:
            try: return WebDriverWait(driver,10).until(EC.element_to_be_clickable(sel))
            except Exception: pass
        return None

    email = _find_any([(By.NAME,"user[email]"),(By.ID,"user_email"),(By.CSS_SELECTOR,"input[type='email']")])
    pwd   = _find_any([(By.NAME,"user[password]"),(By.ID,"user_password"),(By.CSS_SELECTOR,"input[type='password']")])
    btn   = _find_any([(By.CSS_SELECTOR,'button[type="submit"]'),
                       (By.XPATH,"//button[contains(.,'Sign') or contains(.,'Log') or contains(.,'Iniciar')]")])
    if not (email and pwd and btn): return False

    email.clear(); email.send_keys(FLOW_EMAIL); sleep()
    pwd.clear();   pwd.send_keys(FLOW_PASS);    sleep()
    btn.click()
    try:
        WebDriverWait(driver, 40).until(lambda d: "/user/login" not in d.current_url)
        sleep()
        return True
    except TimeoutException:
        return False

def _accept_cookies(driver):
    sels = ['button[aria-label="Accept all"]','button[aria-label="Aceptar todo"]',
            '[data-testid="uc-accept-all-button"]','button[mode="primary"]']
    for css in sels:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, css)
            if btns: btns[0].click(); sleep(0.15,0.25); return True
        except Exception: pass
    # best-effort silencioso
    try:
        driver.execute_script("""
          for (const b of document.querySelectorAll('button')) {
            if (/aceptar|accept|consent|agree/i.test(b.textContent)) { b.click(); break; }
          }
        """)
    except Exception: pass
    return True

# ===================== PAGINACIÓN =====================

def _has_pagination(driver):
    """Detecta si hay paginación y devuelve el número total de páginas"""
    try:
        # Estrategia 1: Buscar elementos de paginación comunes
        pagination_selectors = [
            "[data-phx-link='patch']",
            ".pagination",
            "[class*='pagination']",
            "[phx-click*='page']",
            "a[phx-click*='page']"
        ]
        
        for selector in pagination_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    # Extraer números de página del texto
                    page_numbers = []
                    for el in elements:
                        try:
                            text = el.text.strip()
                            # Buscar números en el texto
                            numbers = re.findall(r'\b\d+\b', text)
                            for num in numbers:
                                num_int = int(num)
                                if 1 <= num_int <= 100:  # Rango razonable
                                    page_numbers.append(num_int)
                        except Exception:
                            continue
                    
                    if page_numbers:
                        max_page = max(page_numbers)
                        log(f"🔍 Paginación detectada: {max_page} páginas totales")
                        return max_page
            except Exception:
                continue
        
        # Estrategia 2: Buscar botones específicos de página
        page_buttons = driver.find_elements(By.CSS_SELECTOR, 
            "a[phx-click*='page'], button[phx-click*='page'], [phx-value-page]")
        
        if page_buttons:
            page_numbers = []
            for btn in page_buttons:
                try:
                    # Obtener número de página del atributo phx-value-page
                    page_attr = btn.get_attribute("phx-value-page")
                    if page_attr and page_attr.isdigit():
                        page_num = int(page_attr)
                        if 1 <= page_num <= 100:
                            page_numbers.append(page_num)
                    
                    # Obtener número de página del texto
                    text = btn.text.strip()
                    if text.isdigit():
                        page_num = int(text)
                        if 1 <= page_num <= 100:
                            page_numbers.append(page_num)
                except Exception:
                    continue
            
            if page_numbers:
                max_page = max(page_numbers)
                log(f"🔍 Paginación detectada: {max_page} páginas totales")
                return max_page
        
        # Estrategia 3: Buscar texto que indique paginación
        page_texts = driver.find_elements(By.XPATH, 
            "//*[contains(text(), 'Page') or contains(text(), 'Página') or contains(text(), 'page')]")
        
        for element in page_texts:
            text = element.text
            numbers = re.findall(r'\b\d+\b', text)
            for num in numbers:
                num_int = int(num)
                if 1 <= num_int <= 100:
                    log(f"🔍 Paginación detectada: {num_int} páginas totales")
                    return num_int
        
        log("📄 No se detectó paginación (una sola página)")
        return 0
        
    except Exception as e:
        log(f"⚠️ Error detectando paginación: {e}")
        return 0

def _navigate_to_page(driver, page_num):
    """Navega a una página específica de participantes"""
    try:
        log(f"🔄 Navegando a página {page_num}...")
        
        # Intentar diferentes selectores de paginación
        selectors = [
            f"a[phx-click*='page'][phx-value-page='{page_num}']",
            f"button[phx-click*='page'][phx-value-page='{page_num}']",
            f"[phx-value-page='{page_num}']",
            f"a[data-page='{page_num}']",
            f"//a[contains(text(), '{page_num}') and not(contains(text(), '…'))]",
            f"//button[contains(text(), '{page_num}')]"
        ]
        
        for selector in selectors:
            try:
                if selector.startswith("//"):
                    page_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                else:
                    page_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_btn)
                sleep(0.5, 1)
                driver.execute_script("arguments[0].click();", page_btn)
                
                # Esperar a que cargue la nueva página
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[phx-click='booking_details_show']"))
                )
                sleep(1, 2)  # Esperar a que se estabilice
                
                log(f"✅ Navegación a página {page_num} exitosa")
                return True
                
            except Exception:
                continue
        
        # Si no encuentra botones específicos, intentar con JavaScript
        try:
            driver.execute_script(f"""
                const buttons = Array.from(document.querySelectorAll('a, button'));
                const target = buttons.find(el => {{
                    const text = el.textContent.trim();
                    const pageAttr = el.getAttribute('phx-value-page');
                    return (text === '{page_num}' || pageAttr === '{page_num}') && 
                           (el.getAttribute('phx-click') || '').includes('page');
                }});
                if (target) target.click();
                return !!target;
            """)
            sleep(2, 3)
            return True
        except Exception:
            pass
            
        log(f"❌ No se pudo navegar a la página {page_num}")
        return False
        
    except Exception as e:
        log(f"❌ Error navegando a página {page_num}: {e}")
        return False

# ===================== JS Mapper (PID) =====================

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

// Etiquetas ES + EN (incluye Fecha/Date y Mangas/Runs para schedule)
const simpleFieldLabels = new Set([
  "Dorsal","Guía","Guia","Handler",
  "Perro","Dog",
  "Raza","Breed",
  "Edad","Age",
  "Género","Genero","Gender",
  "Altura (cm)","Altura","Height (cm)",
  "Nombre de Pedigree","Nombre de Pedrigree",
  "País","Pais","Country",
  "Licencia","License number",
  "Equipo","Team",
  "Club",
  "Federación","Federacion","Federation",
  "Fecha","Date","Mangas","Runs"
]);

while (node){
  if (isHeader(node)){
    const t = txt(node); if (t) currentDay = t;
  } else if (isLabel(node)){
    const label = (txt(node) || "");
    const valueEl = nextStrong(node);
    const value = txt(valueEl) || "";

    const l = label.toLowerCase();
    if (l.startsWith("fecha") || l === "date")       { tmpFecha  = value; }
    else if (l.startsWith("mangas") || l === "runs") { tmpMangas = value; }
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

# ===================== Helpers PID & apertura segura =====================

def _collect_booking_ids(driver):
    """Devuelve lista de booking_id únicos presentes en la página - MEJORADO"""
    try:
        # Múltiples estrategias para encontrar participantes
        ids = []
        
        # Estrategia 1: Elementos con phx-click
        ids1 = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("[phx-click='booking_details_show']")
            ).map(el => el.getAttribute("phx-value-booking_id"))
             .filter(Boolean);
        """) or []
        
        # Estrategia 2: Por estructura de tabla/fila
        ids2 = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("tr, [class*='participant'], [class*='booking']")
            ).map(el => {
                const btn = el.querySelector("[phx-click='booking_details_show']");
                return btn ? btn.getAttribute("phx-value-booking_id") : null;
            }).filter(Boolean);
        """) or []
        
        # Estrategia 3: Buscar por texto en botones
        ids3 = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("button, a, div")
            ).map(el => {
                if (el.textContent && /detalle|ver|más|expand|details/i.test(el.textContent)) {
                    return el.getAttribute("phx-value-booking_id");
                }
                return null;
            }).filter(Boolean);
        """) or []
        
        ids = ids1 + ids2 + ids3
        
    except Exception as e:
        log(f"⚠️ Error en collect_booking_ids: {e}")
        ids = []
    
    seen, out = set(), []
    for x in ids:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    
    log(f"🔍 Encontrados {len(out)} booking_ids en esta página")
    return out

def _get_or_open_panel_by_pid(driver, pid):
    """
    Devuelve el elemento del panel de detalles para 'pid'.
    Si ya está abierto y pintado, NO hace click; si no, click + espera de render.
    """
    def panel_ready(el):
        try:
            strongs = el.find_elements(By.XPATH, ".//div[contains(@class,'font-bold') and contains(@class,'text-sm')]")
            grids   = el.find_elements(By.XPATH, ".//div[contains(@class,'grid') and contains(@class,'grid-cols-2')]")
            return bool(strongs or grids)
        except Exception:
            return False

    # 1) ¿ya existe el bloque?
    try:
        el = driver.find_element(By.ID, pid)
        if panel_ready(el):
            return el
    except Exception:
        el = None

    # 2) no existe/está vacío → click en toggle
    sel = f"[phx-click='booking_details_show'][phx-value-booking_id='{pid}']"
    for _ in range(6):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)  # JS click
            WebDriverWait(driver, 6).until(lambda d: d.find_element(By.ID, pid))
            el = driver.find_element(By.ID, pid)
            t0 = time.time()
            while time.time() - t0 < 3:
                if panel_ready(el): return el
                time.sleep(0.15)
            driver.execute_script("window.scrollBy(0, 160);")
        except (StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException):
            time.sleep(0.2)
            continue
    return None

# ===================== Parseo robusto (BeautifulSoup) =====================

def _parse_panel_html(panel_html):
    """
    Empareja cada etiqueta (div.text-gray-500.text-sm) con su siguiente hermano
    fuerte (div.font-bold.text-sm). Detecta "Open ..." y Fecha/Date + Mangas/Runs.
    Devuelve claves canónicas según ALIASES.
    """
    soup = BeautifulSoup(panel_html, "html.parser")

    # 1) Campos simples (etiqueta -> siguiente 'fuerte')
    fields_raw = {}
    for lab in soup.find_all("div", class_=lambda c: c and "text-gray-500" in c.split() and "text-sm" in c.split()):
        label = _clean(lab.get_text())
        sib = lab
        val = ""
        for _ in range(8):
            sib = sib.find_next_sibling("div")
            if not sib: break
            classes = sib.get("class") or []
            if "font-bold" in classes and "text-sm" in classes:
                val = _clean(sib.get_text())
                if val: break
        if label and val and label not in fields_raw:
            fields_raw[label] = val

    # 2) Bloques "Open ..." y Fecha/Date + Mangas/Runs
    open_blocks = []
    headers = soup.find_all("div", class_=lambda c: c and "font-bold" in c.split() and "text-sm" in c.split())
    for h in headers:
        title = _clean(h.get_text())
        if not title.lower().startswith("open "):
            continue
        block = {"titulo": title, "fecha": "", "mangas": ""}
        cur = h
        for _ in range(16):
            cur = cur.find_next_sibling("div")
            if not cur: break
            txt = _clean(cur.get_text())
            classes = cur.get("class") or []
            if "font-bold" in classes and "text-sm" in classes and txt.lower().startswith("open "):
                break
            tl = txt.lower()
            if tl == "fecha" or tl == "date":
                val = cur.find_next_sibling("div")
                block["fecha"] = _clean(val.get_text()) if val else ""
            elif tl == "mangas" or tl == "runs":
                val = cur.find_next_sibling("div")
                block["mangas"] = _clean(val.get_text()) if val else ""
        open_blocks.append(block)

    # 3) Normaliza (ES + EN)
    out = {}
    for k, v in fields_raw.items():
        kk = _clean(k).lower()
        key = ALIASES.get(kk)
        if key:
            out[key] = v
    out["open_blocks"] = open_blocks

    if DEBUG_PARTICIPANTS:
        out["_raw_panel_html"] = panel_html
    return out

# ===================== Fallback XPATH (siguiente hermano) =====================

def _fallback_map_participant(driver, pid):
    """Si el JS falla, empareja label/valor con XPATH (siguiente hermano fuerte)."""
    fields = {}
    try:
        labels = driver.find_elements(
            By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'text-gray-500') and contains(@class,'text-sm')]"
        )
        for lab_el in labels:
            lt = _clean(lab_el.text or "")
            val_el = None
            try:
                val_el = lab_el.find_element(
                    By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
                )
            except Exception:
                val_el = None
            vt = _clean(val_el.text if val_el else "")
            if lt and vt and lt not in fields:
                fields[lt] = vt

        headers = driver.find_elements(
            By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
        )
        schedule = []
        for h in headers:
            t = _clean(h.text or "")
            if not t.lower().startswith("open "): 
                continue
            fecha = h.find_elements(
                By.XPATH, "following-sibling::div[normalize-space()='Fecha' or normalize-space()='Date']/following-sibling::div[contains(@class,'font-bold')][1]"
            )
            mangas = h.find_elements(
                By.XPATH, "following-sibling::div[normalize-space()='Mangas' or normalize-space()='Runs']/following-sibling::div[contains(@class,'font-bold')][1]"
            )
            schedule.append({
                "day": t,
                "fecha": _clean(fecha[0].text if fecha else ""),
                "mangas": _clean(mangas[0].text if mangas else "")
            })
        return {"fields": fields, "schedule": schedule}
    except Exception:
        return {"fields": {}, "schedule": []}

# ===================== Normalizadores & merge =====================

def _parse_altura_cm(s):
    s = _clean(s).replace(",",".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None

def _to_canonical_from_jsfields(js_fields):
    out = {}
    for k, v in (js_fields or {}).items():
        kk = _clean(k).lower()
        key = ALIASES.get(kk)
        if key and v:
            out[key] = _clean(v)
    return out

def _merge_sources(bs_data, js_payload):
    """
    Funde:
      - bs_data: dict con claves canónicas + 'open_blocks'
      - js_payload: dict {fields:{<label>:<valor>}, schedule:[{day,fecha,mangas}]}
    Prioridad: BeautifulSoup manda; JS rellena huecos.
    """
    merged = {}
    bs_fields = {k:v for k,v in (bs_data or {}).items() if k != "open_blocks"}
    merged.update(bs_fields)

    js_fields = _to_canonical_from_jsfields((js_payload or {}).get("fields") or {})
    for k, v in js_fields.items():
        if not merged.get(k):
            merged[k] = v

    merged_ob = (bs_data or {}).get("open_blocks", [])
    if not merged_ob:
        sch = (js_payload or {}).get("schedule") or []
        merged_ob = [
            {"titulo": _clean(b.get("day","")), "fecha": _clean(b.get("fecha","")), "mangas": _clean(b.get("mangas",""))}
            for b in sch if any(b.values())
        ]
    merged["open_blocks"] = merged_ob

    if DEBUG_PARTICIPANTS and bs_data and bs_data.get("_raw_panel_html"):
        merged["_raw_panel_html"] = bs_data["_raw_panel_html"]

    return merged

def _fields_to_participant(eid, ename, plist, pid, ev_title, fields_dict):
    part = {
        "event_id": eid,
        "event_name": ename,
        "participants_url": plist,
        "BinomID": pid,
        "dorsal": fields_dict.get("dorsal",""),
        "guia": fields_dict.get("guia",""),
        "perro": fields_dict.get("perro",""),
        "raza": fields_dict.get("raza",""),
        "edad": fields_dict.get("edad",""),
        "genero": fields_dict.get("genero",""),
        "altura_cm": fields_dict.get("altura_cm",""),
        "nombre_pedigree": fields_dict.get("nombre_pedigree",""),
        "pais": fields_dict.get("pais",""),
        "licencia": fields_dict.get("licencia",""),
        "club": fields_dict.get("club",""),
        "federacion": fields_dict.get("federacion",""),
        "equipo": fields_dict.get("equipo",""),
        "event_title": ev_title or ename,
        "open_blocks": fields_dict.get("open_blocks", []),
    }
    if DEBUG_PARTICIPANTS and fields_dict.get("_raw_panel_html"):
        part["raw_panel_html"] = fields_dict["_raw_panel_html"]
    if part["altura_cm"]: part["altura_cm"] = _parse_altura_cm(part["altura_cm"])
    return part

# ===================== Extracción por evento CON PAGINACIÓN =====================

def extract_event_participants(driver, event, per_event_deadline):
    plist = (event.get("enlaces") or {}).get("participantes","")
    eid   = event.get("id","")
    ename = event.get("nombre","")
    
    out = {
        "event_id": eid, "event_name": ename, "participants_url": plist,
        "participants_count": 0, "participants": [], "estado": "ok",
        "pages_processed": 0, "total_participants_found": 0,
        "timestamp": datetime.now().isoformat()
    }
    
    if not plist:
        out["estado"] = "sin_url"; return out

    try:
        log(f"🌐 Cargando página de participantes: {plist}")
        driver.get(plist)
        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME,"body")))
        _accept_cookies(driver)
        sleep(1, 2)
        
        # Esperar a que carguen los participantes
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
    except Exception as e:
        log(f"❌ Error cargando página de participantes: {e}")
        out["estado"]="timeout"; return out

    # Detectar paginación
    total_pages = _has_pagination(driver)
    current_page = 1
    
    # Aplicar límite de páginas si está configurado
    if MAX_PAGES > 0 and total_pages > MAX_PAGES:
        log(f"📚 Límite aplicado: procesando {MAX_PAGES} de {total_pages} páginas")
        total_pages = MAX_PAGES
    elif total_pages == 0:
        log("📄 Una sola página detectada")
        total_pages = 1

    all_participants = []
    pages_processed = 0

    while current_page <= total_pages:
        if _left(per_event_deadline) <= 0:
            out["estado"] = "timeout"; break

        log(f"📖 Procesando página {current_page} de {total_pages}")

        # Recoger booking ids (PIDs) de la página actual
        pids = _collect_booking_ids(driver)
        
        if not pids:
            log("⚠️ No se encontraron participantes en esta página")
            # Guardar dump para debugging
            try:
                raw_path = Path(OUT_DIR)/"participants"/f"raw_{eid}_page{current_page}.html"
                Path(OUT_DIR,"participants").mkdir(parents=True, exist_ok=True)
                raw_path.write_text(driver.page_source, encoding="utf-8")
                log(f"💾 Dump HTML guardado en {raw_path}")
            except Exception as dump_error:
                log(f"⚠️ No se pudo guardar dump HTML: {dump_error}")
            
            # Si es la primera página y no hay participantes, salir
            if current_page == 1:
                out["estado"] = "empty"
                break
            else:
                # Continuar con siguiente página
                current_page += 1
                continue

        log(f"👥 Encontrados {len(pids)} participantes en página {current_page}")

        # Título largo del evento (solo en primera página)
        ev_title = ""
        if current_page == 1:
            try:
                h1 = driver.find_elements(By.TAG_NAME, "h1")
                if h1: ev_title = _clean(h1[0].text)
            except Exception:
                pass

        # Procesar cada participante en la página actual
        page_participants = []
        for i, pid in enumerate(pids, 1):
            if _left(per_event_deadline) <= 0:
                log("⏰ Tiempo agotado para este evento")
                break

            log(f"  👤 Procesando participante {i}/{len(pids)} (PID: {pid})")
            
            # Abrir (o reutilizar) el panel del participante
            block_el = _get_or_open_panel_by_pid(driver, pid)
            if not block_el:
                log(f"  ⚠️ No se pudo abrir panel para PID {pid}")
                continue

            sleep(0.3, 0.5)

            # 1) HTML del bloque + BS
            html = ""
            try: 
                html = block_el.get_attribute("outerHTML") or ""
            except Exception: 
                html = ""
            
            bs_data = _parse_panel_html(html) if html else {}

            # 2) JS mapping rico
            payload_js = None
            try:
                payload_js = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
            except Exception:
                payload_js = None

            # 3) Fallback XPATH si no hay nada
            if not bs_data and (not payload_js or not isinstance(payload_js, dict)):
                payload_js = _fallback_map_participant(driver, pid)

            # 4) Fusión
            merged_fields = _merge_sources(bs_data, payload_js)

            # 5) Construye participante
            part = _fields_to_participant(eid, ename, plist, pid, ev_title, merged_fields)
            page_participants.append(part)

            sleep(0.15, 0.25)

        all_participants.extend(page_participants)
        pages_processed += 1

        # Navegar a siguiente página si existe
        if total_pages > 1 and current_page < total_pages:
            next_page = current_page + 1
            if _navigate_to_page(driver, next_page):
                current_page = next_page
                sleep(1, 2)  # Esperar entre páginas
            else:
                log("❌ No se pudo navegar a la siguiente página, terminando...")
                break
        else:
            break

    out["participants"] = all_participants
    out["participants_count"] = len(all_participants)
    out["pages_processed"] = pages_processed
    out["total_participants_found"] = len(all_participants)

    log(f"✅ Evento completado: {len(all_participants)} participantes en {pages_processed} páginas")
    return out

# ===================== MAIN =====================

def main():
    print("🚀 MÓDULO 2 (MEJORADO): PARTICIPANTES DETALLADOS CON PAGINACIÓN")
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    (Path(OUT_DIR)/"participants").mkdir(parents=True, exist_ok=True)

    events_path = Path(OUT_DIR)/"01events.json"
    if not events_path.exists():
        log("❌ Falta ./output/01events.json")
        return False

    try:
        events = json.loads(events_path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"❌ Error leyendo 01events.json: {e}")
        return False

    if LIMIT_EVENTS > 0:
        events = events[:LIMIT_EVENTS]

    driver = _get_driver()
    if not _login(driver):
        log("❌ Login falló")
        try: driver.quit()
        except Exception: pass
        return False

    aggregated = []
    aggregated_debug = []
    global_deadline = _deadline(MAX_RUNTIME_MIN*60) if MAX_RUNTIME_MIN>0 else None

    for idx, ev in enumerate(events, 1):
        if global_deadline and _left(global_deadline) <= 0:
            log("⏹️ Tope global alcanzado"); break

        log(f"🎯 Evento {idx}/{len(events)}: {ev.get('nombre','(sin nombre)')}")
        res = extract_event_participants(driver, ev, _deadline(PER_EVENT_MAX_S))

        # guardar por evento
        out_path = Path(OUT_DIR)/"participants"/f"02p_{ev.get('id','idx'+str(idx))}.json"
        out_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")

        aggregated.extend(res.get("participants", []))
        if DEBUG_PARTICIPANTS:
            aggregated_debug.extend(res.get("participants", []))

        sleep(0.5, 1)  # Mayor espera entre eventos

    # agregados
    Path(OUT_DIR,"02participants.json").write_text(json.dumps(aggregated, ensure_ascii=False, indent=2), encoding="utf-8")
    if DEBUG_PARTICIPANTS:
        Path(OUT_DIR,"02participants_debug.json").write_text(json.dumps(aggregated_debug, ensure_ascii=False, indent=2), encoding="utf-8")
        log("💾 Debug guardado en 02participants_debug.json")

    log(f"✅ Total participantes: {len(aggregated)}")
    try: driver.quit()
    except Exception: pass
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
