#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - MÓDULO 2 (DEBUG, v2 PID): PARTICIPANTES DETALLADOS
- Lee ./output/01events.json
- Para cada evento: abre participants_list, detecta booking_id (PID),
  click por PID, espera bloque #PID y mapea campos con JS (y fallback).
- Salidas:
  * ./output/02participants.json
  * ./output/02participants_debug.json (si DEBUG_PARTICIPANTS=1)
  * ./output/participants/02p_<event_id>.json
  * ./output/participants/raw_<event_id>.html (dump si no hay toggles)
"""

import os, sys, json, re, time, unicodedata, random
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

# ENV
try: load_dotenv(SCRIPT_DIR / ".env")
except: pass

FLOW_EMAIL = os.getenv("FLOW_EMAILRQ", "")
FLOW_PASS  = os.getenv("FLOW_PASSRQ", "")

HEADLESS        = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO       = os.getenv("INCOGNITO", "true").lower() == "true"
OUT_DIR         = os.getenv("OUT_DIR", "./output")
LIMIT_EVENTS    = int(os.getenv("LIMIT_EVENTS", "0"))
PER_EVENT_MAX_S = int(os.getenv("PER_EVENT_MAX_S", "240"))
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
DEBUG_PARTICIPANTS = os.getenv("DEBUG_PARTICIPANTS", "0") == "1"

def log(s): print(f"[{datetime.now().strftime('%H:%M:%S')}] {s}")
def sleep(a=0.25,b=0.6): time.sleep(random.uniform(a,b))

def _clean(s):
    if s is None: return ""
    s = unicodedata.normalize("NFKC", str(s)).strip()
    s = re.sub(r"[ \t]+", " ", s)
    return s

def _now(): return time.time()
def _deadline(sec): return _now()+max(0,sec)
def _left(dl): return max(0.0, dl - _now())

def _get_driver():
    opts = Options()
    if HEADLESS: opts.add_argument("--headless=new")
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
    sleep(0.8,1.5)
    if "/user/login" not in driver.current_url:
        return True
    if not FLOW_EMAIL or not FLOW_PASS:
        log("❌ Falta FLOW_EMAIL / FLOW_PASS"); return False

    def _find_any(cands):
        for sel in cands:
            try: return WebDriverWait(driver,10).until(EC.element_to_be_clickable(sel))
            except: pass
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
        sleep(0.8,1.5)
        return True
    except TimeoutException:
        return False

def _accept_cookies(driver):
    sels = ['button[aria-label="Accept all"]','button[aria-label="Aceptar todo"]',
            '[data-testid="uc-accept-all-button"]','button[mode="primary"]']
    for css in sels:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, css)
            if btns: btns[0].click(); sleep(0.2,0.4); return True
        except: pass
    return True

# ====== JS para mapear panel ======
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

# ====== helpers PID ======
def _collect_booking_ids(driver):
    """Devuelve lista de booking_id únicos presentes en la página."""
    try:
        ids = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("[phx-click='booking_details_show']")
            ).map(el => el.getAttribute("phx-value-booking_id"))
             .filter(Boolean);
        """) or []
    except Exception:
        ids = []
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _click_toggle_by_pid(driver, pid):
    sel = f"[phx-click='booking_details_show'][phx-value-booking_id='{pid}']"
    for _ in range(6):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)  # JS click evita overlays
            # Espera a que aparezca el bloque con id=pid
            WebDriverWait(driver, 8).until(lambda d: d.find_element(By.ID, pid))
            return driver.find_element(By.ID, pid)
        except (StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException):
            sleep(0.2, 0.5)
            try: driver.execute_script("window.scrollBy(0, 160);")
            except Exception: pass
            continue
    return None

def _fallback_map_participant(driver, pid):
    """Si el JS no devuelve payload, intenta emparejar label/valor vía XPATH en #pid."""
    fields = {}
    try:
        labels = driver.find_elements(
            By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'text-gray-500') and contains(@class,'text-sm')]"
        )
        values = driver.find_elements(
            By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'font-bold') and contains(@class,'text-sm')]"
        )
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
    except Exception:
        return {"fields": {}, "schedule": []}

# ====== mapeo final ======
def _fields_to_participant(eid, ename, plist, pid, ev_title, payload):
    fields = payload.get("fields") or {}
    schedule = payload.get("schedule") or []

    def pick(keys, default=""):
        for k in keys:
            v = fields.get(k)
            if v: return _clean(v)
        return default

    part = {
        "event_id": eid,
        "event_name": ename,
        "participants_url": plist,
        "BinomID": pid,  # por si lo quieres conservar
        "dorsal": pick(["Dorsal"]),
        "guia": pick(["Guía","Guia"]),
        "perro": pick(["Perro"]),
        "raza": pick(["Raza"]),
        "edad": pick(["Edad"]),
        "genero": pick(["Género","Genero"]),
        "altura_cm": pick(["Altura (cm)","Altura"]),
        "nombre_pedigree": pick(["Nombre de Pedigree","Nombre de Pedrigree"]),
        "pais": pick(["País","Pais"]),
        "licencia": pick(["Licencia"]),
        "club": pick(["Club"]),
        "federacion": pick(["Federación","Federacion"]),
        "equipo": pick(["Equipo"]),
        "event_title": ev_title or ename,
        "open_blocks": [
            {"titulo": _clean(b.get("day","")),
             "fecha": _clean(b.get("fecha","")),
             "mangas": _clean(b.get("mangas",""))}
            for b in schedule if any(b.values())
        ],
    }
    if DEBUG_PARTICIPANTS and "_raw_panel_html" in payload:
        part["raw_panel_html"] = payload["_raw_panel_html"]
    return part

# ====== flujo por evento ======
def extract_event_participants(driver, event, per_event_deadline):
    plist = (event.get("enlaces") or {}).get("participantes","")
    eid   = event.get("id","")
    ename = event.get("nombre","")
    out = {
        "event_id": eid, "event_name": ename, "participants_url": plist,
        "participants_count": 0, "participants": [], "estado": "ok",
        "timestamp": datetime.now().isoformat()
    }
    if not plist:
        out["estado"] = "sin_url"; return out

    try:
        driver.get(plist)
        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME,"body")))
        _accept_cookies(driver)
        sleep(0.6,1.0)
    except Exception:
        out["estado"]="timeout"; return out

    # recoger booking ids (PIDs)
    pids = _collect_booking_ids(driver)
    if not pids:
        out["estado"]="empty"
        # Guardar dump de la página para afinar selectores
        try:
            raw_path = Path(OUT_DIR)/"participants"/f"raw_{eid}.html"
            Path(OUT_DIR,"participants").mkdir(parents=True, exist_ok=True)
            raw_path.write_text(driver.page_source, encoding="utf-8")
            log(f"💾 Dump HTML guardado en {raw_path}")
        except Exception as e:
            log(f"⚠️ No se pudo guardar dump HTML: {e}")
        return out

    log(f"Encontrados {len(pids)} booking_id (muestras).")

    # titulo largo del evento (por si lo necesitas)
    ev_title = ""
    try:
        h1 = driver.find_elements(By.TAG_NAME, "h1")
        if h1: ev_title = _clean(h1[0].text)
    except Exception:
        pass

    for i, pid in enumerate(pids, 1):
        if _left(per_event_deadline) <= 0:
            out["estado"] = "timeout"; break

        # Click para desplegar bloque
        block_el = _click_toggle_by_pid(driver, pid)
        if not block_el:
            continue

        # Pequeña espera de render
        sleep(0.2, 0.4)

        # Intento 1: JS rico
        payload = None
        try:
            payload = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
        except Exception:
            payload = None

        # Fallback: parse label/valor por XPath
        if not payload or not isinstance(payload, dict):
            payload = _fallback_map_participant(driver, pid)

        # Construye participante
        part = _fields_to_participant(eid, ename, plist, pid, ev_title, payload)
        out["participants"].append(part)
        out["participants_count"] = len(out["participants"])

        # Cierra bloque (click de nuevo)
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", block_el)
            driver.execute_script("arguments[0].click();", block_el)  # casi todos cierran al click sobre header
        except Exception:
            try:
                # si el bloque no cierra así, intenta clickar el botón otra vez
                _click_toggle_by_pid(driver, pid)
            except Exception:
                pass

        sleep(0.12, 0.25)

    return out

# ====== MAIN ======
def main():
    print("🚀 MÓDULO 2 (DEBUG v2 PID): PARTICIPANTES DETALLADOS")
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    (Path(OUT_DIR)/"participants").mkdir(parents=True, exist_ok=True)

    events_path = Path(OUT_DIR)/"01events.json"
    if not events_path.exists():
        log("❌ Falta ./output/01events.json")
        return False

    events = json.loads(events_path.read_text(encoding="utf-8"))
    if LIMIT_EVENTS > 0:
        events = events[:LIMIT_EVENTS]

    driver = _get_driver()
    if not _login(driver):
        log("❌ Login falló")
        try: driver.quit()
        except: pass
        return False

    aggregated = []
    aggregated_debug = []
    global_deadline = _deadline(MAX_RUNTIME_MIN*60) if MAX_RUNTIME_MIN>0 else None

    for idx, ev in enumerate(events, 1):
        if global_deadline and _left(global_deadline) <= 0:
            log("⏹️ Tope global alcanzado"); break

        log(f"Evento {idx}/{len(events)}: {ev.get('nombre','(sin nombre)')}")
        res = extract_event_participants(driver, ev, _deadline(PER_EVENT_MAX_S))

        # guardar por evento
        out_path = Path(OUT_DIR)/"participants"/f"02p_{ev.get('id','idx'+str(idx))}.json"
        out_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")

        aggregated.extend(res.get("participants", []))
        if DEBUG_PARTICIPANTS:
            aggregated_debug.extend(res.get("participants", []))

        sleep(0.3, 0.7)

    # agregados
    Path(OUT_DIR,"02participants.json").write_text(json.dumps(aggregated, ensure_ascii=False, indent=2), encoding="utf-8")
    if DEBUG_PARTICIPANTS:
        Path(OUT_DIR,"02participants_debug.json").write_text(json.dumps(aggregated_debug, ensure_ascii=False, indent=2), encoding="utf-8")
        log("💾 Debug guardado en 02participants_debug.json")

    log(f"✅ Total participantes: {len(aggregated)}")
    try: driver.quit()
    except: pass
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
