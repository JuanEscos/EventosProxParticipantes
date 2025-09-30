#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - MÓDULO 2 (DEBUG): PARTICIPANTES (DETALLE + RAW HTML)
- Lee ./output/01events.json
- Para cada evento: visita participants_list, abre cada participante,
  extrae campos del panel Binomio + Pruebas RFEC + bloques "Open ..."
- Salidas:
  * ./output/02participants.json (lista agregada, limpia)
  * ./output/02participants_debug.json (si DEBUG_PARTICIPANTS=1, incluye raw_panel_html)
  * ./output/participants/02p_<event_id>.json (detalle por evento)

ENV útiles:
  FLOW_EMAIL / FLOW_PASS (login)
  HEADLESS=true/false
  INCOGNITO=true/false
  OUT_DIR=./output
  LIMIT_EVENTS=0  (0 = sin límite)
  PER_EVENT_MAX_S=240
  MAX_RUNTIME_MIN=0 (0 = sin tope)
  DEBUG_PARTICIPANTS=1 para generar 02participants_debug.json con raw_panel_html
"""

import os, sys, json, re, time, unicodedata, random, traceback
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
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, StaleElementReferenceException

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

FLOW_EMAIL = os.getenv("FLOW_EMAIL", "")
FLOW_PASS  = os.getenv("FLOW_PASS", "")

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
    if "/user/login" not in driver.current_url: return True

    if not FLOW_EMAIL or not FLOW_PASS:
        log("❌ Falta FLOW_EMAIL / FLOW_PASS"); return False

    def _find_any(cands):
        for sel in cands:
            try:
                return WebDriverWait(driver,10).until(EC.element_to_be_clickable(sel))
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
    # fallback JS best-effort
    try:
        driver.execute_script("""
          for (const b of document.querySelectorAll('button')) {
            if (/aceptar|accept|consent|agree/i.test(b.textContent)) { b.click(); break; }
          }
        """); sleep(0.2,0.3)
    except: pass
    return True

# ---------- helpers participantes ----------

ALIASES = {
    "dorsal":"dorsal","guía":"guia","guia":"guia","perro":"perro","raza":"raza",
    "edad":"edad","género":"genero","genero":"genero","altura (cm)":"altura_cm",
    "club":"club","licencia":"licencia","federación":"federacion","federacion":"federacion",
}

def _parse_open_blocks(panel_soup):
    blocks = []
    for h in panel_soup.find_all("div", class_=re.compile(r"\bfont-bold\b.*\btext-sm\b")):
        title = _clean(h.get_text())
        if not title.lower().startswith("open "): continue
        block = {"titulo": title, "fecha": "", "mangas": ""}
        for sib in h.find_next_siblings("div"):
            txt = _clean(sib.get_text())
            cls = " ".join(sib.get("class", []))
            # si aparece otro título fuerte de Open, cerramos bloque
            if re.search(r"\bfont-bold\b.*\btext-sm\b", cls) and "open" in txt.lower(): break
            if txt.lower() == "fecha":
                val = sib.find_next_sibling("div")
                block["fecha"] = _clean(val.get_text()) if val else ""
            if txt.lower() == "mangas":
                val = sib.find_next_sibling("div")
                block["mangas"] = _clean(val.get_text()) if val else ""
        blocks.append(block)
    return blocks

def _parse_panel_html(panel_html):
    """Devuelve dict con campos normalizados + (opcional) _raw_panel_html."""
    soup = BeautifulSoup(panel_html, "html.parser")
    data = {}
    # 1) pares etiqueta/valor
    for lab in soup.select("div.text-gray-500.text-sm"):
        label = _clean(lab.get_text()).lower()
        val_div = lab.find_next_sibling("div")
        value = _clean(val_div.get_text()) if val_div else ""
        key = ALIASES.get(label)
        if key and value:
            data[key] = value
    # 2) bloques Open …
    data["open_blocks"] = _parse_open_blocks(soup)
    if DEBUG_PARTICIPANTS:
        data["_raw_panel_html"] = panel_html
    return data

def _wait_panel_and_parse(driver):
    """Espera el último panel 'Binomio' y lo parsea."""
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class,'grid') and contains(@class,'grid-cols-2')][.//div[contains(normalize-space(.),'Binomio')]]")
            )
        )
        panels = driver.find_elements(
            By.XPATH, "//div[contains(@class,'grid') and contains(@class,'grid-cols-2')][.//div[contains(normalize-space(.),'Binomio')]]"
        )
        if not panels: return {}
        panel = panels[-1]
        html = panel.get_attribute("outerHTML")
        return _parse_panel_html(html)
    except Exception:
        return {}

def _find_all_toggles(driver):
    sels = [
        '[phx-click*="booking_details"]',
        '[data-phx-click*="booking_details"]',
        '[phx-value-booking_id]', '[phx-value-booking-id]',
        '[data-phx-value-booking_id]', '[data-phx-value-booking-id]',
        '[id^="booking-"], [id^="booking_"]'
    ]
    els = driver.find_elements(By.CSS_SELECTOR, ", ".join(sels))
    vis = []
    for e in els:
        try:
            if e.is_displayed() and e.size.get("height",0) > 8:
                vis.append(e)
        except: pass
    return vis

def _click_safely(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        sleep(0.1,0.2)
        el.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except: return False
    except: return False

def _parse_age_meses(s):
    s = _clean(s).lower()
    m = re.search(r"(\d+)\s*mes", s)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*a[nñ]o", s)
    if m: return int(m.group(1))*12
    return None

def _parse_altura_cm(s):
    s = _clean(s).replace(",",".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None

def extract_event_participants(driver, event, per_event_deadline):
    """Procesa una URL de participantes y devuelve dict con participants[]."""
    url = (event.get("enlaces") or {}).get("participantes","")
    eid = event.get("id","")
    ename = event.get("nombre","")
    out = {
        "event_id": eid,
        "event_name": ename,
        "participants_url": url,
        "participants_count": 0,
        "participants": [],
        "estado": "ok",
        "timestamp": datetime.now().isoformat()
    }
    if not url:
        out["estado"]="sin_url"; return out
    try:
        driver.get(url)
        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME,"body")))
        _accept_cookies(driver)
    except Exception as e:
        out["estado"]="timeout"; return out

    toggles = _find_all_toggles(driver)
    if not toggles:
        out["estado"]="empty"; return out

    for idx, t in enumerate(toggles, 1):
        if _left(per_event_deadline) <= 0:
            out["estado"]="timeout"; break
        if not _click_safely(driver, t): continue

        # margen para hidratar el panel
        sleep(0.2,0.4)

        # 1ª pasada
        data = _wait_panel_and_parse(driver)
        # Fallback si faltan claves críticas
        if not data or (not data.get("dorsal") and not data.get("guia") and not data.get("perro")):
            sleep(0.3,0.5)
            data = _wait_panel_and_parse(driver)

        if not data:
            _click_safely(driver, t)  # cerrar si abrió
            continue

        p = {
            "event_id": eid, "event_name": ename,
            "dorsal": data.get("dorsal",""),
            "guia": data.get("guia",""),
            "perro": data.get("perro",""),
            "raza": data.get("raza",""),
            "edad_meses": _parse_age_meses(data.get("edad","")),
            "genero": data.get("genero",""),
            "altura_cm": _parse_altura_cm(data.get("altura_cm","")),
            "club": data.get("club",""),
            "licencia": data.get("licencia",""),
            "federacion": data.get("federacion",""),
            "open_blocks": data.get("open_blocks", []),
        }
        if DEBUG_PARTICIPANTS:
            p["raw_panel_html"] = data.get("_raw_panel_html", "")

        out["participants"].append(p)
        out["participants_count"] = len(out["participants"])

        # cerrar el panel para mantener el "último grid" coherente
        _click_safely(driver, t)
        sleep(0.1,0.2)

    return out

def main():
    print("🚀 MÓDULO 2 (DEBUG): PARTICIPANTES DETALLADOS")
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    (Path(OUT_DIR)/"participants").mkdir(parents=True, exist_ok=True)

    events_path = Path(OUT_DIR)/"01events.json"
    if not events_path.exists():
        log("❌ Falta ./output/01events.json")
        return False

    events = json.load(open(events_path,"r",encoding="utf-8"))
    if LIMIT_EVENTS>0: events = events[:LIMIT_EVENTS]

    driver = _get_driver()
    if not _login(driver):
        log("❌ Login falló"); 
        try: driver.quit()
        except: pass
        return False

    aggregated = []
    aggregated_debug = []
    start_global = _deadline(MAX_RUNTIME_MIN*60) if MAX_RUNTIME_MIN>0 else None

    for i, ev in enumerate(events,1):
        if start_global and _left(start_global) <= 0:
            log("⏹️ Tope global alcanzado"); break

        log(f"Evento {i}/{len(events)}: {ev.get('nombre')}")
        res = extract_event_participants(driver, ev, _deadline(PER_EVENT_MAX_S))

        # Guardar por evento
        out_path = Path(OUT_DIR)/"participants"/f"02p_{ev.get('id','idx'+str(i))}.json"
        with open(out_path,"w",encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)

        # Agregados
        aggregated.extend(res.get("participants", []))
        if DEBUG_PARTICIPANTS:
            aggregated_debug.extend(res.get("participants", []))

        sleep(0.3,0.7)

    # Guardar agregados
    with open(Path(OUT_DIR)/"02participants.json","w",encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, indent=2)
    if DEBUG_PARTICIPANTS:
        with open(Path(OUT_DIR)/"02participants_debug.json","w",encoding="utf-8") as f:
            json.dump(aggregated_debug, f, ensure_ascii=False, indent=2)
        log("💾 Debug guardado en 02participants_debug.json")

    log(f"✅ Total participantes: {len(aggregated)}")
    try: driver.quit()
    except: pass
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)

