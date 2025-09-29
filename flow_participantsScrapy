#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - MÓDULO 2: PARTICIPANTES (DETALLE)
- Lee ./output/01events.json
- Para cada evento, visita participants_list y:
  * Hace click en cada toggle de participante (booking_details)
  * Extrae campos del panel "Binomio" + "Pruebas RFEC" + bloques "Open ..."
- Salida:
  * ./output/02participants.json  (lista agregada de participantes de todos los eventos)
  * ./output/participants/02p_<event_id>.json (por evento: resumen + participants[])
"""

import os, sys, json, re, time, unicodedata, random, traceback
from datetime import datetime
from pathlib import Path

# 3rd party
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Selenium
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

FLOW_EMAIL = os.getenv("FLOW_EMAILRQ", "")
FLOW_PASS  = os.getenv("FLOW_PASSRQ",  "")

HEADLESS        = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO       = os.getenv("INCOGNITO", "true").lower() == "true"
OUT_DIR         = os.getenv("OUT_DIR", "./output")
LIMIT_EVENTS    = int(os.getenv("LIMIT_EVENTS", "0"))
PER_EVENT_MAX_S = int(os.getenv("PER_EVENT_MAX_S", "240"))
PER_PAGE_MAX_S  = int(os.getenv("PER_PAGE_MAX_S",  "35"))
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))  # 0 = sin tope

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

    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin and os.path.exists(chrome_bin):
        opts.binary_location = chrome_bin

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
        log("❌ Faltan FLOW_EMAIL / FLOW_PASS"); return False

    # localizar campos
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
    if not (email and pwd and btn):
        log("❌ No se encuentran campos de login"); return False

    email.clear(); email.send_keys(FLOW_EMAIL); sleep()
    pwd.clear();   pwd.send_keys(FLOW_PASS);    sleep()
    btn.click()
    try:
        WebDriverWait(driver, 40).until(lambda d: "/user/login" not in d.current_url)
        sleep(0.8,1.5)
        log("✅ Login ok")
        return True
    except TimeoutException:
        log("❌ Timeout tras login"); return False

def _accept_cookies(driver):
    sels = ['button[aria-label="Accept all"]','button[aria-label="Aceptar todo"]',
            '[data-testid="uc-accept-all-button"]','button[mode="primary"]']
    for css in sels:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, css)
            if btns:
                btns[0].click(); sleep(0.2,0.4); return True
        except: pass
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

def _parse_open_blocks(soup):
    """Devuelve lista de bloques 'Open X' con Fecha y Mangas."""
    out = []
    # Cualquier div que empiece por 'Open ' y sea título de bloque
    for h in soup.find_all("div", class_=re.compile(r"\bfont-bold\b")):
        title = _clean(h.get_text())
        if title.lower().startswith("open "):
            block = {"titulo": title, "fecha":"", "mangas":""}
            # buscar parejas label/valor posteriores inmediatas
            cur = h
            # recoge las dos siguientes parejas "Fecha"/valor y "Mangas"/valor
            pairs = []
            # Busca en los siguientes siblings inmediatos hasta otro título o fin
            sib = h.find_next_siblings("div", limit=6)  # suele estar cerca
            for i in range(0, len(sib)-1, 2):
                lab = _clean(sib[i].get_text())
                val = _clean(sib[i+1].get_text())
                if lab.lower() in ("fecha","mangas"):
                    pairs.append((lab,val))
            for lab,val in pairs:
                if lab.lower()=="fecha":  block["fecha"]=val
                if lab.lower()=="mangas": block["mangas"]=val
            out.append(block)
    return out

def _extract_label_value_pairs(grid_div):
    """Del panel .grid.grid-cols-2 extrae dict con claves normalizadas."""
    data = {}
    cells = grid_div.find_all("div", recursive=False)
    # fallback: si recursive=False no pilla, usa all y empareja en parejas label/valor
    if not cells:
        cells = grid_div.find_all("div")
    # recorrer por parejas (label, value)
    for i in range(0, len(cells)-1, 2):
        label = _clean(cells[i].get_text()).lower()
        value = _clean(cells[i+1].get_text())
        if not label or not value: continue
        key = ALIASES.get(label)
        if key:
            data[key] = value
    # bloques Open...
    data["open_blocks"] = _parse_open_blocks(grid_div)
    return data

def _find_all_toggles(driver):
    # elementos que abren detalles (varias variantes)
    sels = [
        '[phx-click*="booking_details"]',
        '[data-phx-click*="booking_details"]',
        '[phx-value-booking_id]', '[phx-value-booking-id]',
        '[data-phx-value-booking_id]', '[data-phx-value-booking-id]',
        '[id^="booking-"], [id^="booking_"]'
    ]
    els = driver.find_elements(By.CSS_SELECTOR, ", ".join(sels))
    # filtrar visibles/clicables
    vis = []
    for e in els:
        try:
            if e.is_displayed() and e.size.get("height",0) > 8:
                vis.append(e)
        except: pass
    # deduplicar por referencia DOM (hash id)
    seen = set()
    uniq = []
    for e in vis:
        try:
            key = e.get_attribute("id") or e.get_attribute("phx-value-booking_id") \
                or e.get_attribute("phx-value-booking-id") or e.get_attribute("data-phx-value-booking_id") \
                or e.get_attribute("data-phx-value-booking-id") or str(hash(e))
            if key not in seen:
                seen.add(key); uniq.append(e)
        except: pass
    return uniq

def _click_safely(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        sleep(0.15,0.30)
        el.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False
    except Exception:
        return False

def _wait_panel_and_parse(driver):
    """Espera un panel de detalles visible y devuelve dict parseado."""
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.grid.grid-cols-2"))
        )
        # Usamos el ÚLTIMO panel grid (el recién abierto suele añadirse al final)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        grids = soup.select("div.grid.grid-cols-2")
        if not grids:
            return {}
        panel = grids[-1]
        # Debe contener 'Binomio' en algún título cercano
        if "binomio" not in panel.get_text(" ").lower():
            # buscar el más cercano que tenga Binomio
            for g in reversed(grids):
                if "binomio" in g.get_text(" ").lower():
                    panel = g; break
        return _extract_label_value_pairs(panel)
    except Exception:
        return {}

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
    """Devuelve dict con resumen y lista participants[]."""
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
        out["estado"]="timeout"
        return out

    # localizar toggles
    toggles = _find_all_toggles(driver)
    if not toggles:
        out["estado"]="empty"
        return out

    # recorre cada participante
    for idx, t in enumerate(toggles, 1):
        if _left(per_event_deadline) <= 0: 
            out["estado"]="timeout"; break

        ok = _click_safely(driver, t)
        if not ok:
            continue
        # esperar y parsear panel
        data = _wait_panel_and_parse(driver)
        if not data:
            continue

        # normalizaciones
        p = {
            "event_id": eid,
            "event_name": ename,
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
        out["participants"].append(p)
        out["participants_count"] = len(out["participants"])
        # cerrar el panel (otro click) para no acumular demasiados
        _click_safely(driver, t)
        sleep(0.15,0.30)

    return out

def main():
    print("🚀 MÓDULO 2: PARTICIPANTES (DETALLE)")
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    (Path(OUT_DIR)/"participants").mkdir(parents=True, exist_ok=True)

    events_path = Path(OUT_DIR)/"01events.json"
    if not events_path.exists():
        log("❌ Falta ./output/01events.json. Ejecuta Módulo 1 o usa el YAML que lo descarga/genera.")
        return False

    events = json.load(open(events_path,"r",encoding="utf-8"))
    if LIMIT_EVENTS>0:
        events = events[:LIMIT_EVENTS]

    driver = _get_driver()
    if not _login(driver):
        log("❌ Login falló"); 
        try: driver.quit()
        except: pass
        return False

    global_deadline = _deadline(MAX_RUNTIME_MIN*60) if MAX_RUNTIME_MIN>0 else None
    aggregated = []
    ok_events = 0

    for i, ev in enumerate(events,1):
        if global_deadline and _left(global_deadline) <= 0:
            log("⏹️ Tope global alcanzado"); break
        log(f"Evento {i}/{len(events)}: {ev.get('nombre','(sin nombre)')}")
        per_event_dl = _deadline(PER_EVENT_MAX_S)
        res = extract_event_participants(driver, ev, per_event_dl)
        aggregated.extend(res["participants"])
        # guardar por evento
        out_path = Path(OUT_DIR)/"participants"/f"02p_{ev.get('id','idx'+str(i))}.json"
        with open(out_path,"w",encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        if res["participants_count"]>0: ok_events += 1
        sleep(0.3,0.7)

    # guardar agregado
    all_out = Path(OUT_DIR)/"02participants.json"
    with open(all_out,"w",encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, indent=2)

    log(f"✅ Participantes totales: {len(aggregated)} | Eventos con datos: {ok_events}")
    try: driver.quit()
    except: pass
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
