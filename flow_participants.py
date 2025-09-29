#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - MÓDULO 2: PARTICIPANTES POR EVENTO
- Lee ./output/01events.json
- Para cada evento, visita 'participants_list' y obtiene:
    - numero_participantes (robusto: DOM vivo + fallback HTML)
    - estado: ok | empty | login | timeout | error
- Guarda:
    - ./output/02participants.json  (lista agregada)
    - ./output/participants/02p_<event_id>.json  (uno por evento)
"""

import os
import sys
import json
import re
import time
import traceback
import unicodedata
import random
from datetime import datetime
from pathlib import Path

# Third-party imports
try:
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Falta dependencia: {e}")
    sys.exit(1)

# Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

BASE = "https://www.flowagility.com"
SCRIPT_DIR = Path(__file__).resolve().parent

# ENV
try:
    load_dotenv(SCRIPT_DIR / ".env")
except Exception:
    pass

FLOW_EMAIL = os.getenv("FLOW_EMAILRC", "")
FLOW_PASS  = os.getenv("FLOW_PASSRC",  "")

HEADLESS          = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO         = os.getenv("INCOGNITO", "true").lower() == "true"
OUT_DIR           = os.getenv("OUT_DIR", "./output")
LIMIT_EVENTS      = int(os.getenv("LIMIT_EVENTS", "0"))    # 0 = sin límite
PER_EVENT_MAX_S   = int(os.getenv("PER_EVENT_MAX_S", "180"))
PER_PAGE_MAX_S    = int(os.getenv("PER_PAGE_MAX_S",  "35"))
MAX_RUNTIME_MIN   = int(os.getenv("MAX_RUNTIME_MIN", "0")) # 0 = sin tope global

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
def slow(min_s=0.5, max_s=1.2): time.sleep(random.uniform(min_s, max_s))

def _clean(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def _now(): return time.time()
def _deadline(sec): return _now() + max(0, sec)
def _time_left(dline): return max(0.0, dline - _now())

def _get_driver(headless=True):
    if not HAS_SELENIUM:
        raise ImportError("Selenium no instalado")

    opts = Options()
    if headless: opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-browser-side-navigation")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--window-size=1920,1080")
    ua = os.getenv("CHROME_UA", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin and os.path.exists(chrome_bin):
        opts.binary_location = chrome_bin

    try:
        chromedriver_path = None
        for path in ["/usr/local/bin/chromedriver", "/usr/bin/chromedriver", "/snap/bin/chromedriver"]:
            if os.path.exists(path):
                chromedriver_path = path; break
        if not chromedriver_path and HAS_WEBDRIVER_MANAGER:
            chromedriver_path = ChromeDriverManager().install()

        if chromedriver_path:
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            driver = webdriver.Chrome(options=opts)

        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(75)
        driver.implicitly_wait(2)
        return driver
    except Exception as e:
        log(f"Error creando driver: {e}")
        traceback.print_exc()
        return None

def _login(driver):
    if not driver: return False
    try:
        driver.get(f"{BASE}/user/login")
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        slow(1.2, 2.2)
        if "/user/login" not in driver.current_url:
            log("Ya autenticado"); return True

        # Campos
        email_sel = [(By.NAME,"user[email]"), (By.ID,"user_email"), (By.CSS_SELECTOR,"input[type='email']")]
        pass_sel  = [(By.NAME,"user[password]"), (By.ID,"user_password"), (By.CSS_SELECTOR,"input[type='password']")]
        btn_sel   = [(By.CSS_SELECTOR,'button[type="submit"]'),
                     (By.XPATH,"//button[contains(text(),'Sign') or contains(text(),'Log') or contains(text(),'Iniciar')]")]

        email = None
        for s in email_sel:
            try: email = WebDriverWait(driver,10).until(EC.element_to_be_clickable(s)); break
            except: pass
        if not email: log("No email field"); return False

        pwd = None
        for s in pass_sel:
            try: pwd = driver.find_element(*s); break
            except: pass
        if not pwd: log("No password field"); return False

        btn = None
        for s in btn_sel:
            try: btn = driver.find_element(*s); break
            except: pass
        if not btn: log("No submit button"); return False

        if not FLOW_EMAIL or not FLOW_PASS:
            log("❌ Faltan credenciales FLOW_EMAIL/FLOW_PASS"); return False

        email.clear(); email.send_keys(FLOW_EMAIL); slow(0.3,0.6)
        pwd.clear();   pwd.send_keys(FLOW_PASS);    slow(0.3,0.6)
        btn.click()

        WebDriverWait(driver, 40).until(lambda d: "/user/login" not in d.current_url)
        slow(1.0, 1.8)
        log("✅ Login correcto")
        return True
    except TimeoutException:
        log("❌ Timeout en login"); return False
    except Exception as e:
        log(f"❌ Error en login: {e}"); return False

def _accept_cookies(driver):
    try:
        sels = [
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            '[data-testid="uc-accept-all-button"]',
            'button[mode="primary"]',
        ]
        for css in sels:
            btns = driver.find_elements(By.CSS_SELECTOR, css)
            if btns:
                btns[0].click(); slow(0.2, 0.4); return True
        # Fallback JS
        driver.execute_script("""
            for (const b of document.querySelectorAll('button')) {
              if (/aceptar|accept|consent|agree/i.test(b.textContent)) { b.click(); break; }
            }
        """)
        slow(0.2,0.3)
        return True
    except Exception:
        return False

def _wait_state_participants_page(driver, timeout_s):
    """Determina estado de la página de participantes."""
    t_end = _deadline(timeout_s)
    did_scroll = False
    while _now() < t_end:
        url = (driver.current_url or "")
        if "/user/login" in url: return "login"
        # Conteo rápido en DOM vivo
        try:
            cnt = driver.execute_script("""
                const qs = document.querySelectorAll(
                  '[phx-value-booking_id],'+
                  '[phx-value-booking-id],'+
                  '[data-phx-value-booking_id],'+
                  '[data-phx-value-booking-id],'+
                  '[phx-click*="booking_details"],'+
                  '[data-phx-click*="booking_details"],'+
                  '[id^="booking-"],[id^="booking_"]'
                );
                return qs ? qs.length : 0;
            """) or 0
            if int(cnt) > 0: return "ok"
        except: pass

        # Texto de vacío
        try:
            body_txt = driver.find_element(By.TAG_NAME, "body").text.lower()
            if re.search(r"no hay|sin participantes|no results|0 participantes|no participants", body_txt):
                return "empty"
        except: pass

        # Micro-scroll
        if not did_scroll:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.25)
                driver.execute_script("window.scrollTo(0, 0);")
                did_scroll = True
            except: pass

        time.sleep(0.25)
    return "timeout"

def _count_participants_fast(driver) -> int:
    try:
        result = driver.execute_script("""
            const set = new Set();
            const nodes = document.querySelectorAll(
              '[phx-value-booking_id],'+
              '[phx-value-booking-id],'+
              '[data-phx-value-booking_id],'+
              '[data-phx-value-booking-id],'+
              '[phx-click*="booking_details"],'+
              '[data-phx-click*="booking_details"],'+
              '[id^="booking-"],[id^="booking_"]'
            );
            for (const n of nodes) {
              const v = n.getAttribute('phx-value-booking_id')
                      || n.getAttribute('phx-value-booking-id')
                      || n.getAttribute('data-phx-value-booking_id')
                      || n.getAttribute('data-phx-value-booking-id')
                      || n.id || '';
              if (v) set.add(v);
            }
            return set.size || nodes.length || 0;
        """) or 0
        return int(result) if result else 0
    except Exception:
        return 0

def _count_participants_from_html(html: str) -> int:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        elems = soup.find_all(attrs={'phx-value-booking_id': True}) \
              + soup.find_all(attrs={'phx-value-booking-id': True})
        if elems: return len(elems)

        elems = soup.find_all(attrs={'phx-click': re.compile(r'booking_details')}) \
              + soup.find_all(attrs={'data-phx-click': re.compile(r'booking_details')})
        if elems: return len(elems)

        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) > 1:
                hdr = rows[0].get_text(" ").lower()
                if any(k in hdr for k in ["dorsal","guía","guia","perro","nombre"]):
                    return max(0, len(rows)-1)
                if 5 <= len(rows) <= 2000:
                    return len(rows)-1

        txt = soup.get_text(" ").lower()
        m = re.search(r"(\d+)\s*(participantes?|inscritos?|competidores?)", txt)
        if m:
            n = int(m.group(1))
            if 0 <= n <= 5000: return n
    except Exception:
        pass
    return 0

def main():
    print("🚀 MÓDULO 2: EXTRAER PARTICIPANTES")
    print(f"📂 OUT_DIR: {OUT_DIR}")
    os.makedirs(OUT_DIR, exist_ok=True)
    per_event_dir = Path(OUT_DIR) / "participants"
    per_event_dir.mkdir(parents=True, exist_ok=True)

    events_path = Path(OUT_DIR) / "01events.json"
    if not events_path.exists():
        log("❌ No existe ./output/01events.json. Ejecuta primero el Módulo 1.")
        return False

    events = json.load(open(events_path, "r", encoding="utf-8"))
    if LIMIT_EVENTS > 0:
        events = events[:LIMIT_EVENTS]

    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome"); return False

    try:
        if not _login(driver):
            raise RuntimeError("No se pudo iniciar sesión")

        global_deadline = _deadline(MAX_RUNTIME_MIN * 60) if MAX_RUNTIME_MIN > 0 else None
        results = []

        total = len(events)
        for idx, ev in enumerate(events, 1):
            if global_deadline and _now() >= global_deadline:
                log("⏹️  Tiempo global agotado; guardo y salgo.")
                break

            nombre = ev.get("nombre","N/A")
            eid    = ev.get("id","")
            plist  = (ev.get("enlaces") or {}).get("participantes","")
            if not plist:
                log(f"({idx}/{total}) {nombre}: sin URL de participantes; salto.")
                continue

            log(f"({idx}/{total}) {nombre}: accediendo a participantes…")
            event_deadline = _deadline(PER_EVENT_MAX_S)
            status = "error"; n = 0

            try:
                driver.get(plist)
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                _accept_cookies(driver)

                state = _wait_state_participants_page(driver, timeout_s=min(PER_PAGE_MAX_S, _time_left(event_deadline)))

                if state == "login":
                    log("  Sesión caducada; relogin…")
                    if _login(driver):
                        driver.get(plist)
                        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        _accept_cookies(driver)
                        state = _wait_state_participants_page(driver, timeout_s=min(PER_PAGE_MAX_S, _time_left(event_deadline)))
                    else:
                        state = "timeout"

                if state == "empty":
                    status = "empty"; n = 0; log("  Lista vacía (empty)")
                elif state == "ok":
                    n = _count_participants_fast(driver)
                    if n == 0:
                        n = _count_participants_from_html(driver.page_source)
                    status = "ok" if n > 0 else "empty"
                    log(f"  Conteo participantes: {n}")
                else:
                    status = "timeout"; n = 0; log("  Timeout esperando lista")
            except Exception as e:
                status = "error"; n = 0; log(f"  Error en participantes: {e}")

            record = {
                "id": eid,
                "nombre": nombre,
                "participants_url": plist,
                "numero_participantes": int(n),
                "estado": status,
                "timestamp": datetime.now().isoformat()
            }
            results.append(record)

            # Guardar por evento
            per_path = per_event_dir / f"02p_{eid or idx}.json"
            with open(per_path, "w", encoding="utf-8") as f: json.dump(record, f, ensure_ascii=False, indent=2)
            slow(0.3, 0.8)

        # Guardar agregado
        out_all = Path(OUT_DIR) / "02participants.json"
        with open(out_all, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        # Resumen
        total_ok = sum(1 for r in results if r["estado"] == "ok")
        total_n  = sum(r["numero_participantes"] for r in results)
        log(f"✅ Guardado {len(results)} registros | {total_ok} OK | total participantes: {total_n}")
        return True

    except Exception as e:
        log(f"❌ Error global: {e}")
        traceback.print_exc()
        return False
    finally:
        try: driver.quit()
        except: pass

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
