#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper de participantes para FlowAgility (o similar) con Selenium.

- Lee ./output/01events.json
- Aplica chunking: CHUNK_SIZE + CHUNK_OFFSET
- Respeta LIMIT_EVENTS (0 = sin límite) y MAX_RUNTIME_MIN (corte ordenado)
- Login con email/contraseña (si procede)
- Para cada evento, abre su página de "lista de participantes" y extrae filas

ENV claves que puedes ajustar sin tocar código:
  FLOW_BASE_URL                (default: "https://flowagility.com")
  FLOW_LOGIN_URL               (default: FLOW_BASE_URL + "/login")
  FLOW_SELECTOR_EMAIL          (default: input[name='email'])
  FLOW_SELECTOR_PASSWORD       (default: input[name='password'])
  FLOW_SELECTOR_LOGIN_BTN      (default: button[type='submit'])
  FLOW_SELECTOR_LOGIN_CHECK    (default: a[href*='/logout'], usado para verificar login)
  FLOW_SELECTOR_PARTS_TABLE    (default: "table")
  FLOW_SELECTOR_PARTS_THEAD    (default: "thead tr th")
  FLOW_SELECTOR_PARTS_ROWS     (default: "tbody tr")
  FLOW_SELECTOR_PARTS_CELLS    (default: "td")
  FLOW_PARTS_URL_SUFFIX        (default: "/participants_list")  # si no se da participants_url

Además:
  HEADLESS=true|false
  THROTTLE_EVENT_S_MIN / THROTTLE_EVENT_S_MAX
  PER_EVENT_MAX_S
  MAX_PANELS_PER_EVENT (no usado si no hay subpaneles)
  DEBUG_PARTICIPANTS (si 1, limita a 2 eventos)
  OUT_DIR (default: ./output)

Requisitos:
  pip install selenium beautifulsoup4 python-dotenv lxml requests webdriver-manager
"""

import argparse, json, os, random, sys, time, pathlib, signal, re
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ------------------------ Utilidades ENV/CLI ------------------------

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name, str(default)).strip()
    try:
        return int(v)
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name, str(default)).strip()
    try:
        return float(v)
    except Exception:
        return default

def getenv_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None else default

def parse_args():
    p = argparse.ArgumentParser(description="Scraper de participantes (chunking + límites + Selenium real)")
    p.add_argument("--chunk-size", type=int, default=None, help="Tamaño de tanda (por defecto: env CHUNK_SIZE o 50)")
    p.add_argument("--chunk-offset", type=int, default=None, help="Offset de tanda (por defecto: env CHUNK_OFFSET o 0)")
    p.add_argument("--limit-events", type=int, default=None, help="Limitar nº de eventos (0 = sin límite)")
    p.add_argument("--max-runtime-min", type=int, default=None, help="Tiempo máx. global (min) para cortar ordenadamente")
    return p.parse_args()


# ------------------------ Carga de eventos ------------------------

def load_events(path: pathlib.Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "events" in data:
        events = data["events"]
    else:
        events = data
    if not isinstance(events, list):
        raise ValueError("01events.json no es lista ni dict con 'events'")
    return events

def pick_event_id(ev: dict) -> str:
    for k in ("uuid","id","event_id","slug","code"):
        if isinstance(ev, dict) and k in ev and ev[k]:
            return str(ev[k])
    t = None
    for k in ("title","name"):
        if isinstance(ev, dict) and k in ev and ev[k]:
            t = str(ev[k])
            break
    if not t:
        t = str(ev)[:32]
    return "".join(c for c in t if c.isalnum() or c in ("-","_"))[:48] or f"event_{int(time.time())}"

def pick_event_title(ev: dict) -> str:
    for k in ("title","name"):
        if isinstance(ev, dict) and k in ev and ev[k]:
            return str(ev[k])
    return pick_event_id(ev)

def pick_participants_url(ev: dict) -> Optional[str]:
    # Intenta varias claves razonables
    for k in ("participants_url", "participants_list"):
        u = ev.get(k)
        if u: return str(u)
    # Fallback desde event_url/url + sufijo
    base = ev.get("event_url") or ev.get("url")
    if base:
        suffix = getenv_str("FLOW_PARTS_URL_SUFFIX", "/participants_list")
        return str(base).rstrip("/") + suffix
    return None


# ------------------------ Selenium helpers ------------------------

def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    # Evitar “automation” banners
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Si chromedriver está en PATH, Selenium lo encuentra. Si no, se puede usar webdriver_manager
    try:
        drv = webdriver.Chrome(options=opts)
        return drv
    except WebDriverException as e:
        # Fallback a webdriver_manager si hiciera falta en entorno local
        from webdriver_manager.chrome import ChromeDriverManager
        drv = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
        return drv


def selenium_get(driver: webdriver.Chrome, url: str, timeout: int = 30):
    driver.set_page_load_timeout(timeout)
    driver.get(url)


def wait_css(driver: webdriver.Chrome, css: str, timeout: int = 20):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))


def element_exists(driver: webdriver.Chrome, css: str) -> bool:
    try:
        driver.find_element(By.CSS_SELECTOR, css)
        return True
    except NoSuchElementException:
        return False


# ------------------------ Login ------------------------

def perform_login(driver: webdriver.Chrome) -> bool:
    """
    Realiza login si FLOW_EMAIL/FLOW_PASS están definidos.
    Devuelve True si considera que hay sesión abierta (o no es necesario).
    """
    email = getenv_str("FLOW_EMAIL", "").strip()
    pw = getenv_str("FLOW_PASS", "").strip()
    base = getenv_str("FLOW_BASE_URL", "https://flowagility.com").rstrip("/")
    login_url = getenv_str("FLOW_LOGIN_URL", base + "/login")

    # Si no tenemos credenciales, asumimos que no hace falta login
    if not email or not pw:
        print("[INFO] Sin credenciales → se asume acceso público.", flush=True)
        return True

    print(f"[INFO] Login en {login_url} (headless={getenv_str('HEADLESS','true')})", flush=True)

    try:
        selenium_get(driver, login_url, timeout=30)
    except TimeoutException:
        print("[WARN] Timeout cargando login; sigo e intento localizar formulario…", flush=True)

    sel_email = getenv_str("FLOW_SELECTOR_EMAIL", "input[name='email']")
    sel_pass = getenv_str("FLOW_SELECTOR_PASSWORD", "input[name='password']")
    sel_btn  = getenv_str("FLOW_SELECTOR_LOGIN_BTN", "button[type='submit']")
    sel_check= getenv_str("FLOW_SELECTOR_LOGIN_CHECK", "a[href*='/logout']")

    try:
        wait_css(driver, sel_email, 20)
        driver.find_element(By.CSS_SELECTOR, sel_email).clear()
        driver.find_element(By.CSS_SELECTOR, sel_email).send_keys(email)

        wait_css(driver, sel_pass, 10)
        driver.find_element(By.CSS_SELECTOR, sel_pass).clear()
        driver.find_element(By.CSS_SELECTOR, sel_pass).send_keys(pw)

        driver.find_element(By.CSS_SELECTOR, sel_btn).click()

        # Espera a que aparezca un indicador de login (o redirección)
        WebDriverWait(driver, 25).until(lambda d: element_exists(d, sel_check) or d.current_url != login_url)
        print("[INFO] Login correcto (o navegación tras enviar formulario).", flush=True)
        return True
    except Exception as e:
        print(f"[ERROR] Fallo en login: {e}", flush=True)
        # Aun así, no abortamos: quizá la página era pública
        return element_exists(driver, sel_check)


# ------------------------ Extracción de participantes ------------------------

def header_slugify(h: str) -> str:
    """
    Normaliza el nombre de columna a un alias estable:
    p.ej. 'Nº Licencia' -> 'licencia', 'Guía' -> 'guia', 'Perro' -> 'perro'
    """
    s = (h or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    rep = {
        "nº": "n",
        "núm.": "num",
        "número": "numero",
        "guía": "guia",
        "perr@": "perro",
        "perr€": "perro",
        "raza/size": "raza",
        "tamaño": "altura",
        "talla": "altura",
        "grade": "grado",
    }
    for k, v in rep.items():
        s = s.replace(k, v)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    # Mapas comunes a tus datasets
    alias = {
        "binomid": "BinomID",
        "binom_id": "BinomID",
        "dorsal": "Dorsal",
        "guia": "guia",
        "perro": "perro",
        "raza": "Raza",
        "altura": "Altura",
        "edad": "Edad",
        "genero": "Género",
        "sexo": "Género",
        "club": "Club",
        "licencia": "Licencia",
        "federacion": "Federación",
        "fed": "Federación",
        "pais": "País",
        "equipo": "Equipo",
        "grado": "Grado",
        "nombre_de_pedigree": "Nombre de Pedigree",
        "nombre_pedigree": "Nombre de Pedigree",
        "vel_media": "Vel_media",
    }
    return alias.get(s, s)


def extract_table(driver: webdriver.Chrome) -> Tuple[List[Dict], List[str]]:
    """
    Extrae cabeceras + filas de la tabla de participantes visible en la página actual.
    Usa selectores configurables (ENV) y devuelve (rows, headers_norm).
    """
    sel_table = getenv_str("FLOW_SELECTOR_PARTS_TABLE", "table")
    sel_thead = getenv_str("FLOW_SELECTOR_PARTS_THEAD", "thead tr th")
    sel_rows  = getenv_str("FLOW_SELECTOR_PARTS_ROWS", "tbody tr")
    sel_cells = getenv_str("FLOW_SELECTOR_PARTS_CELLS", "td")

    wait_css(driver, sel_table, 30)
    # Tomamos cabeceras
    headers_raw = [th.text.strip() for th in driver.find_elements(By.CSS_SELECTOR, sel_thead)]
    headers = [header_slugify(h) for h in headers_raw]
    # Si no hay thead, intentar inferir desde primera fila
    if not headers:
        first_row = driver.find_elements(By.CSS_SELECTOR, sel_rows)[:1]
        if first_row:
            n = len(first_row[0].find_elements(By.CSS_SELECTOR, sel_cells))
            headers = [f"col_{i+1}" for i in range(n)]

    out = []
    for tr in driver.find_elements(By.CSS_SELECTOR, sel_rows):
        tds = tr.find_elements(By.CSS_SELECTOR, sel_cells)
        if not tds:
            continue
        values = [td.text.strip() for td in tds]
        # Alinear longitudes
        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        elif len(values) > len(headers):
            values = values[:len(headers)]
        row = dict(zip(headers, values))
        out.append(row)

    return out, headers


def to_participants_schema(rows: List[Dict], event_id: str, event_title: str) -> List[Dict]:
    """
    Mapea las columnas genéricas a tu esquema de salida estándar.
    Si faltan algunas, las deja vacías.
    """
    out = []
    for r in rows:
        out.append({
            "event_id": event_id,
            "event_title": event_title,
            "BinomID": r.get("BinomID") or r.get("binomid") or r.get("binom_id") or "",
            "Dorsal": r.get("Dorsal") or r.get("dorsal") or "",
            "guia": r.get("guia") or r.get("Guia") or "",
            "perro": r.get("perro") or r.get("Perro") or "",
            "Raza": r.get("Raza") or r.get("raza") or "",
            "Edad": r.get("Edad") or r.get("edad") or "",
            "Género": r.get("Género") or r.get("genero") or r.get("sexo") or "",
            "Altura": r.get("Altura") or r.get("altura") or r.get("talla") or "",
            "Nombre de Pedigree": r.get("Nombre de Pedigree") or r.get("nombre_de_pedigree") or "",
            "País": r.get("País") or r.get("pais") or "",
            "Licencia": r.get("Licencia") or r.get("licencia") or "",
            "Club": r.get("Club") or r.get("club") or "",
            "Federación": r.get("Federación") or r.get("federacion") or r.get("fed") or "",
            "Equipo": r.get("Equipo") or r.get("equipo") or "",
            "Grado": r.get("Grado") or r.get("grado") or "",
        })
    return out


# ------------------------ Scrape por evento ------------------------

def scrape_participants_for_event(
    driver: webdriver.Chrome,
    ev: dict,
    per_event_max_s: int = 360
) -> Tuple[List[Dict], Dict]:
    """
    Abre la URL de participantes de un evento y extrae todos los registros visibles.
    """
    t0 = time.time()
    event_id = pick_event_id(ev)
    event_title = pick_event_title(ev)
    parts_url = pick_participants_url(ev)

    if not parts_url:
        raise RuntimeError("No se pudo derivar participants_url para el evento.")

    print(f"[INFO] URL participantes: {parts_url}", flush=True)

    # Cargar página
    try:
        selenium_get(driver, parts_url, timeout=min(60, per_event_max_s))
    except TimeoutException:
        print("[WARN] Timeout cargando lista; aún así intento leer tabla si está presente…", flush=True)

    # Si hay controles como "Mostrar X por página" o paginación:
    # - Este código sólo extrae la tabla visible. Si hay paginación, puedes:
    #   1) Subir "mostrar 1000 por página" si existe
    #   2) Iterar paginación (selector ENV) y concatenar
    # Para mantenerlo general, aquí extraemos la tabla actual.

    rows, headers = extract_table(driver)
    participants = to_participants_schema(rows, event_id, event_title)

    dbg = {
        "event_id": event_id,
        "event_title": event_title,
        "participants_url": parts_url,
        "headers_detected": headers,
        "count": len(participants),
        "duration_s": round(time.time() - t0, 2),
    }
    return participants, dbg


# ------------------------ Main ------------------------

def main():
    args = parse_args()

    # Parámetros
    chunk_size      = args.chunk_size      if args.chunk_size      is not None else getenv_int("CHUNK_SIZE", 50)
    chunk_offset    = args.chunk_offset    if args.chunk_offset    is not None else getenv_int("CHUNK_OFFSET", 0)
    limit_events    = args.limit_events    if args.limit_events    is not None else getenv_int("LIMIT_EVENTS", 0)
    max_runtime_min = args.max_runtime_min if args.max_runtime_min is not None else getenv_int("MAX_RUNTIME_MIN", 55)

    dbg_mode = getenv_int("DEBUG_PARTICIPANTS", 0) == 1

    throttle_event_s_min = getenv_float("THROTTLE_EVENT_S_MIN", 8.0)
    throttle_event_s_max = getenv_float("THROTTLE_EVENT_S_MAX", 14.0)
    per_event_max_s      = getenv_int("PER_EVENT_MAX_S", 360)

    out_dir = pathlib.Path(getenv_str("OUT_DIR", "./output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "participants").mkdir(parents=True, exist_ok=True)

    # Cargar eventos
    events = load_events(out_dir / "01events.json")
    total_events = len(events)
    print(f"[INFO] Total eventos en 01events.json: {total_events}")

    # Slice por chunk
    if chunk_size and chunk_size > 0:
        start = max(0, chunk_offset)
        end = min(total_events, start + chunk_size)
    else:
        start, end = 0, total_events

    events_slice = events[start:end]
    print(f"[INFO] Procesando slice [{start}:{end}] → {len(events_slice)} eventos")

    # Limit opcional
    if limit_events and limit_events > 0:
        events_slice = events_slice[:limit_events]
        print(f"[INFO] Aplicado limit_events={limit_events} → {len(events_slice)} eventos")

    # Debug → fuerza 2
    if dbg_mode:
        events_slice = events_slice[:2]
        print("[DEBUG] DEBUG_PARTICIPANTS=1 → forzando 2 eventos")

    # Corte global
    t_global = time.time()
    deadline = t_global + max_runtime_min * 60

    # Señales corte ordenado
    stop = {"flag": False}
    def handler(sig, frame):
        print(f"[WARN] Señal {sig} → corte ordenado…", flush=True)
        stop["flag"] = True
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    headless = getenv_str("HEADLESS", "true").lower() == "true"
    driver = build_driver(headless=headless)

    all_participants: List[Dict] = []
    per_event_debug: List[Dict] = []

    try:
        # Login si procede
        if not perform_login(driver):
            print("[WARN] No se pudo verificar login. Intento continuar en público…", flush=True)

        for idx, ev in enumerate(events_slice, 1):
            if stop["flag"] or time.time() >= deadline:
                print("[INFO] Tiempo agotado o corte solicitado. Salgo del bucle.", flush=True)
                break

            event_id = pick_event_id(ev)
            event_title = pick_event_title(ev)
            print(f"[INFO] ({idx}/{len(events_slice)}) {event_title} [{event_id}]", flush=True)

            try:
                participants, dbg = scrape_participants_for_event(
                    driver, ev, per_event_max_s=per_event_max_s
                )
            except Exception as e:
                print(f"[ERROR] Evento {event_id} → {e}", file=sys.stderr, flush=True)
                participants, dbg = [], {
                    "event_id": event_id,
                    "event_title": event_title,
                    "error": str(e),
                }

            # Guardado por evento
            per_event_path = out_dir / "participants" / f"02p_{event_id}.json"
            try:
                per_event_path.write_text(json.dumps(participants, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"[ERROR] Escribiendo {per_event_path}: {e}", file=sys.stderr, flush=True)

            all_participants.extend(participants)
            per_event_debug.append(dbg)

            # Throttle entre eventos
            if idx < len(events_slice):
                sleep_s = random.uniform(throttle_event_s_min, throttle_event_s_max)
                if time.time() + sleep_s < deadline:
                    time.sleep(sleep_s)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # Guardados globales
    out_json = out_dir / "02participants.json"
    out_dbg  = out_dir / "02participants_debug.json"

    try:
        out_json.write_text(json.dumps(all_participants, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Escrito {out_json} ({len(all_participants)} participantes)", flush=True)
    except Exception as e:
        print(f"[ERROR] Escribiendo {out_json}: {e}", file=sys.stderr, flush=True)

    try:
        meta = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_events_input": total_events,
            "slice_start": start,
            "slice_end": end,
            "limit_events": limit_events,
            "debug_mode": dbg_mode,
            "max_runtime_min": max_runtime_min,
            "per_event": per_event_debug
        }
        out_dbg.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Escrito {out_dbg}", flush=True)
    except Exception as e:
        print(f"[ERROR] Escribiendo {out_dbg}: {e}", file=sys.stderr, flush=True)

    print("[OK] Finalizado.", flush=True)


if __name__ == "__main__":
    main()
