import asyncio
import os
import aiohttp
import pandas as pd
from datetime import date, timedelta, datetime
from pathlib import Path
from io import StringIO
import unicodedata
import locale
from playwright.async_api import async_playwright

print(">>> SCRIPT CARGADO <<<", flush=True)

# ======================================================
# ENV
# ======================================================
def _require_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return val

IDERM_USER = _require_env("IDERM_USER")
IDERM_PASS = _require_env("IDERM_PASS")

RESPONDIO_TOKENS = {
    "sabino": _require_env("RESPONDIO_TOKEN_SABINO"),
    "bori":   _require_env("RESPONDIO_TOKEN_BORI"),
    "cdc":    _require_env("RESPONDIO_TOKEN_CDC"),
    "rey":    _require_env("RESPONDIO_TOKEN_REY"),
}

# ======================================================
# LOCALE (NO DEPENDER DE ESTO)
# ======================================================
try:
    locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
except locale.Error:
    pass

DIAS_ES = ["lunes","martes","miercoles","jueves","viernes","sabado","domingo"]
MESES_ES = [
    "enero","febrero","marzo","abril","mayo","junio",
    "julio","agosto","septiembre","octubre","noviembre","diciembre"
]
MESES_ABR_ES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]

# ======================================================
# PATHS (DOCKER)
# ======================================================
BASE_DIR = Path("/data")
BASE_DIR.mkdir(exist_ok=True)

AUX_PATH = BASE_DIR / "aux_recordatorios.xlsx"
CSV_BASE = BASE_DIR / "CSV" / "Iderma"
(CSV_BASE / "Sabino").mkdir(parents=True, exist_ok=True)
(CSV_BASE / "Bori").mkdir(parents=True, exist_ok=True)

# ======================================================
# RESPOND.IO
# ======================================================
BASE_URL = "https://api.respond.io/v2"

def authenticate(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def convertir_fecha_iso(raw):
    if not raw or str(raw).strip() == "":
        return ""
    try:
        return datetime.strptime(raw, "%m/%d/%y").strftime("%Y-%m-%d")
    except:
        return ""

async def get_contact(session, phone):
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.get(url) as r:
        if r.status == 200:
            return await r.json()
        if r.status == 404:
            return None
        return {"error": r.status}

async def create_contact(session, payload):
    phone = payload["phone"]
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.post(url, json=payload) as r:
        return r.status

async def update_contact(session, phone, payload):
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.put(url, json=payload) as r:
        return r.status

async def upsert_contact(session, payload):
    phone = payload["phone"]
    exists = await get_contact(session, phone)
    if exists is None:
        return await create_contact(session, payload)
    else:
        return await update_contact(session, phone, payload)

def convertir_row_a_payload(row):
    return {
        "firstName": str(row["First Name"]).strip(),
        "phone": str(row["Phone Number"]).strip(),
        "custom_fields": [
            {"name": "fecha_cita", "value": convertir_fecha_iso(row["Fecha Num"])},
            {"name": "fecha_larga", "value": str(row["Fecha Text"]).strip()},
            {"name": "hora_cita", "value": str(row["Hora"]).strip()},
            {"name": "nombre_doctor", "value": str(row["Doctor"]).strip()},
            {"name": "location", "value": str(row["Location"]).strip()},
        ]
    }

async def actualizar_id_pac_en_batch(phones, workspace, concurrencia=5):
    print("\nEjecutando actualización id_pac...\n")
    token = RESPONDIO_TOKENS[workspace]
    headers = authenticate(token)
    sem = asyncio.Semaphore(concurrencia)

    async with aiohttp.ClientSession(headers=headers) as session:
        async def procesar(phone):
            async with sem:
                url = f"{BASE_URL}/contact/phone:{phone}"
                await session.put(url, json={
                    "custom_fields": [{"name": "id_pac", "value": 11}]
                })

        await asyncio.gather(*(procesar(p) for p in phones))

    print("\n==============================")
    print(" ACTUALIZACIÓN id_pac FINALIZADA ")
    print("==============================")
    print(f"Total procesados: {len(phones)}")
    print("==============================\n")

async def subir_contactos_dataframe(df, workspace, concurrencia=5):
    print(f"\nSubiendo contactos a Respond.io ({workspace})...")
    token = RESPONDIO_TOKENS[workspace]
    headers = authenticate(token)
    sem = asyncio.Semaphore(concurrencia)

    results_ok = []
    results_err = []

    async with aiohttp.ClientSession(headers=headers) as session:
        async def procesar(row):
            async with sem:
                payload = convertir_row_a_payload(row)
                status = await upsert_contact(session, payload)
                if status in (200, 201):
                    results_ok.append(payload["phone"])
                else:
                    results_err.append(payload["phone"])

        await asyncio.gather(*(procesar(r) for _, r in df.iterrows()))

    print("\n==============================")
    print("        RESUMEN UP SERT")
    print("==============================")
    print(f"Total enviados:  {len(df)}")
    print(f"Correctos:       {len(results_ok)}")
    print(f"Errores:         {len(results_err)}")
    print("==============================")

    if results_ok:
        await actualizar_id_pac_en_batch(results_ok, workspace, concurrencia)

# ======================================================
# UTILIDADES (IGUAL QUE TU SCRIPT)
# ======================================================
def limpiar_telefono(t):
    if pd.isna(t):
        return None
    t = str(t).strip()
    if t.endswith(".0"):
        t = t[:-2]
    t = t.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if t.startswith("+"):
        t = t[1:]
    if not t.isdigit():
        return None
    if len(t) == 11 and t.startswith("34"):
        return "+" + t
    if len(t) == 9:
        return "+34" + t
    return None

def quitar_tildes(s):
    if not isinstance(s, str):
        return s
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def corregir_miercoles(texto):
    if not isinstance(texto, str):
        return texto
    texto = quitar_tildes(texto)
    texto = texto.replace("Miércoles", "Miercoles")
    texto = texto.replace("Mierc©rcoles", "Miercoles")
    texto = texto.replace("Mia©rcoles", "Miercoles")
    texto = texto.replace("Mie©rcoles", "Miercoles")
    return texto

# ======================================================
# LOGIN IDERMA
# ======================================================
async def login(page):
    print("Iniciando sesión...")
    await page.goto("https://control.iderma.es/07/LOGIN/default.cfm")
    await page.fill("input[name='MAIL']", IDERM_USER)
    await page.fill("input[name='PWD']", IDERM_PASS)
    await page.click("input[type='submit']")
    await page.wait_for_load_state("networkidle")
    print("Sesión iniciada.")

async def verificar_sesion(page):
    print("Verificando sesión...")
    await page.goto("https://control.iderma.es/07/_STAGE/default.cfm")
    await page.wait_for_load_state("networkidle")

    if "login" in page.url.lower():
        print("Sesión no válida.")
        return False

    frame_top = page.frame(name="frTop")
    if frame_top:
        try:
            await frame_top.wait_for_selector("#SITE", timeout=4000)
            print("Sesión activa detectada.")
            return True
        except:
            pass

    print("Sesión no activa.")
    return False

# ======================================================
# DESCARGAR AGENDA
# ======================================================
async def descargar_agenda(page):
    print("Navegando hacia Agenda Programada...")

    hoy = date.today()
    d = hoy.weekday()
    if d < 4:
        fecha = hoy + timedelta(days=1)
    else:
        fecha = hoy + timedelta(days=(7 - d) % 7 or 7)

    mes = MESES_ES[fecha.month - 1].capitalize()
    nombre_archivo = f"Recordatorios {fecha.day} de {mes}.xls"
    ruta = BASE_DIR / nombre_archivo

    print(f"Usando fecha final: {fecha.strftime('%Y-%m-%d')}")
    print(f"Nombre final del archivo: {nombre_archivo}")

    frame_top = page.frame(name="frTop")
    await frame_top.wait_for_selector("#SITE")
    await frame_top.select_option("#SITE", label="intranet.iderma.es")

    frame_menu = page.frame(name="frMenu")
    await frame_menu.wait_for_selector("a.btn:has-text('Listados y Reports')")
    await frame_menu.click("a.btn:has-text('Listados y Reports')")

    frame_sub = page.frame(name="frSubmenu")
    await frame_sub.wait_for_selector("a.btn.btn-info.btn-submenu:has-text('Agenda programada')")
    await frame_sub.click("a.btn.btn-info.btn-submenu:has-text('Agenda programada')")

    frame_center = page.frame(name="frCenter")
    await frame_center.wait_for_selector("form#form_agenda_list")

    await frame_center.fill("input[name='fechafin']", fecha.strftime("%Y-%m-%d"))
    await frame_center.click("button:has-text('Filtrar')")
    await page.wait_for_timeout(3000)

    async with page.expect_download() as dwn:
        await frame_center.click("button:has-text('Exportar a Excel')")
    download = await dwn.value
    await page.wait_for_timeout(3000)

    await download.save_as(ruta)
    print("Agenda descargada:", ruta)
    return ruta, fecha

# ======================================================
# TRANSFORMACIÓN COMPLETA
# ======================================================
def cargar_auxiliares(ruta_aux):
    hojas = ["Agenda","Acto","Centro","Doctor","Direccion"]
    return {h: pd.read_excel(ruta_aux, sheet_name=h) for h in hojas}

def aplicar_verificaciones(df, aux):
    doctor_map = aux["Doctor"].set_index("Dr Codigo")["Nombre Profesional"].to_dict()
    df["Nombre Profesional"] = df["Prof"].map(doctor_map)

    direccion_map = aux["Direccion"].set_index("Clinica")["Direccion"].to_dict()
    df["Direccion Centro"] = df["Centro"].map(direccion_map)

    df["Verif1_Agenda"] = df["Estado"].map(
        aux["Agenda"].set_index("Código")["Enviar confirmación?"]
    ).fillna("No")

    acto_df = aux["Acto"].drop_duplicates(subset=["actID"])
    acto_map = acto_df.set_index("actID")["Enviar confirmación?"].to_dict()
    df["Verif2_Acto"] = df["Acto ID"].map(acto_map).fillna("Si")

    centro_map = aux["Centro"].set_index("Centro")["Enviar confirmación?"].to_dict()
    df["Verif3_Centro"] = df["Centro"].map(centro_map).fillna("No")

    df = df.drop_duplicates(subset=["movil"]).reset_index(drop=True)
    df["Verif4_Repetido"] = "Si"

    hoy = date.today()
    d = hoy.weekday()
    mañana = hoy + timedelta(days=1 if d < 4 else (7 - d) % 7 or 7)

    df["Verif5_Mañana"] = df["Start Time"].apply(
        lambda x: "Si" if pd.to_datetime(x, errors="coerce").date() == mañana else "No"
    )

    df["Usar"] = df[
        ["Verif1_Agenda","Verif2_Acto","Verif3_Centro","Verif4_Repetido","Verif5_Mañana"]
    ].apply(lambda r: "Si" if all(v=="Si" for v in r) else "No", axis=1)

    return df

def extraer_fecha_hora_es(valor):
    if pd.isna(valor) or str(valor).strip()=="":
        return "","",""

    dt = pd.to_datetime(str(valor), errors="coerce")
    if pd.isna(dt):
        return "","",""

    fecha_num = dt.strftime("%m/%d/%y")
    dia = DIAS_ES[dt.weekday()]
    mes = MESES_ES[dt.month-1]
    fecha_text = f"{dia}, {dt.day} de {mes} de {dt.year}".capitalize()
    fecha_text = corregir_miercoles(quitar_tildes(fecha_text))
    hora = dt.strftime("%H:%M")
    return fecha_num, fecha_text, hora

def transformar_y_generar_csv(ruta_archivo, fecha_objetivo):
    with open(ruta_archivo,"r",encoding="utf-8",errors="ignore") as f:
        html = f.read()
    df = pd.read_html(StringIO(html))[0]

    aux = cargar_auxiliares(AUX_PATH)
    df_final = aplicar_verificaciones(df, aux)

    salida_xlsx = BASE_DIR / "agenda_filtrada.xlsx"
    df_final.to_excel(salida_xlsx, index=False)
    print(f"\nArchivo Excel filtrado guardado:\n{salida_xlsx}")

    df_final = df_final[df_final["Usar"]=="Si"].copy()

    df_final["movil"] = df_final["movil"].apply(limpiar_telefono)
    df_final = df_final[df_final["movil"].notna()]

    base = pd.DataFrame()
    base["First Name"] = df_final["Nombre"].astype(str).str.title()
    base["Phone Number"] = df_final["movil"]

    fechas = df_final["Start Time"].apply(extraer_fecha_hora_es)
    base["Fecha Num"] = [x[0] for x in fechas]
    base["Fecha Text"] = [x[1] for x in fechas]
    base["Hora"] = [x[2] for x in fechas]

    base["Doctor"] = df_final["Nombre Profesional"].apply(lambda x: quitar_tildes(str(x)))
    base["Location"] = df_final["Direccion Centro"].apply(quitar_tildes)

    sabino = base[base["Location"].str.contains("Sabino Arana", case=False, na=False)]
    bori = base[base["Location"].str.contains("Bori i Fontesta", case=False, na=False)]

    abr = MESES_ABR_ES[fecha_objetivo.month-1]

    ruta_sabino = ruta_bori = None

    if not sabino.empty:
        ruta_sabino = CSV_BASE / "Sabino" / f"Recordatorios Sabino {fecha_objetivo.day} de {abr}.csv"
        sabino.to_csv(ruta_sabino, index=False, encoding="utf-8-sig")
        print(f"\n📍 CSV Sabino guardado: {ruta_sabino}")

    if not bori.empty:
        ruta_bori = CSV_BASE / "Bori" / f"Recordatorios Bori {fecha_objetivo.day} de {abr}.csv"
        bori.to_csv(ruta_bori, index=False, encoding="utf-8-sig")
        print(f"\n📍 CSV Bori guardado: {ruta_bori}")

    print("\nProceso completado con éxito.")
    return sabino, bori, ruta_sabino, ruta_bori

# ======================================================
# MAIN
# ======================================================
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox","--disable-dev-shm-usage"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        sesion_valida = await verificar_sesion(page)
        if not sesion_valida:
            await login(page)

        ruta_archivo, fecha_objetivo = await descargar_agenda(page)
        await browser.close()

    sabino_df, bori_df, _, _ = transformar_y_generar_csv(ruta_archivo, fecha_objetivo)

    if sabino_df is not None and not sabino_df.empty:
        await subir_contactos_dataframe(sabino_df, "sabino")

    if bori_df is not None and not bori_df.empty:
        await subir_contactos_dataframe(bori_df, "bori")

if __name__ == "__main__":
    asyncio.run(main())
