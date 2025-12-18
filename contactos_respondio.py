from __future__ import annotations

import os
import asyncio
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
import pandas as pd

BASE_URL: str = "https://api.respond.io/v2"


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return v


TOKENS: Dict[str, str] = {
    "sabino": _require_env("RESPONDIO_TOKEN_SABINO"),
    "bori": _require_env("RESPONDIO_TOKEN_BORI"),
    # Déjalos si quieres reutilizar el módulo luego con Flowww:
    "cdc": _require_env("RESPONDIO_TOKEN_CDC"),
    "rey": _require_env("RESPONDIO_TOKEN_REY"),
}

APLICAR_ID_PAC: bool = os.getenv("RESPONDIO_APLICAR_ID_PAC", "true").strip().lower() == "true"
ID_PAC_VALUE: int = int(os.getenv("RESPONDIO_ID_PAC", "77"))
HTTP_TIMEOUT_SECONDS: int = int(os.getenv("RESPONDIO_TIMEOUT_SECONDS", "30"))


def authenticate(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def convertir_fecha_iso(raw: Any) -> str:
    if raw is None or str(raw).strip() == "":
        return ""
    try:
        dt = datetime.strptime(str(raw), "%m/%d/%y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


async def get_contact(session: aiohttp.ClientSession, phone: str) -> dict | None:
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.get(url) as r:
        if r.status == 200:
            return await r.json()
        if r.status == 404:
            return None
        raise RuntimeError(f"GET contacto {phone} → {r.status}: {await r.text()}")


async def create_contact(session: aiohttp.ClientSession, payload: dict) -> dict:
    phone = payload["phone"]
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.post(url, json=payload) as r:
        if r.status in (200, 201):
            return await r.json()
        raise RuntimeError(f"CREATE {phone} → {r.status}: {await r.text()}")


async def update_contact(session: aiohttp.ClientSession, phone: str, payload: dict) -> dict:
    url = f"{BASE_URL}/contact/phone:{phone}"
    async with session.put(url, json=payload) as r:
        if r.status == 200:
            return await r.json()
        raise RuntimeError(f"UPDATE {phone} → {r.status}: {await r.text()}")


async def upsert_contact(session: aiohttp.ClientSession, payload: dict) -> dict:
    phone: str = payload["phone"]
    exists = await get_contact(session, phone)
    if exists is None:
        return await create_contact(session, payload)
    return await update_contact(session, phone, payload)


def convertir_row_a_payload(row: pd.Series) -> dict:
    phone = str(row["Phone Number"]).strip()
    location = str(row["Location"]).strip()

    return {
        "firstName": str(row["First Name"]).strip(),
        "phone": phone,
        "custom_fields": [
            {"name": "fecha_cita", "value": convertir_fecha_iso(row["Fecha Num"])},
            {"name": "fecha_larga", "value": str(row["Fecha Text"]).strip()},
            {"name": "hora_cita", "value": str(row["Hora"]).strip()},
            {"name": "nombre_doctor", "value": str(row["Doctor"]).strip()},
            {"name": "location", "value": location},
        ],
    }


async def actualizar_id_pac_en_batch(phones: List[str], workspace: str, concurrencia: int = 5) -> None:
    token: str = TOKENS[workspace.lower()]
    headers = authenticate(token)
    sem = asyncio.Semaphore(concurrencia)

    payload_idpac = {
        "custom_fields": [
            {"name": "id_pac", "value": ID_PAC_VALUE},
        ]
    }

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:

        async def procesar(phone: str) -> None:
            async with sem:
                url = f"{BASE_URL}/contact/phone:{phone}"
                async with session.put(url, json=payload_idpac) as r:
                    if r.status != 200:
                        raise RuntimeError(f"id_pac {phone} → {r.status}: {await r.text()}")

        await asyncio.gather(*(procesar(p) for p in phones))


async def subir_contactos_dataframe(df: pd.DataFrame, workspace: str, concurrencia: int = 5) -> dict:
    token: str = TOKENS[workspace.lower()]
    headers = authenticate(token)
    sem = asyncio.Semaphore(concurrencia)
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)

    results_ok: List[str] = []
    results_err: List[dict] = []

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:

        async def procesar(row: pd.Series) -> None:
            async with sem:
                payload = convertir_row_a_payload(row)
                try:
                    await upsert_contact(session, payload)
                    results_ok.append(payload["phone"])
                except Exception as e:
                    results_err.append(
                        {"status": "error", "phone": payload["phone"], "detail": str(e)}
                    )

        await asyncio.gather(*(procesar(row) for _, row in df.iterrows()))

    print(f"[RESPONDIO] workspace={workspace} total={len(df)} ok={len(results_ok)} err={len(results_err)}")

    if results_err:
        for e in results_err[:10]:
            print(f"[RESPONDIO][ERROR] {e['phone']} → {e['detail']}")

    if APLICAR_ID_PAC and results_ok:
        print(f"[RESPONDIO] Aplicando id_pac={ID_PAC_VALUE} automáticamente a {len(results_ok)} contactos")
        await actualizar_id_pac_en_batch(results_ok, workspace, concurrencia)

    return {"ok": results_ok, "error": results_err}
