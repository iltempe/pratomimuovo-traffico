#!/usr/bin/env python3
"""
PratoMiMuovo – Raccolta dati traffico Snodo del Soccorso
Scrive i dati su Supabase via REST API.

Variabili d'ambiente richieste (GitHub Secrets):
    TOMTOM_API_KEY     — chiave TomTom Traffic API
    SUPABASE_URL       — es. https://xxxx.supabase.co
    SUPABASE_ANON_KEY  — chiave anon Supabase
"""

import os
import sys
import requests
from datetime import datetime, timezone

# ─── Punti di misura ───────────────────────────────────────────────────────────
PUNTI_MISURA = [
    # 11 punti equidistanti (322m) interpolati su nodi OSM reali di Viale Leonardo da Vinci
    # Coordinate derivate dalla geometria OSM (carreggiata nord, Way 39330780/288208347/…)
    {"id": "vldv_w3",      "nome": "Viale L. da Vinci – W3 (Casale)",    "lat": 43.88354, "lon": 11.04166},
    {"id": "vldv_w2",      "nome": "Viale L. da Vinci – W2",             "lat": 43.88288, "lon": 11.04557},
    {"id": "vldv_w1",      "nome": "Viale L. da Vinci – W1",             "lat": 43.88204, "lon": 11.04941},
    {"id": "vldv_ov_lon",  "nome": "Viale L. da Vinci – Ovest lontano",  "lat": 43.88100, "lon": 11.05316},
    {"id": "vldv_ov_vic",  "nome": "Viale L. da Vinci – Ovest vicino",   "lat": 43.88028, "lon": 11.05704},
    {"id": "vldv_incrocio","nome": "Viale L. da Vinci – × Via P.Nenni",  "lat": 43.87947, "lon": 11.06089},
    {"id": "vldv_es_vic",  "nome": "Viale L. da Vinci – Est vicino",     "lat": 43.87872, "lon": 11.06476},
    {"id": "vldv_es_lon",  "nome": "Viale L. da Vinci – Est lontano",    "lat": 43.87794, "lon": 11.06863},
    {"id": "vldv_e1",      "nome": "Viale L. da Vinci – E1",             "lat": 43.87716, "lon": 11.07250},
    {"id": "vldv_e2",      "nome": "Viale L. da Vinci – E2",             "lat": 43.87603, "lon": 11.07618},
    {"id": "vldv_e3",      "nome": "Viale L. da Vinci – E3 (Reggiana)",  "lat": 43.87452, "lon": 11.07959},
]

TOMTOM_URL  = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

# ─── Fetch TomTom ──────────────────────────────────────────────────────────────
def fetch_flusso(lat, lon, api_key):
    r = requests.get(TOMTOM_URL, params={
        "key": api_key, "point": f"{lat},{lon}", "unit": "KMPH", "openLr": "false"
    }, timeout=10)
    r.raise_for_status()
    return r.json()

def estrai_metriche(dati):
    fd = dati["flowSegmentData"]
    vel_attuale = fd.get("currentSpeed")
    vel_libera  = fd.get("freeFlowSpeed")
    rapporto    = (vel_attuale / vel_libera) if vel_libera and vel_libera > 0 else None
    confidence  = fd.get("confidence")
    if rapporto is not None:
        livello = 1 if rapporto >= 0.85 else (2 if rapporto >= 0.60 else 3)
    else:
        livello = 0
    return vel_attuale, vel_libera, rapporto, livello, confidence

# ─── Supabase insert ───────────────────────────────────────────────────────────
def insert_supabase(records, url, anon_key):
    endpoint = f"{url}/rest/v1/rilevazioni"
    headers  = {
        "apikey":        anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }
    r = requests.post(endpoint, json=records, headers=headers, timeout=10)
    r.raise_for_status()

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    api_key     = os.environ["TOMTOM_API_KEY"]
    supa_url    = os.environ["SUPABASE_URL"].rstrip("/")
    supa_key    = os.environ["SUPABASE_ANON_KEY"]

    ora = datetime.now(timezone.utc).isoformat()
    print(f"[{ora}] Raccolta dati – {len(PUNTI_MISURA)} punti")

    records = []
    etichette = {0: "?", 1: "🟢 scorrevole", 2: "🟡 rallentato", 3: "🔴 congestionato"}

    for p in PUNTI_MISURA:
        errore = None
        vel_attuale = vel_libera = rapporto = confidence = None
        livello = 0
        try:
            dati = fetch_flusso(p["lat"], p["lon"], api_key)
            vel_attuale, vel_libera, rapporto, livello, confidence = estrai_metriche(dati)
            print(f"  {etichette[livello]} {p['nome']}: {vel_attuale} km/h (freeflow: {vel_libera})")
        except Exception as e:
            errore = str(e)
            print(f"  ❌ {p['nome']}: {errore}")

        records.append({
            "timestamp":        ora,
            "punto_id":         p["id"],
            "punto_nome":       p["nome"],
            "lat":              p["lat"],
            "lon":              p["lon"],
            "velocita_attuale": vel_attuale,
            "velocita_libera":  vel_libera,
            "rapporto_flusso":  rapporto,
            "livello_conf":     livello,
            "confidence":       confidence,
            "errore":           errore,
        })

    insert_supabase(records, supa_url, supa_key)
    print(f"✅ {len(records)} record inseriti su Supabase")

if __name__ == "__main__":
    main()
