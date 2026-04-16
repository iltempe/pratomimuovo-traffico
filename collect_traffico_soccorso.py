#!/usr/bin/env python3
"""
PratoMiMuovo – Raccolta dati traffico Viale L. da Vinci (Prato)
32 misurazioni: 16 posizioni × 2 direzioni (→Firenze / →Pistoia)
Offset ±0.0002° lat per atterrare sulla carreggiata corretta.

Variabili d'ambiente richieste (GitHub Secrets):
    TOMTOM_API_KEY     — chiave TomTom Traffic API
    SUPABASE_URL       — es. https://xxxx.supabase.co
    SUPABASE_ANON_KEY  — chiave anon Supabase
"""

import os
import requests
from datetime import datetime, timezone

# ─── 16 posizioni centrali (coordinate OSM, Viale L. da Vinci, Prato) ─────────
# Interpolate equidistanti ~322 m lungo la carreggiata (W→E, lon crescente)
POSIZIONI = [
    {"id": "vldv_w3",       "nome": "W3 – Casale",      "lat": 43.88354, "lon": 11.04166},
    {"id": "vldv_w2",       "nome": "W2",               "lat": 43.88288, "lon": 11.04557},
    {"id": "vldv_w1",       "nome": "W1",               "lat": 43.88204, "lon": 11.04941},
    {"id": "vldv_ov_lon",   "nome": "Ovest Lontano",    "lat": 43.88100, "lon": 11.05316},
    {"id": "vldv_ov_vic",   "nome": "Ovest Vicino",     "lat": 43.88028, "lon": 11.05704},
    {"id": "vldv_incrocio", "nome": "Incrocio P.Nenni", "lat": 43.87947, "lon": 11.06089},
    {"id": "vldv_es_vic",   "nome": "Est Vicino",       "lat": 43.87872, "lon": 11.06476},
    {"id": "vldv_es_lon",   "nome": "Est Lontano",      "lat": 43.87794, "lon": 11.06863},
    {"id": "vldv_e1",       "nome": "E1",               "lat": 43.87716, "lon": 11.07250},
    {"id": "vldv_e2",       "nome": "E2",               "lat": 43.87603, "lon": 11.07618},
    {"id": "vldv_e3",       "nome": "E3 – Reggiana",    "lat": 43.87452, "lon": 11.07959},
    # 5 nuovi punti verso Firenze (OSM nodes interpolati a ~322 m da E3)
    {"id": "vldv_e4",       "nome": "E4",               "lat": 43.87280, "lon": 11.08282},
    {"id": "vldv_e5",       "nome": "E5",               "lat": 43.87110, "lon": 11.08609},
    {"id": "vldv_e6",       "nome": "E6",               "lat": 43.86945, "lon": 11.08938},
    {"id": "vldv_e7",       "nome": "E7",               "lat": 43.86779, "lon": 11.09268},
    {"id": "vldv_e8",       "nome": "E8",               "lat": 43.86608, "lon": 11.09593},
]

# 32 punti: per ogni posizione due misure
#   _fi  →Firenze  lat - 0.0002  (carreggiata sud / direzione est)
#   _pi  →Pistoia  lat + 0.0002  (carreggiata nord / direzione ovest)
PUNTI_MISURA = []
for _p in POSIZIONI:
    PUNTI_MISURA.append({
        "id":   _p["id"] + "_fi",
        "nome": _p["nome"] + " →Firenze",
        "lat":  round(_p["lat"] - 0.0002, 5),
        "lon":  _p["lon"],
    })
    PUNTI_MISURA.append({
        "id":   _p["id"] + "_pi",
        "nome": _p["nome"] + " →Pistoia",
        "lat":  round(_p["lat"] + 0.0002, 5),
        "lon":  _p["lon"],
    })

TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

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
    r = requests.post(endpoint, json=records, headers=headers, timeout=15)
    r.raise_for_status()

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    api_key  = os.environ["TOMTOM_API_KEY"]
    supa_url = os.environ["SUPABASE_URL"].rstrip("/")
    supa_key = os.environ["SUPABASE_ANON_KEY"]

    ora = datetime.now(timezone.utc).isoformat()
    print(f"[{ora}] Raccolta dati – {len(PUNTI_MISURA)} punti "
          f"({len(POSIZIONI)} posizioni × 2 direzioni)")

    records   = []
    etichette = {0: "?", 1: "🟢 scorrevole", 2: "🟡 rallentato", 3: "🔴 congestionato"}

    for p in PUNTI_MISURA:
        errore = None
        vel_attuale = vel_libera = rapporto = confidence = None
        livello = 0
        try:
            dati = fetch_flusso(p["lat"], p["lon"], api_key)
            vel_attuale, vel_libera, rapporto, livello, confidence = estrai_metriche(dati)
            print(f"  {etichette[livello]} {p['nome']}: {vel_attuale} km/h "
                  f"(ff: {vel_libera})")
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
