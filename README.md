# Inline-Prozesskontrolle

Dieses Projekt implementiert eine Kafka-basierte Streaming-Lösung zur Inline-Prozessanalyse einer induktiven Drahtvergütung mit inkonstantem Durchmesser.

## Projektinhalt

Die Lösung besteht aus mehreren entkoppelten Modulen:

- **Kafka Producer**  
  Liest die simulierten Prozessdaten aus `data/process_data.json` und veröffentlicht sie in das Kafka-Topic `1031103_1000`.

- **Korrelationsanalyse**  
  Berechnet die Pearson-Korrelation zwischen Drahtgeschwindigkeit und Härtetemperatur in Zeitfenstern und schreibt die Ergebnisse in `1031103_1000_corr`.

- **Six-Sigma-Monitor**  
  Überwacht Härtetemperatur und Anlasstemperatur in einem gleitenden Fenster. Bei Verletzung der Eingriffsgrenzen (Mittelwert ± 3σ) werden Alarm-Events in `1031103_801` publiziert.

- **Profilerkennung**  
  Erkennt Drahtprofile anhand von Drahtlänge und Durchmesser, bestimmt die Segmentlängen L1 bis L4 und bewertet die Profile als i.O. oder n.i.O.  
  Alle erkannten Profile werden in `1031103_1000_profiles` veröffentlicht.  
  n.i.O.-Profile werden zusätzlich in `1031103_1000_profile_events` publiziert.

- **Dashboard**  
  Visualisiert Durchmesser, Härtetemperatur und Anlasstemperatur über der Drahtlänge sowie die Anzahl erkannter und fehlerhafter Profile.

## Projektstruktur

- `data/` – simulierte Prozessdaten
- `src/producer/` – Kafka Producer
- `src/analytics/` – Korrelationsanalyse, Six-Sigma-Monitor, Profilerkennung
- `src/dashboard/` – Streamlit-Dashboard
- `loesung_template/analyse.md` – schriftliche Dokumentation
- `docker-compose.yml` – Start der kompletten Umgebung

## Starten des Projekts

Voraussetzungen:
- Docker Desktop
- Docker Compose

Projekt starten:

```bash
docker compose up --build
