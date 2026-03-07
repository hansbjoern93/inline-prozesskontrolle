# Lösung: Inline Prozesskontrolle

**Name:** Björn Blocksdorf
**Matrikelnummer:** 3054698

## 1. Vorgehensweise und Architektur

Ich habe die Aufgabe in mehrere Module aufgeteilt, damit nicht alles in einem einzigen Skript passiert. So bleibt die Lösung übersichtlicher und die einzelnen Teile können getrennt voneinander ausgeführt werden. Zuerst gibt es einen Producer, der die (bereinigten) Prozessdaten aus einer JSON-Datei einliest und in das Kafka-Topic 1031103_1000 schreibt. Dieses Topic dient dann als gemeinsame Datenquelle für die anderen Module. Dafür nutze ich vor allem confluent_kafka für Producer und Consumer. Für die Verarbeitung der Daten kommen je nach Modul u. a. json, os, numpy, statistics und collections.deque zum Einsatz. Für die Visualisierung nutze ich Streamlit, Plotly und Pandas.

Anschließend übernehmen mehrere unabhängige Kafka-Consumer die Verarbeitung. Der correlation_analyzer liest aus 1031103_1000, berechnet die Pearson-Korrelation zwischen Geschwindigkeit und Härtetemperatur innerhalb eines Zeitfensters und schreibt das Ergebnis in das Topic 1031103_1000_corr. Die Berechnung läuft als Streaming-Verarbeitung. Der Consumer sammelt die eingehenden Messwerte fortlaufend im aktuellen Zeitfenster und berechnet die Korrelation immer dann, wenn das Fenster abgeschlossen ist. Der gesamte Datensatz muss dafür nicht vorher komplett geladen werden.

Publiziert wird ein JSON mit fenster_start, fenster_ende, anzahl_punkte und pearson_korrelation. Die Laufzeitparameter werden über Umgebungsvariablen gesteuert. Im Docker-Compose sind zum Beispiel WINDOW_SIZE_SEC=300 und MIN_POINTS=1000 gesetzt. Der six_sigma_monitor überwacht die Härte- und Anlasstemperatur in einem gleitenden Fenster. Bei Ausreißern sendet er Alarm-Events in das Topic 1031103_801. Der profile_detector erkennt die Drahtprofile (L1–L4) und bewertet sie. Erkannte Profile veröffentlicht er in 1031103_1000_profiles. n.i.O.-Events publiziert er zusätzlich in 1031103_1000_profile_events. 

Zusätzlich gibt es ein Dashboard zur Visualisierung. Das Dashboard liest die Rohdaten aus 1031103_1000 sowie die Ergebnis-Topics 1031103_1000_profiles und 1031103_1000_profile_events direkt aus Kafka. Eine Anzeige der Korrelations- und Alarm-Topics (1031103_1000_corr, 1031103_801) wäre als Erweiterung möglich.
Es zeigt Durchmesser, Härtetemperatur und Anlasstemperatur jeweils über der Drahtlänge. Außerdem zeigt es die Anzahl erkannter und fehlerhafter Profile an.
Mir war bei der Umsetzung wichtig, dass die Module nicht direkt voneinander abhängen. Deshalb übernimmt Kafka die Verteilung der Daten. Jedes Modul liest selbst aus Kafka und verarbeitet nur die Daten, die es für seine Aufgabe braucht. Das passt zur Aufgabenstellung, weil Profilerkennung und Visualisierung getrennt umgesetzt werden sollen.

In meinem docker-compose ist Redis zusätzlich enthalten, wird aber nicht von allen Modulen zwingend benötigt. Das Dashboard kann Redis optional als Zwischenspeicher verwenden, damit bestimmte Werte innerhalb eines laufenden Systemstarts schneller verfügbar sind und die Anzeige flüssiger läuft. Die Analyse-Module, also Korrelationsanalyse und Six-Sigma-Monitor, arbeiten unabhängig davon und lesen ihre Eingangsdaten direkt aus Kafka. Für die Profilerkennung nutze ich Redis zusätzlich, um den internen Zustand während der Laufzeit zu speichern. Dadurch kann der profile_detector bei kurzen Unterbrechungen oder internen Neustarts innerhalb derselben Umgebung an der zuletzt verarbeiteten Stelle weitermachen. Die eigentlichen Prozessdaten laufen trotzdem weiterhin ausschließlich über Kafka, also über die entsprechenden Ein- und Ausgabe-Topics.

Redis und Kafka sind in meiner Konfiguration allerdings bewusst nicht persistent ausgelegt. Bei einem vollständigen Neustart der gesamten Umgebung gehen diese Zustände deshalb verloren. Das ist in meinem Fall aber beabsichtigt, weil ich für die Hausarbeit eine möglichst reproduzierbare Ausführung haben wollte. So startet jeder neue Lauf wieder mit denselben Anfangsbedingungen, sodass die Ergebnisse besser nachvollzogen und erneut erzeugt werden können.

Zum Starten des gesamten Systems nutze ich ein PowerShell-Skript (start.ps1), das den Stack reproduzierbar neu hochfährt. Es führt zuerst docker compose down aus und startet danach alle Services mit docker compose up --build -d neu. Nach einer kurzen Wartezeit wird außerdem automatisch das Dashboard im Browser unter http://localhost:8501 geöffnet.

## 2. Korrelationsanalyse

Für die Korrelationsanalyse habe ich ein Zeitfenster von 300 Sekunden gewählt. Der Grund dafür ist, dass bei zu kleinen Fenstern die Werte oft stark schwanken würden. Dann könnte sich die Korrelation schnell ändern, obwohl es im Prozess vielleicht gar keine wirklich relevante Veränderung gibt. Mit 300 Sekunden ist das Fenster noch klein genug, um Veränderungen noch rechtzeitig zu erkennen, aber auch groß genug, damit die Ergebnisse stabiler sind.
Ich nutze die Pearson-Korrelation, weil Geschwindigkeit und Härtetemperatur Zahlenwerte sind und ich prüfen möchte, ob sie sich im Prozess ähnlich verhalten. Steigt die Geschwindigkeit, steigt dann auch eher die Temperatur (oder umgekehrt)?

Pearson ist hier gut geeignet, weil damit ein linearer Zusammenhang zwischen zwei numerischen Größen gemessen wird. Das Ergebnis liegt immer zwischen -1 und +1 und lässt sich dadurch gut interpretieren. Man kann also relativ schnell erkennen, ob eher ein positiver Zusammenhang, ein negativer Zusammenhang oder kein klarer linearer Zusammenhang vorliegt. Falls der Zusammenhang eher nicht linear wäre, könnte man stattdessen auch Spearman verwenden.
Die konkreten Laufzeitparameter werden in meinem docker-compose.yml gesetzt. Für den corr-analyzer sind dort WINDOW_SIZE_SEC=300 und MIN_POINTS=1000 konfiguriert.

Zusätzlich habe ich eine Mindestanzahl an Messpunkten eingebaut. Die Korrelation wird also nicht sofort mit den ersten wenigen Werten berechnet, sondern erst dann, wenn innerhalb des Fensters genügend gültige Messwerte vorliegen. Das ist sinnvoll, weil Ergebnisse auf Basis von nur wenigen Punkten oft wenig belastbar sind und leichter durch einzelne Werte beeinflusst werden können. Da der Datenstrom für diese Hausarbeit bereits vorbereitet wurde, sollte dieser Fall hier normalerweise nicht auftreten. Für einen realen Betrieb oder ein späteres Deployment halte ich diese Absicherung trotzdem für sinnvoll.

Eine Herausforderung bei der Berechnung im Stream war, dass die Daten nacheinander ankommen und nicht komplett auf einmal vorliegen. Man kann also nicht einfach am Ende alles zusammen auswerten, sondern muss die Werte während des laufenden Datenstroms sammeln. Mein Skript speichert deshalb die Werte für Geschwindigkeit und Härtetemperatur zunächst im aktuellen Zeitfenster und berechnet die Pearson-Korrelation erst danach.

Ich arbeite mit nicht überlappenden Zeitfenstern. Das bedeutet: Für jedes Fenster (z. B. 300 Sekunden) sammle ich die Messwerte und berechne am Ende genau ein Korrelations-Ergebnis. Anschließend beginnt ein neues Fenster wieder bei null, also ohne Werte aus dem alten Fenster mitzunehmen. Das ist praktisch, weil die Ergebnisse dann vergleichbar sind (Fenster 1, Fenster 2, Fenster 3...) in den Logs leichter sieht, ob sich die Korrelation zu einem bestimmten Zeitpunkt verändert hat.

Das Sammeln der Werte passiert in meinem Code hier:
```python
if fenster_start is None:
    fenster_start = zeitstempel

geschwindigkeiten.append(float(geschwindigkeit))
haertetemperaturen.append(float(haertetemperatur))
```
Sobald das Zeitfenster vollständig ist, wird geprüft, ob genug Messpunkte vorhanden sind. Erst dann wird die Korrelation berechnet:
```python
if (zeitstempel - fenster_start) >= fenster_sekunden:
    if len(geschwindigkeiten) >= min_punkte:
        korrelation = float(np.corrcoef(geschwindigkeiten, haertetemperaturen)[0, 1])
```
Die Pearson-Korrelation wird also direkt aus den im aktuellen Fenster gesammelten Werten berechnet. Das Ergebnis wird anschließend in ein eigenes Kafka-Topic geschrieben:
```python
ergebnis = {
    "fenster_start": fenster_start,
    "fenster_ende": zeitstempel,
    "anzahl_punkte": len(geschwindigkeiten),
    "pearson_korrelation": korrelation,
}

producer.produce(topic_ausgang, json.dumps(ergebnis).encode("utf-8"))
```
## 3. Schwellwerterkennung (Six Sigma)
Bei der Schwellwerterkennung habe ich ein gleitendes Zeitfenster umgesetzt. Für jede der beiden überwachten Temperaturen speichere ich die Werte der letzten 300 Sekunden in einer deque. Sobald ein neuer Messwert ankommt, wird er an das Fenster angehängt. Gleichzeitig werden alle Werte entfernt, die älter als 300 Sekunden sind. Dadurch bleiben pro Messgröße immer nur die aktuell relevanten Werte im Speicher.

Die Verwendung einer deque ist hier sinnvoll, weil neue Werte effizient hinten angefügt und alte Werte vorne wieder entfernt werden können. Das passt gut zu einem gleitenden Fenster, das sich bei jedem neuen Messwert fortlaufend aktualisiert. So muss nicht der gesamte Datenstrom gespeichert werden, sondern nur der Abschnitt, der für die aktuelle Berechnung von Mittelwert und Standardabweichung benötigt wird.

Das passiert im Code so:
```python
def aktualisiere_fenster(self, messgroesse: str, zeitstempel: float, wert: float):
    q = self.fenster[messgroesse]
    q.append((zeitstempel, wert))

    while q and (zeitstempel - q[0][0]) > self.fenster_sekunden:
        q.popleft()

    self.speichere_punkt_in_redis(messgroesse, zeitstempel, wert)
```
Mittelwert und Standardabweichung berechne ich jeweils auf Basis des aktuellen Fensters. Ich nutze dafür keine spezielle Online-Formel wie zum Beispiel Welford, sondern bestimme beide Kennwerte bei jeder Prüfung direkt aus den Werten, die sich aktuell im Fenster befinden. Die „Running Statistics“ entstehen in meiner Lösung also dadurch, dass Mittelwert und Standardabweichung mit jedem neu eingehenden Messwert für das gleitende Zeitfenster erneut berechnet werden.
```python
werte = [x for _, x in q]
mittelwert = statistics.mean(werte)
standardabweichung = statistics.stdev(werte)
```
Auf dieser Grundlage prüfe ich, ob der aktuelle Messwert außerhalb der Eingriffsgrenzen liegt. Als Grenze verwende ich Mittelwert ± 3 · Standardabweichung:
```python
alarm = (
    wert > mittelwert + 3 * standardabweichung
    or wert < mittelwert - 3 * standardabweichung
)
```
Damit nicht zu früh unnötige Fehlalarme entstehen, startet die Alarmprüfung erst dann, wenn im aktuellen Fenster genügend Messwerte vorhanden sind. Zusätzlich wird der Fall abgefangen, dass die Standardabweichung 0 ist, zum Beispiel am Anfang oder bei über längere Zeit sehr konstanten Werten. Auch dann wird kein Alarm ausgelöst. Standardmäßig verwendet der Service dafür MIN_POINTS = 30, falls kein anderer Wert gesetzt ist. Da in unserem Projekt mit einem bereits vorbereiteten und bereinigten Datensatz gearbeitet wird, sollte dieser Fall hier normalerweise nicht auftreten. Für einen praktischen Einsatz oder ein späteres Deployment halte ich diese Absicherung aber trotzdem für sinnvoll.

```python
if len(q) < self.min_punkte:
    return

if standardabweichung == 0:
    return
```
Wenn ein Wert außerhalb der berechneten Grenzen liegt, wird zunächst ein alarm_start-Event erzeugt. Hält diese Auffälligkeit über mehrere Messpunkte hinweg an, wird sie intern als zusammenhängendes Ereignis weitergeführt, statt für jeden einzelnen Wert ein neues Event zu erzeugen. Sobald sich der Messwert wieder im normalen Bereich befindet, wird ein alarm_summary-Event gesendet. Dieses enthält unter anderem die Dauer des Alarms, den Peak-Wert und die Anzahl der betroffenen Messpunkte. Dadurch lassen sich Alarmphasen später besser als zusammenhängende Ereignisse nachvollziehen.
```python
if alarm:
    if ereignis is None:
        self.sende_alarm(
            {
                "type": "alarm_start",
                "timestamp": zeitstempel,
                "metric": messgroesse,
                "value": wert,
            }
        )
        self.ereignisse[messgroesse] = {
            "start": zeitstempel,
            "end": zeitstempel,
            "peak": wert,
            "count": 1,
        }
self.sende_alarm(
    {
        "type": "alarm_summary",
        "event_start": ereignis["start"],
        "event_end": ereignis["end"],
        "duration_sec": round(ereignis["end"] - ereignis["start"], 2),
        "metric": messgroesse,
        "peak_value": ereignis["peak"],
        "alarm_count": ereignis["count"],
    }
)
```
Der six_sigma_monitor arbeitet in meiner aktuellen docker-compose-Konfiguration ohne Redis und liest seine Daten direkt aus Kafka. Im Code ist Redis jedoch optional vorbereitet, sodass Fensterzustand und Alarmstatus bei Bedarf auch in Redis gespeichert und wiederhergestellt werden könnten.

## 4. Profilerkennung
Für die Profilerkennung verwende ich einen zustandsbasierten Algorithmus, also eine State Machine, die den kontinuierlichen Kafka-Datenstrom schrittweise in einzelne Profilsegmente unterteilt. Ein Profil wird dabei über die Zustände WAIT, SPACE, RISE, PLATEAU und FALL erkannt. Als Grundlage nutze ich ausschließlich die beiden Variablen „Drahtlänge 1“ in Millimetern und „Durchmesser 1“. Die Segmentlängen werden dabei als L2 für den Space-Bereich, L3 für den Anstieg, L1 für das Plateau und L4 für den Abfall bestimmt. Der Zustand WAIT dient nur zur Initialisierung. Ab dem ersten Messpunkt beginnt die eigentliche Segmentierung dann im Zustand SPACE.

Zunächst glätte ich die Durchmesserwerte mit einem kurzen gleitenden Mittelwert, um Messrauschen zu verringern. Dadurch führen einzelne Ausreißer nicht sofort zu einem Zustandswechsel. Gleichzeitig speichere ich die geglätteten Werte zusammen mit der Drahtlänge in einem Trendpuffer, um daraus die Steigung über ein kurzes Fenster zu bestimmen. Ein Verlauf wird dabei als flach betrachtet, wenn die absolute Steigung unter einem festgelegten Schwellwert liegt.

```python
self.glaettung.append(durchmesser_mm)
d_glatt = sum(self.glaettung) / len(self.glaettung)

self.trend_punkte.append((laenge_mm, d_glatt))
ist_flach = abs(self.trend_steigung()) < FLACH_STEIGUNG
```
Zusätzlich nutze ich adaptive statistische Grenzen, die laufend aus den bisher beobachteten Durchmesserwerten berechnet werden. Dabei werden Mittelwert und Standardabweichung fortlaufend aktualisiert. Sobald genügend Messwerte vorliegen, bestimme ich daraus eine obere und eine untere Grenze in Form von Mittelwert ± k · Standardabweichung. So kann der Algorithmus erkennen, wann der Durchmesser im SPACE-Bereich deutlich ansteigt und der Übergang in den RISE-Zustand beginnt oder wann er im PLATEAU-Bereich deutlich abfällt und damit der Wechsel in den FALL-Zustand startet.

```python
self.stats.update(d_glatt)
sigma_ready = self.stats.ready()
ueber_oben  = sigma_ready and d_glatt > self.stats.upper()
unter_unten = sigma_ready and d_glatt < self.stats.lower()
```
Damit ein Zustandswechsel nicht schon durch einzelne auffällige Messpunkte ausgelöst wird, müssen bestimmte Bedingungen erst über mehrere Messpunkte hinweg bestätigt werden. Eine Grenze muss also mehrfach hintereinander über- oder unterschritten werden, bevor der Zustand tatsächlich geändert wird. Außerdem werden die Zustände RISE und FALL erst dann beendet, wenn der Verlauf über mehrere Punkte wieder nahezu flach ist, also nur noch eine sehr geringe Steigung zeigt. In Kombination mit einer Mindestlänge pro Abschnitt verhindert das, dass der Algorithmus zu schnell zwischen den Zuständen hin- und herspringt, und macht die Profilerkennung insgesamt robuster.

```python
if self.zustand in ("RISE", "FALL"):
    flat_ok = self.gate_flat.feed(ist_flach)
else:
    self.gate_flat.reset()

if self.zustand == "SPACE":
    sig_ok = self.gate_signal.feed(ueber_oben)
elif self.zustand == "PLATEAU":
    sig_ok = self.gate_signal.feed(unter_unten)
else:
    self.gate_signal.reset()
```
Die eigentliche Segmentierung erfolgt über mehrere fest definierte Zustandsübergänge. Der Übergang von SPACE zu RISE findet dann statt, wenn der Durchmesser im SPACE-Bereich über mehrere Messpunkte hinweg die obere Grenze überschreitet und zusätzlich die Mindestlänge des Abschnitts erreicht ist. In diesem Moment wird L2 abgeschlossen. Der Übergang von RISE zu PLATEAU erfolgt dann, wenn der Verlauf nach dem Anstieg über mehrere Punkte wieder als flach erkannt wird und auch hier die Mindestlänge erfüllt ist. Dadurch wird L3 abgeschlossen. Vom Zustand PLATEAU wechselt der Algorithmus in den Zustand FALL, wenn der Durchmesser über mehrere Messpunkte hinweg unter die untere Grenze fällt und die Mindestlänge erreicht wurde. In diesem Schritt wird L1 abgeschlossen. Schließlich endet das Profil, wenn nach dem Abfall der Verlauf wieder über mehrere Punkte als flach erkannt wird und auch der FALL-Abschnitt lang genug ist. Dann wird L4 abgeschlossen und das Profil gilt als vollständig.

Bei den Übergängen, die über die Flachheit des Verlaufs erkannt werden, also von RISE zu PLATEAU und von FALL zum Profilende, korrigiere ich die Übergangsposition zusätzlich über korrigierte_laenge(). Damit gleiche ich die kleine Verzögerung aus, die durch die Glättung der Messwerte und die notwendige Bestätigung über mehrere Punkte entsteht.

Sobald alle vier Segmentlängen L1 bis L4 vorliegen, wird daraus ein Profilobjekt erzeugt und bewertet. Für diese Bewertung führe ich laufende Referenzmittelwerte für alle vier Segmentlängen. Diese Mittelwerte werden inkrementell über alle erkannten Profile aktualisiert. Das erste erkannte Profil dient dabei zur Initialisierung der Referenz. Ein Profil wird als n.i.O. bewertet, wenn mindestens eine der vier Segmentlängen um mehr als ±30 mm vom aktuellen Referenzmittelwert abweicht. In diesem Fall wird zusätzlich ein Ereignis mit Begründung erzeugt und in ein separates Kafka-Topic geschrieben.

Die Ergebnisse werden anschließend über Kafka veröffentlicht. Jedes erkannte Profil wird in das Topic 1031103_1000_profiles geschrieben. Wenn ein Profil als n.i.O. bewertet wird, wird zusätzlich ein entsprechendes Ereignis in das Topic 1031103_1000_profile_events gesendet.

Optional speichere ich außerdem den aktuellen internen Zustand in Redis. Dazu gehören zum Beispiel die aktuelle Profilnummer, der momentane Zustand der State Machine, bereits erkannte Segmentzwischenstände und die aktuellen Referenzmittelwerte. Dadurch kann der Service nach einem Neustart innerhalb derselben Umgebung an der zuletzt verarbeiteten Stelle weitermachen. Die Entkopplung zwischen Profilerkennung und Visualisierung bleibt dabei trotzdem erhalten, weil die eigentliche Datenverteilung weiterhin ausschließlich über Kafka läuft.

## 5. Ergebnisse
Die Anomalieerkennung lässt sich in meinem System direkt anhand der Laufzeitausgaben nachvollziehen. Insbesondere der profile-detector protokolliert, dass Profile erkannt, vermessen und anschließend als i.O. oder n.i.O. bewertet werden. Bei n.i.O.-Profilen wird zusätzlich ausgegeben, welche Segmentlängen vom Referenzwert abweichen und wie groß die Abweichung im Vergleich zum Grenzwert von ±30 mm ist. Dadurch ist nicht nur erkennbar, dass eine Anomalie vorliegt, sondern auch warum.
Die Logs aus einem Programmlauf zeigen dies exemplarisch:

```text
profile-detector  | 2026-02-28T21:28:31 [INFO] profil_erkennung – Profil #1 | L1=5668.2 L2=1154.4 L3=181.1 L4=283.8 | i.O.
profile-detector  | 2026-02-28T21:30:12 [INFO] profil_erkennung – Profil #2 | L1=5662.2 L2=904.7 L3=281.5 L4=282.7 | n.i.O.
profile-detector  | 2026-02-28T21:30:12 [WARNING] profil_erkennung – n.i.O. #2: L2 Abw. 249.7mm > 30.0mm; L3 Abw. 100.5mm > 30.0mm
profile-detector  | 2026-02-28T21:31:53 [INFO] profil_erkennung – Profil #3 | L1=5665.6 L2=921.1 L3=265.9 L4=274.4 | n.i.O.
profile-detector  | 2026-02-28T21:31:53 [WARNING] profil_erkennung – n.i.O. #3: L2 Abw. 108.5mm > 30.0mm; L3 Abw. 34.6mm > 30.0mm
```

Zusätzlich überwacht der six_sigma_monitor die Härte- und Anlasstemperatur mithilfe dynamischer Eingriffsgrenzen auf Basis von Mittelwert ± 3 · Standardabweichung in einem gleitenden Zeitfenster. Auffällige Werte werden dabei nicht nur intern erkannt, sondern auch als Alarm-Events in das Kafka-Topic 1031103_801 geschrieben. Dass die Erkennung tatsächlich funktioniert, lässt sich an den produzierten Kafka-Nachrichten nachvollziehen, zum Beispiel an alarm_start- und alarm_summary-Events.

```text
{"type":"alarm_start","timestamp":1772352030.0,"metric":"fHaertetemperatur (Haerten)","value":985.139}
{"type":"alarm_summary","event_start":1772352030.0,"event_end":1772352032.1,"duration_sec":2.1,"metric":"fHaertetemperatur (Haerten)","peak_value":973.308,"alarm_count":22}
{"type":"alarm_start","timestamp":1772352004.6,"metric":"fAnlasstemperatur (Anlassen)","value":455.6}
{"type":"alarm_summary","event_start":1772352004.6,"event_end":1772352004.6,"duration_sec":0.0,"metric":"fAnlasstemperatur (Anlassen)","peak_value":455.6,"alarm_count":1}
```
Für das Korrelationsmodul wird die Pearson-Korrelation zwischen Drahtgeschwindigkeit und Härtetemperatur innerhalb eines Zeitfensters berechnet. Das Ergebnis wird anschließend als eigene Nachricht in ein separates Kafka-Topic geschrieben, in meinem Fall 1031103_1000_corr. Auf diese Weise wird die berechnete Korrelation nicht nur intern verarbeitet, sondern auch als eigenständiges Ergebnis im Datenstrom verfügbar gemacht. Ein beispielhafter Output aus einem Fenster sieht wie folgt aus:

```text
{"fenster_start": 1772352000.1, "fenster_ende": 1772352300.1, "anzahl_punkte": 3000, "pearson_korrelation": -0.004024611098122389}
{"fenster_start": 1772352300.2, "fenster_ende": 1772352600.2, "anzahl_punkte": 3001, "pearson_korrelation": 0.02678477607142766}
```
Damit ist nachvollziehbar, dass das System sowohl geometrische Abweichungen (Profilerkennung) als auch temperaturbasierte Ausreißer (Six-Sigma) tatsächlich erkennt und zusätzlich statistische Zusammenhänge (Korrelation) kontinuierlich aus dem Datenstrom ableitet und als Events verfügbar macht.

Ich halte Echtzeit-Analysen in der Inline-Kontrolle bei Mubea grundsätzlich für gut umsetzbar. Der Grund ist, dass während der Produktion sowieso laufend Messwerte entstehen. Wenn man diese Daten direkt in ein Streaming-System einspeist, kann man sie in kurzer Zeit auswerten und sofort erkennen, ob etwas auffällig ist. Die Architektur mit Kafka und getrennten Modulen passt dafür gut, weil Datenaufnahme, Analyse und Visualisierung voneinander getrennt sind. So können mehrere Auswertungen parallel laufen, ohne dass sich die Module direkt gegenseitig beeinflussen.

In der Hausarbeit kommen die Daten aus einer JSON-Datei, weil der Datenstrom damit simuliert wird. In einer echten Anlage würden die Messwerte direkt während der Produktion erfasst und über einen Datensammler oder ein vorhandenes Datensystem in Kafka eingespeist werden. Außerdem müsste man das System so betreiben, dass es dauerhaft stabil läuft, also mit persistenter Speicherung, sauberem Monitoring und Logs, damit Neustarts oder kurze Ausfälle nicht dazu führen, dass Zustände und Daten verloren gehen.

Die größten Herausforderungen sehe ich vor allem bei der Datenqualität und bei besonderen Situationen im laufenden Prozess. Auch wenn in dieser Hausarbeit mit einem vorbereiteten Datensatz gearbeitet wird, muss eine Inline-Prozessanalyse grundsätzlich damit umgehen können, dass Messwerte rauschen, kurzfristig ausfallen oder der Prozess zeitweise noch nicht stabil läuft, zum Beispiel beim Anfahren oder beim Wechsel eines Produkts. Solche Situationen können dazu führen, dass entweder zu viele Fehlalarme entstehen oder tatsächliche Abweichungen schwerer zu erkennen sind. Deshalb müssten die verwendeten Regeln, Fenstergrößen und Grenzwerte in der Praxis gemeinsam mit Prozesswissen genauer abgestimmt werden.

Eine weitere Herausforderung ist aus meiner Sicht die praktische Nutzung der Ergebnisse. Es reicht nicht aus, Auffälligkeiten nur zu erkennen oder im Dashboard anzuzeigen. Damit die Analyse im Betrieb einen echten Mehrwert hat, müsste auch klar definiert sein, welche Reaktion auf ein erkanntes Ereignis folgen soll, zum Beispiel das Markieren eines Abschnitts, das Aussortieren von Material oder das Auslösen einer zusätzlichen Prüfung. Dafür wäre außerdem eine saubere Zuordnung der Messwerte zu konkreten Materialabschnitten oder Produktionskontexten notwendig. Das ist besonders wichtig, wenn die Ergebnisse später nicht nur beobachtet, sondern auch für echte Qualitätsentscheidungen genutzt werden sollen.