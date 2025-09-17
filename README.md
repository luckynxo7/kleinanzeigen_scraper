# Kleinanzeigen Scraper

Dieses Projekt stellt einen einfachen Scraper für die deutsche
Kleinanzeigen‑Plattform bereit und verpackt die Funktionalität in
einer Streamlit‑Webanwendung.  Ziel ist es, alle Inserate eines
Händlers automatisch abzurufen, die relevanten Daten aus Titel und
Beschreibung zu extrahieren und sämtliche Bilder zu speichern.  Die
Daten können anschließend als CSV heruntergeladen und per Bulk‑Upload
in eine WordPress‑Instanz importiert werden.

## Funktionsumfang

* **Händlerseiten scrapen** – das Tool ruft die Profilseite eines
  Händlers auf, sammelt die Links aller Inserate und folgt dabei
  ggf. vorhandenen Paginierungslinks.
* **Anzeige‑Details parsen** – für jede Anzeige werden Titel,
  Beschreibung und eine Liste der Bilder ausgelesen.  Mittels
  regulärer Ausdrücke werden Felder wie Felgenhersteller,
  Reifengröße, Einpresstiefe usw. erkannt.
* **Bilder herunterladen** – sämtliche Bilder eines Inserats werden
  gespeichert und stehen als ZIP‑Archiv zum Download bereit.
* **Streamlit‑Frontend** – über eine einfache Weboberfläche können
  beliebig viele Händlerlinks eingegeben werden.  Nach Abschluss
  steht eine CSV‑Datei zum Download zur Verfügung.

## Voraussetzungen

Installieren Sie die benötigten Abhängigkeiten in einer virtuellen
Umgebung:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Nutzung

Starten Sie die Streamlit‑App mit:

```bash
streamlit run app.py
```

Im Browser geben Sie die URLs der gewünschten Händlerprofile ein
(jeweils eine URL pro Zeile) und klicken auf **„Scrape starten“**.
Sobald der Prozess abgeschlossen ist, können Sie die Ergebnisse als
CSV sowie ein ZIP‑Archiv mit allen Bildern herunterladen.

## Hinweise

* Der Scraper führt HTTP‑Requests zu Kleinanzeigen aus.  Achten Sie
  darauf, die Nutzungsbedingungen und etwaige Rate‑Limits der
  Plattform einzuhalten.  Der Nutzer dieses Projekts hat nach eigener
  Aussage die Erlaubnis seitens Kleinanzeigen und der Händler zum
  Scraping.
* Eine geringe Verzögerung zwischen den Requests kann in der
  Anwendung eingestellt werden, um Sperren zu vermeiden.
* Die Erkennung der Felder aus den Anzeigen erfolgt per Heuristik.
  Abhängig von der Formulierung der Inserate können einzelne Felder
  fehlen oder falsch interpretiert werden.  Bei Bedarf lässt sich
  die Logik in `scraper.py` anpassen.
