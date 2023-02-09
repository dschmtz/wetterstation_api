"""
Das ist die Hauptdatei, die wesentliche Bestandteile der REST-API definiert.
Insbesondere das Routing der HTTP-Anfragen wird hier festgelegt sowie die 
Verarbeitung der Requests und die Transformation der Daten.
"""

import json
import datetime

from datetime import datetime, timedelta
from flask import Flask, make_response, jsonify, request

from database import MongoDBConnector


# MongoDB Atlas Data API Key
DB_API_KEY = "???"

# Konfiguration des MongoDB-Konnektors
db = MongoDBConnector(DB_API_KEY)

db.set_database("prod")
db.set_data_source("Cluster0")

# Definition der Flask-App
app = Flask(__name__)

@app.route("/")
def index():
    """
    Anzeige einer statischen Seite, um Verfügbarkeit der API zu zeigen.
    Zeigt außerdem die aufrufbaren URIs mit jeweiliger Methode an.
    """
    return """
        <h2>WWI-20-DSA Wetterstation REST-API v1.1</h2>
        <table>
            <tr>
                <th>Methode</th>
                <th>URI</th>
                <th>Beschreibung</th>
            </tr>
            <tr>
                <td>GET</td>
                <td><a href="/measurements/all">/measurements/all</a></td>
                <td>Gibt die 20 neusten Messungen im 
                    <a href="https://www.cumuluswiki.org/a/Realtime.txt">Cumulus Realtime.txt-Format</a> 
                    zurück
                </td>
            </tr>
            <tr>
                <td>GET</td>
                <td><a href="/measurements/latest">/measurements/latest</a></td>
                <td>Gibt die neuste Messungen im 
                    <a href="https://www.cumuluswiki.org/a/Realtime.txt">Cumulus Realtime.txt-Format</a> 
                    zurück
                </td>
            </tr>
            <tr>
                <td>POST</td>
                <td><a href="/measurements/insert/">/measurements/insert/&#60;token&#62;</a></td>
                <td>Fügt eine neue Messung, bestehend aus Temperatur, Luftfeuchtigkeit und Luftdruck, 
                    in die Datenbank ein
                </td>
            </tr>
            <tr>
                <td>GET</td>
                <td><a href="/predictions/latest">/predictions/latest</a></td>
                <td>Gibt die Features und Predictions der letzten 5 Vorhersagen des 
                    <a href="https://github.com/yassdenn/WeatherPrediction">ML-Modells</a> 
                    zurück, sowie die aktuellen Messwerte
                </td>
            </tr>
            <tr>
                <td>POST</td>
                <td><a href="/predictions/insert/">/predictions/insert/&#60;token&#62;</a></td>
                <td>Fügt die aktuelle Vorhersage hinzu, sofern innerhalb einer Stunde nicht bereits 
                    eine Vorhersage hinzugefügt wurde
                </td>
            </tr>
        </table>
        <br/><br/>
        Der Quellcode ist <a href="https://github.com/dschmtz/wetterstation_api">hier</a> verfügbar.
    """

@app.route("/measurements/insert/<token>", methods=["POST"])
def insert_measurement(token):
    """
    Definition des Endpunkts zur Speicherung der Messwerte in der Datenbank.
    Um nur verifizierte Eintragungen des Raspberry PIs zu zulassen,
    wird ein Token zusätzlich bei einer Anfrage verlangt. 
    """
    # Eintragung authentifizieren
    if token != "jQyygCygQqb3cL8v":
        return "Invalid token", 403

    print(request.data)

    # Leere Inserts nicht verarbeiten
    if not request.data:
        return "Bad request", 400

    # Je nach Laufzeitumgebung ist eine andere
    # Systemzeit vorhanden, daher wird die UTC-Zeit verwendet
    date = datetime.utcnow()
    date = date + timedelta(hours=1)

    # Da die Datenbank ebenfalls über eine REST-API angebunden ist,
    # wird das Datetime-Objekt in zwei Strings überführt
    data = {
        "date": date.strftime("%d/%m/%y"),
        "time": date.strftime("%H:%M:%S"),
    }

    # In das data-dict die Messwerte aus der Anfrage einfügen
    # und gleichzeitig den Druck von Pa in hPa umrechnen
    for key, val in json.loads(request.data).items():
        if key == "pressure":
            val = int(val) / 100
        data[key] = val

    # Persistente Speicherung in Datenbank
    db.insert("measurements", data)
    
    return jsonify(request.form)

@app.route("/measurements/all")
def measurements_all():
    """
    Gibt die 20 neusten Messwerte aus der Datenbank zurück.
    Dabei sind die Messwerte nach ihrer Aktualität sortiert
    und werden in einem für Cumulus verarbeitbaren Textformat
    zurückgegeben.
    """
    result = ""

    for measure in db.find("measurements"):
        print(measure)

        # Fehlende Werte mit Dummy-Werten füllen
        data = {
            "date": measure.get("date", "01/01/23"),
            "time": measure.get("time", "00:00:00"),
            "temperature": measure.get("temperature", 0),
            "humidity": measure.get("humidity", 0),
            "pressure": measure.get("pressure", 0),
        }

        # Transformation der Daten in Cumulus geeignetes Format
        result = transform_data(data) + "\n" + result

    # Antwort als Text an Client schicken
    response = make_response(result, 200)
    response.mimetype = "text/plain"
    return response


@app.route("/measurements/latest")
def measurements_latest():
    """
    Gibt den neusten Messwert (der als letztes in die Datenbank 
    eingefügt wurde) zurück. Dabei wird dieser in ein für Cumulus
    verarbeitbares Format transformiert.
    """
    result = ""

    for measure in db.find("measurements", limit=1):
        # Fehlende Werte mit Dummy-Werten füllen
        data = {
            "date": measure.get("date", "01/01/23"),
            "time": measure.get("time", "00:00:00"),
            "temperature": measure.get("temperature", 0),
            "humidity": measure.get("humidity", 0),
            "pressure": measure.get("pressure", 0),
        }

        # Transformation der Daten in Cumulus geeignetes Format
        result = transform_data(data) + "\n" + result

    # Antwort als Text an Client schicken
    response = make_response(result, 200)
    response.mimetype = "text/plain"
    return response


@app.route("/predictions/latest")
def predictions_latest():
    """
    Gibt die letzten fünf Predicitions zurück und die
    zugehörige Temperature, Humidity, Pressure, welche
    dazu verwendet wurden. Außerdem gibt sie die benötigten
    Messwerte für eine aktuelle Prediction zurück.
    """
    result = ""

    for measure in db.find("measurements", limit=1):
        # Zur Prediction wird nur die Temperatur, Luftfeuchtigkeit
        # und der Luftdruck benötigt.
        data = {
            "temperature": measure.get("temperature", 0),
            "humidity": measure.get("humidity", 0),
            "pressure": measure.get("pressure", 0)
        }
        print(data)

        result = transform_prediction(data, annotated=False) + ", " + result

    for pred in db.find("predictions", limit=5):
        data = {
            "temperature": pred.get("temperature", 0),
            "humidity": pred.get("humidity", 0),
            "pressure": pred.get("pressure", 0),
            "class": pred.get("class", 4)
        }
        print(data)

        # Vorhersagen an Ergebnis anhängen
        result = transform_prediction(data) + ", " + result

    # Ergebnis als JSON-Array zurück geben,
    # damit dieses direkt in JavaScript verarbeitet
    # werden kann und nicht konvertiert werden muss
    result = "[" + result[:-2] + "]"
    response = make_response(result, 200)
    response.mimetype = "application/json"
    return response

@app.route("/predictions/insert/<token>", methods=["POST"])
def insert_prediction(token):
    """
    Ähnlich wie bei den Messwerten werden auch
    die Predicitions nur über einen gültigen Token
    gespeichert. Außerdem wird sichergestellt, dass
    maximal einmal pro Stunde eine neue Predicition
    gespeichert wird, da diese clientseitig berechnet
    wird und der Client nicht weiß, wann die letzte
    durchgeführt wurde.
    """
    # Zeitpunkt der letzten gespeicherten Vorhersage erhalten
    for measure in db.find("predictions", limit=1):
        time_str = measure.get("date", "01/01/23") + " " + measure.get("time", "00:00:00")
        last_time = datetime.strptime(time_str, "%d/%m/%y %H:%M:%S")

        print(last_time)

    now = datetime.utcnow()
    now = now + timedelta(hours=1)

    # Wenn diese nicht älter als 1 Stunde ist
    # wird die aktuelle Vorhersage nicht gespeichert
    if now - last_time < timedelta(hours=1):
        return "", 200

    # Überprüfung des Tokens
    if token != "KPQHYyj4L6qKbULV":
        return "Invalid token", 403

    print(request.data)

    if not request.data:
        return "Bad request", 400

    date = datetime.utcnow()
    date = date + timedelta(hours=1)

    # Zur Prediction wird die aktuelle Uhrzeit gespeichert
    data = {
        "date": date.strftime("%d/%m/%y"),
        "time": date.strftime("%H:%M:%S"),
    }

    # Ergebnisse der Predicition, die in der Anfrage enthalten
    # sind, in das data-dict überführen
    for key, val in json.loads(request.data).items():
        data[key] = val

    # Persistente Speicherung in MongoDB
    db.insert("predictions", data)
    
    return jsonify(request.form)


def transform_prediction(data, annotated=True):
    """
    Transformation der Daten in das benötigte Format
    für die Prediction. Da der letzte Wert nicht
    annotiert ist, ist eine Fallunterscheidung
    eingebaut.
    """
    result = ""

    result += str(data["temperature"]) + ", "
    result += str(data["humidity"]) + ", "
    result += str(data["pressure"])

    if annotated:
        result += ", " + str(data["class"])

    return result


def transform_data(data):
    """
    Transformiert die Daten in ein textartiges Format,
    das von Cumulus verarbeitet werden kann:
    https://www.cumuluswiki.org/a/Realtime.txt

    Ein Großteil der Werte wird mit Dummy-Werten
    aufgefüllt, da diese nicht vorhanden sind.
    """
    result = str(data["date"]) + " "
    result += str(data["time"]) + " "
    result += str(data["temperature"]) + " "
    result += str(data["humidity"]) + " "

    result += "0 " * 6

    result += str(data["pressure"]) + " "

    # Restliche Werte mit Dummy-Werten füllen
    result += "W 0 km/h C hPa mm 0 +0.1 "
    result += 8 * "0 " + 6 * "00:00 0 "
    result +=  "819 " + 11 * "0 "
    result += "NNW 2040 ft " + 5 * "1 "

    # Letztes Leerzeichen entfernen
    return result[:-1]


# REST-API bereitstellen
app.run()