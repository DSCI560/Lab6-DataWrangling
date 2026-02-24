from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import os
import pymysql.cursors
import datetime

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

db_conf = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASS", "admin123"),
    "db": os.environ.get("DB_NAME", "oil_wells"),
    "cursorclass": pymysql.cursors.DictCursor,
    "charset": "utf8mb4"
}

def get_conn():
    return pymysql.connect(**db_conf)

def clean_row(row):
    for k, v in list(row.items()):
        if isinstance(v, (datetime.date, datetime.datetime)):
            row[k] = v.isoformat()
    return row

@app.route('/api/wells')
def api_wells():
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT id, well_name, well_number, api, latitude, longitude, county, state, operator, address, qc_status, created_at FROM wells")
            wells = cur.fetchall()
            for w in wells:
                cur.execute(
                    "SELECT id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stages, volume, volume_units, treatment_pressure, max_treatment_rate, treatment_type, lbs_proppant, acid_percent, additional_info FROM stimulations WHERE well_id=%s",
                    (w['id'],)
                )
                s = cur.fetchall()
                w['stimulations'] = [clean_row(x) for x in s]
            wells = [clean_row(w) for w in wells]
        return jsonify(wells)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except:
            pass

@app.route('/api/wells/<int:wid>')
def api_well(wid):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT id, well_name, well_number, api, latitude, longitude, county, state, operator, address, qc_status, raw_text, created_at FROM wells WHERE id=%s", (wid,))
            w = cur.fetchone()
            if not w:
                return jsonify({}), 404
            cur.execute(
                "SELECT id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stages, volume, volume_units, treatment_pressure, max_treatment_rate, treatment_type, lbs_proppant, acid_percent, additional_info FROM stimulations WHERE well_id=%s",
                (wid,)
            )
            s = cur.fetchall()
            w['stimulations'] = [clean_row(x) for x in s]
            w = clean_row(w)
        return jsonify(w)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except:
            pass

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'favicon.ico')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)