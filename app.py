from flask import Flask, request, jsonify, render_template, send_file
import pandas as pd
import requests
from io import BytesIO

app = Flask(__name__)

# Dictionary to hold uploaded DataFrames
uploads = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    df = pd.read_excel(file)
    df['Latitude'] = None
    df['Longitude'] = None
    uploads[file.filename] = df
    columns = df.columns.tolist()

    # Create a preview of the first 10 rows
    preview_rows = df.head(10).values.tolist()
    return jsonify(columns=columns, preview={"columns": columns, "rows": preview_rows})


@app.route('/get_address_count', methods=['POST'])
def get_address_count():
    try:
        data = request.get_json(force=True)  # Explicitly parse JSON and raise error if not possible
    except Exception as e:
        app.logger.error("Invalid JSON format received")
        return jsonify({"error": "Invalid request format"}), 400

    # Check essential keys
    if 'file_name' not in data or 'address_column' not in data:
        return jsonify({"error": "Request missing 'file_name' or 'address_column'"}), 400

    file_name = data['file_name']
    address_column = data['address_column']

    # Confirm file presence
    if file_name not in uploads:
        return jsonify({"error": "File not found"}), 404

    df = uploads[file_name]

    # Validate the column exists
    if address_column not in df.columns:
        return jsonify({"error": "Column not found in the file"}), 400

    return jsonify(len(df[address_column]))



@app.route('/geocode_chunk', methods=['POST'])
def geocode_chunk():
    try:
        address_column = request.form['address_column']
        index = int(request.form['index'])
        file_name = request.form['file_name']

        if file_name not in uploads:
            return jsonify({"error": "File not found"}), 404

        df = uploads[file_name]
        address = df[address_column].iloc[index]

        lat, lon = None, None
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address, "format": "json"},
                headers={"User-Agent": "GeocodingApp"},
                timeout=10
            )
            data = response.json()
            if data:
                lat = data[0]["lat"]
                lon = data[0]["lon"]
                df.at[index, 'Latitude'] = lat
                df.at[index, 'Longitude'] = lon
        except requests.exceptions.RequestException as req_err:
            return jsonify({"error": "Network error", "address": address}), 500

        uploads[file_name] = df
        return jsonify({"latitude": lat, "longitude": lon})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download_result', methods=['POST'])
def download_result():
    data = request.get_json()
    file_name = data['file_name']

    if file_name not in uploads:
        return jsonify({"error": "File not found"}), 404

    df = uploads[file_name]
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)

    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     download_name='geocoded_result.xlsx', as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
