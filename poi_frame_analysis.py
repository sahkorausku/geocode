from flask import Blueprint, request, jsonify, send_file
import pandas as pd
from io import BytesIO
from geopy.distance import geodesic

poi_analysis = Blueprint('poi_analysis', __name__)

uploads = {}
last_results_file = BytesIO()

@poi_analysis.route('/upload-files', methods=['POST'])
def upload_files():
    if 'frames_file' not in request.files or 'pois_file' not in request.files:
        return jsonify({"error": "Both files are required"}), 400

    frames_file = request.files['frames_file']
    pois_file = request.files['pois_file']

    if frames_file.filename == '' or pois_file.filename == '':
        return jsonify({"error": "File selection is required"}), 400

    frames_df = pd.read_excel(frames_file)
    pois_df = pd.read_excel(pois_file)

    uploads['frames'] = frames_df
    uploads['pois'] = pois_df

    return jsonify({
        "frames_columns": frames_df.columns.tolist(),
        "pois_columns": pois_df.columns.tolist()
    })

@poi_analysis.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()
    max_radius = data.get('max_radius')
    frame_lat_col = data.get('frame_lat_col')
    frame_lon_col = data.get('frame_lon_col')
    poi_lat_col = data.get('poi_lat_col')
    poi_lon_col = data.get('poi_lon_col')

    if any(col is None for col in [frame_lat_col, frame_lon_col, poi_lat_col, poi_lon_col, max_radius]):
        return jsonify({"error": "All column selections and radius are required"}), 400

    max_radius = int(max_radius)
    
    frames_df = uploads.get('frames')
    pois_df = uploads.get('pois')

    if frames_df is None or pois_df is None:
        return jsonify({"error": "Files not processed properly"}), 400

    results = []
    frame_data = []  # Collect raw frame data to send to the client
    frame_coords = frames_df[[frame_lat_col, frame_lon_col]].dropna().to_numpy()
    poi_coords = pois_df[[poi_lat_col, poi_lon_col]].dropna().to_numpy()
    
    for frame in frames_df.itertuples():
        frame_dict = frame._asdict()
        frame_data.append({
            'lat': getattr(frame, frame_lat_col),
            'lon': getattr(frame, frame_lon_col)
        })

    for poi_index, (poi_lat, poi_lon) in enumerate(poi_coords):
        poi_row = pois_df.iloc[poi_index].to_dict()

        within_radius = False
        for frame_index, (frame_lat, frame_lon) in enumerate(frame_coords):
            frame_row = frames_df.iloc[frame_index].to_dict()

            distance = geodesic((poi_lat, poi_lon), (frame_lat, frame_lon)).meters
            if distance <= max_radius:
                within_radius = True
                result_row = {
                    **poi_row,
                    **frame_row,
                    'poi_lat': poi_lat,
                    'poi_lon': poi_lon,
                    'frame_lat': frame_lat,
                    'frame_lon': frame_lon,
                    'radius': distance
                }
                results.append(result_row)

        if not within_radius:
            results.append({
                **poi_row,
                'poi_lat': poi_lat,
                'poi_lon': poi_lon,
                'frame_lat': None,
                'frame_lon': None,
                'radius': None
            })

    results_df = pd.DataFrame(results)
    global last_results_file

    last_results_file = BytesIO()
    with pd.ExcelWriter(last_results_file, engine='xlsxwriter') as writer:
        results_df.to_excel(writer, index=False)
    last_results_file.seek(0)

    # Send frame_data with the response
    return jsonify({"data": results, "frames": frame_data})


@poi_analysis.route('/download-results', methods=['GET'])
def download_results():
    last_results_file.seek(0)
    return send_file(last_results_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     download_name='poi_frame_analysis.xlsx', as_attachment=True)
