from flask import Flask, request, jsonify, send_from_directory
import os
import tempfile
import shutil
from pathlib import Path
from main import extract_papers, convert_cisi_to_markdown
import threading
import uuid

app = Flask(__name__)

# Store job status
jobs = {}

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"})

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    return jsonify(jobs)

@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(jobs[job_id])

@app.route('/api/extract/parquet', methods=['POST'])
def extract_parquet():
    # Check if file was uploaded
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    # Get parameters
    output_dir = request.form.get('output_dir', 'extracted_papers')
    num_papers = int(request.form.get('num_papers', 1000))
    seed = int(request.form.get('seed', 42))
    
    # Create a temporary directory to store the uploaded file
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)
    
    # Save the uploaded file
    file.save(temp_file_path)
    
    # Generate a job ID
    job_id = str(uuid.uuid4())
    
    # Set initial job status
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "type": "parquet",
        "file": file.filename,
        "output_dir": output_dir,
        "num_papers": num_papers,
        "seed": seed
    }
    
    # Start the extraction in a background thread
    def process_file():
        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Extract papers
            extract_papers(temp_file_path, output_dir, num_papers, seed)
            
            # Update job status
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["file_count"] = len(os.listdir(output_dir))
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir)
    
    # Start the processing thread
    thread = threading.Thread(target=process_file)
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "running",
        "message": f"Processing {file.filename} in the background"
    })

@app.route('/api/extract/cisi', methods=['POST'])
def extract_cisi():
    # Check if file was uploaded
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    # Get parameters
    output_dir = request.form.get('output_dir', 'cisi_papers')
    
    # Create a temporary directory to store the uploaded file
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file.filename)
    
    # Save the uploaded file
    file.save(temp_file_path)
    
    # Generate a job ID
    job_id = str(uuid.uuid4())
    
    # Set initial job status
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "type": "cisi",
        "file": file.filename,
        "output_dir": output_dir
    }
    
    # Start the conversion in a background thread
    def process_file():
        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Convert CISI to markdown
            convert_cisi_to_markdown(temp_file_path, output_dir)
            
            # Update job status
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["file_count"] = len(os.listdir(output_dir))
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir)
    
    # Start the processing thread
    thread = threading.Thread(target=process_file)
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "running",
        "message": f"Processing {file.filename} in the background"
    })

@app.route('/api/files', methods=['GET'])
def list_files():
    output_dir = request.args.get('output_dir', '')
    
    if not output_dir or not os.path.exists(output_dir):
        return jsonify({"error": f"Directory '{output_dir}' not found"}), 404
    
    files = os.listdir(output_dir)
    return jsonify({
        "output_dir": output_dir,
        "file_count": len(files),
        "files": files
    })

@app.route('/api/files/<path:filename>', methods=['GET'])
def download_file(filename):
    output_dir = request.args.get('output_dir', '')
    
    if not output_dir or not os.path.exists(output_dir):
        return jsonify({"error": f"Directory '{output_dir}' not found"}), 404
    
    if not os.path.exists(os.path.join(output_dir, filename)):
        return jsonify({"error": f"File '{filename}' not found in '{output_dir}'"}), 404
    
    return send_from_directory(output_dir, filename)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4000) 