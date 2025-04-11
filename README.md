# Papers Python Extractor

A tool to extract papers from Parquet files or CISI datasets into individual Markdown files.

## Overview

This tool allows you to:
1. Extract academic papers stored in Parquet format (such as those from Hugging Face datasets) and save them as individual Markdown files.
2. Convert CISI dataset files (a classic information retrieval dataset) to Markdown format.

It's particularly useful for researchers and data scientists who want to work with paper content in a more readable format.

## Features

- Extract a specified number of papers from Parquet files (default: 1000)
- Convert CISI dataset documents to Markdown
- Save papers as individual Markdown files
- Include metadata as YAML frontmatter
- Intelligent column detection for content and titles
- Progress bar to track extraction

## Setup

1. Clone this repository
2. Ensure you have Python installed
3. Set up a virtual environment:
   ```
   python -m venv env
   ```
4. Activate the virtual environment:
   - On macOS/Linux:
     ```
     source env/bin/activate
     ```
   - On Windows:
     ```
     env\Scripts\activate
     ```
5. Install dependencies:
   ```
   pip install pandas pyarrow tqdm
   ```

## Usage

### Extracting Papers from Parquet Files

You can extract papers from Parquet files in two ways:

**Method 1: Using the parquet subcommand**
```
python main.py parquet path/to/your/file.parquet --output_dir extracted_papers --num_papers 1000
```

**Method 2: Using the legacy format (for backward compatibility)**
```
python main.py path/to/your/file.parquet --output_dir extracted_papers --num_papers 1000
```

### Converting CISI Dataset to Markdown

To convert the CISI dataset to Markdown format:

```
python main.py cisi data/cisi/CISI.ALL --output_dir cisi_papers
```

### Optional Parameters

For Parquet extraction:
- `--output_dir` - Specify a custom output directory (default: "extracted_papers")
- `--num_papers` - Number of papers to extract (default: 1000)
- `--seed` - Random seed for reproducibility (default: 42)

For CISI conversion:
- `--output_dir` - Specify a custom output directory (default: "cisi_papers")

## Output

The extracted papers will be saved in the specified output directory. Each file will contain:

- YAML frontmatter with metadata (if available)
- The full content of the paper in Markdown format

Filenames will be based on paper titles or document IDs when available, otherwise sequential numbering will be used.

## Notes

- The Parquet extraction script attempts to automatically detect which column contains the paper content
- If it can't identify the content column, it will raise an error with instructions
- The CISI conversion handles the specific format of the CISI.ALL file, parsing the various sections (.T for title, .A for author, .W for content)
- The extraction process may take some time depending on the size of your files 

## REST API

The application also provides a REST API for extracting papers without using the command line.

### Starting the API Server

```
python api.py
```

The server will run on http://localhost:5000 by default.

### API Endpoints

#### Health Check
```
GET /api/health
```
Returns the API health status.

#### Extract Parquet Papers
```
POST /api/extract/parquet
```
Upload a parquet file and extract papers.

Parameters:
- `file`: The parquet file to process (multipart/form-data)
- `output_dir`: Output directory (optional, default: "extracted_papers")
- `num_papers`: Number of papers to extract (optional, default: 1000)
- `seed`: Random seed (optional, default: 42)

#### Extract CISI Documents
```
POST /api/extract/cisi
```
Upload a CISI file and convert to markdown.

Parameters:
- `file`: The CISI file to process (multipart/form-data)
- `output_dir`: Output directory (optional, default: "cisi_papers")

#### List Jobs
```
GET /api/jobs
```
List all extraction jobs and their status.

#### Get Job Status
```
GET /api/jobs/{job_id}
```
Get the status of a specific job.

#### List Files
```
GET /api/files?output_dir={directory}
```
List all files in the specified output directory.

#### Download File
```
GET /api/files/{filename}?output_dir={directory}
```
Download a specific file from the output directory.

### Example API Usage

Using curl to extract papers from a parquet file:
```bash
curl -X POST -F "file=@path/to/papers.parquet" -F "output_dir=my_papers" -F "num_papers=500" http://localhost:5000/api/extract/parquet
```

Check job status:
```bash
curl http://localhost:5000/api/jobs/{job_id}
```

List extracted files:
```bash
curl http://localhost:5000/api/files?output_dir=my_papers
```

### Python API Client

A Python API client is also provided for easier integration with other Python applications:

```python
python api_client.py --help
```

Example usage:

```python
# Check API health
python api_client.py health

# Extract papers from a parquet file
python api_client.py parquet /path/to/papers.parquet --output_dir my_papers --num_papers 500 --wait

# Convert CISI file to markdown
python api_client.py cisi /path/to/cisi.all --output_dir cisi_papers --wait

# Check job status
python api_client.py job <job_id>

# List files in output directory
python api_client.py list my_papers

# Download a specific file
python api_client.py download paper_0001.md --output_dir my_papers --save_path ./downloaded_paper.md 