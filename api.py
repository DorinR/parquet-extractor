from flask import Flask, request, jsonify, send_from_directory
import os
import tempfile
import shutil
from pathlib import Path
from main import extract_papers, convert_cisi_to_markdown
import threading
import uuid
import ir_datasets
from fpdf import FPDF
from markdown import markdown
import html
from tqdm import tqdm
import re
import requests
from bs4 import BeautifulSoup
import wikipediaapi
import mwclient
import html2text
import time

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

def extract_wikir_to_pdf(output_dir, dataset_name='wikir/en1k/validation', limit=100):
    """
    Extract documents from ir-datasets wikir collection and convert them to PDF.
    
    Args:
        output_dir (str): Directory to save the PDF files
        dataset_name (str): Name of the ir-datasets dataset to use
        limit (int): Maximum number of documents to extract (default: 100, use None for all)
    
    Returns:
        dict: Information about the extraction process
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Set a hard limit for safety
    MAX_DOCS = 500
    if limit is None or limit > MAX_DOCS:
        print(f"Warning: Limiting to maximum of {MAX_DOCS} documents for safety")
        limit = MAX_DOCS
    
    print(f"Starting wikir extraction to {output_dir} from dataset {dataset_name} (limit: {limit})")
    
    try:
        # Load the dataset
        print(f"Loading dataset {dataset_name}...")
        dataset = ir_datasets.load(dataset_name)
        print(f"Dataset loaded successfully")
        
        # Get documents iterator
        docs_iter = dataset.docs_iter()
        print(f"Successfully got docs_iter()")
        
        # Get documents
        doc_count = 0
        files_created = []
        errors = []
        
        # Process documents
        for i, doc in enumerate(docs_iter):
            # Hard limit check
            if i >= limit:
                print(f"Reached limit of {limit} documents, stopping")
                break
                
            try:
                if i % 10 == 0:
                    print(f"Processing document {i}/{limit}...")
                
                # Check if document has necessary attributes
                if not hasattr(doc, 'doc_id'):
                    errors.append(f"Document at index {i} has no doc_id")
                    continue
                
                # File already exists check
                filename = f"wikir_{doc.doc_id}.pdf"
                file_path = os.path.join(output_dir, filename)
                if os.path.exists(file_path):
                    print(f"File {filename} already exists, skipping")
                    files_created.append(filename)
                    doc_count += 1
                    continue
                
                # Create a PDF document
                pdf = FPDF()
                pdf.add_page()
                
                # Set font and add title
                pdf.set_font("Arial", "B", 16)
                title = ""
                if hasattr(doc, 'title') and doc.title:
                    title = doc.title
                elif hasattr(doc, 'url') and doc.url:
                    title = f"URL: {doc.url}"
                else:
                    title = f"Document {doc.doc_id}"
                
                # Sanitize title for PDF (FPDF has encoding limitations)
                title = title[:80]  # Truncate long titles
                try:
                    # Try to encode with latin-1 to catch any encoding issues
                    title.encode('latin-1')
                except UnicodeEncodeError:
                    # Fall back to ASCII if there are encoding issues
                    title = ''.join(c if ord(c) < 128 else '_' for c in title)
                    
                pdf.cell(0, 10, txt=title, ln=True)
                
                # Add document ID
                pdf.set_font("Arial", "I", 12)
                pdf.cell(0, 10, txt=f"Document ID: {doc.doc_id}", ln=True)
                
                # Add content
                pdf.set_font("Arial", "", 12)
                
                # Process content based on what's available in the document
                content = ""
                if hasattr(doc, 'text') and doc.text:
                    content = doc.text
                elif hasattr(doc, 'body') and doc.body:
                    content = doc.body
                else:
                    # List all available attributes for debugging
                    attrs = [attr for attr in dir(doc) if not attr.startswith('_')]
                    content_attrs = {}
                    for attr in attrs:
                        try:
                            val = getattr(doc, attr)
                            if isinstance(val, str) and val:
                                content_attrs[attr] = val[:100] + "..." if len(val) > 100 else val
                        except:
                            pass
                    
                    if content_attrs:
                        content = "Available attributes:\n\n" + "\n\n".join(f"{k}: {v}" for k, v in content_attrs.items())
                    else:
                        content = "No text content available for this document"
                
                # Clean the content for PDF
                if content:
                    try:
                        content = html.unescape(content)
                    except:
                        # If unescaping fails, use as is
                        pass
                else:
                    content = "No content available"
                
                try:
                    # Sanitize content for PDF
                    # FPDF has issues with non-latin1 characters, so we'll replace them
                    content = ''.join(c if ord(c) < 128 else '_' for c in content)
                    
                    # Limit content to avoid memory issues
                    content = content[:50000]
                    
                    # Add text
                    pdf.multi_cell(0, 10, txt=content)
                except Exception as text_error:
                    errors.append(f"Error adding text for doc {doc.doc_id}: {str(text_error)}")
                    # Try with minimal content
                    pdf.multi_cell(0, 10, txt="Error processing document content")
                
                try:
                    pdf.output(file_path)
                    print(f"Created PDF: {file_path}")
                    files_created.append(filename)
                    doc_count += 1
                except Exception as pdf_error:
                    errors.append(f"Error saving PDF for doc {doc.doc_id}: {str(pdf_error)}")
                    
            except Exception as doc_error:
                errors.append(f"Error processing doc at index {i}: {str(doc_error)}")
                continue
                
            # Additional limit check
            if doc_count >= limit:
                print(f"Reached document count limit of {limit}")
                break
        
        if files_created:
            print(f"Successfully created {len(files_created)} PDF files in {output_dir}")
        else:
            print(f"No PDF files were created in {output_dir}")
        
        if errors:
            print(f"Encountered {len(errors)} errors during processing")
            for error in errors[:10]:  # Show first 10 errors
                print(f"- {error}")
        
        return {
            "status": "success" if doc_count > 0 else "warning",
            "message": f"Extracted {doc_count} documents to {output_dir} (limited to {limit})",
            "docs_extracted": doc_count,
            "files_created": len(files_created),
            "output_dir": output_dir,
            "files": files_created[:100],  # Limit files list to 100 for response size
            "total_files": len(files_created),
            "errors_count": len(errors),
            "errors": errors[:20]  # Limit errors to 20 for response size
        }
        
    except Exception as e:
        import traceback
        print(f"Error in wikir extraction: {str(e)}")
        print(traceback.format_exc())
        return {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }

@app.route('/api/extract/wikir', methods=['POST'])
def extract_wikir():
    # Get parameters
    output_dir = request.form.get('output_dir', 'wikir_pdfs')
    dataset_name = request.form.get('dataset_name', 'wikir/en1k/validation')
    
    # Set a default limit
    limit = 100
    if 'limit' in request.form:
        try:
            user_limit = int(request.form.get('limit'))
            # Cap the limit at 500 for safety
            limit = min(user_limit, 500)
        except ValueError:
            return jsonify({"error": "Limit must be an integer"}), 400
    
    # Generate a job ID
    job_id = str(uuid.uuid4())
    
    # Set initial job status
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "type": "wikir",
        "dataset_name": dataset_name,
        "output_dir": output_dir,
        "limit": limit,
        "log": ["Job started", f"Using dataset: {dataset_name}", f"Output directory: {output_dir}", f"Document limit: {limit}"]
    }
    
    # Start the extraction in a background thread
    def process_dataset():
        try:
            # Append to job log
            jobs[job_id]["log"].append(f"Starting extraction from {dataset_name}")
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Extract documents
            result = extract_wikir_to_pdf(output_dir, dataset_name, limit)
            
            # Update job status
            jobs[job_id]["status"] = result.get("status", "completed")
            jobs[job_id]["result"] = result
            jobs[job_id]["log"].append(f"Extraction finished with status: {result.get('status')}")
            
            if result.get("status") == "error":
                jobs[job_id]["error"] = result.get("message", "Unknown error")
                jobs[job_id]["traceback"] = result.get("traceback", "")
                jobs[job_id]["log"].append(f"Error: {result.get('message')}")
            
            if "errors" in result and result["errors"]:
                jobs[job_id]["errors_count"] = len(result["errors"])
                jobs[job_id]["errors"] = result["errors"][:20]  # Limit to first 20 errors
                jobs[job_id]["log"].append(f"Encountered {len(result['errors'])} errors during processing")
            
            if os.path.exists(output_dir):
                files = os.listdir(output_dir)
                jobs[job_id]["file_count"] = len(files)
                jobs[job_id]["log"].append(f"Found {len(files)} files in output directory")
                
                if not files:
                    jobs[job_id]["log"].append("Warning: No files were created in the output directory")
            else:
                jobs[job_id]["file_count"] = 0
                jobs[job_id]["log"].append("Warning: Output directory does not exist")
            
        except Exception as e:
            import traceback
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["traceback"] = traceback.format_exc()
            jobs[job_id]["log"].append(f"Job failed with error: {str(e)}")
    
    # Start the processing thread
    thread = threading.Thread(target=process_dataset)
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "running",
        "message": f"Processing wikir dataset {dataset_name} in the background (limit: {limit})"
    })

def count_tokens(text):
    """
    Count the number of tokens in a text string.
    Simple tokenization by splitting on whitespace and punctuation.
    
    Args:
        text (str): The text to tokenize
    
    Returns:
        int: Number of tokens
    """
    if not text:
        return 0
        
    # Replace punctuation with spaces, then split on whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    tokens = re.split(r'\s+', text.lower())
    # Filter out empty tokens
    tokens = [t for t in tokens if t]
    return len(tokens)

def analyze_wikir_dataset(dataset_name='wikir/en1k/validation', limit=None):
    """
    Analyze the wikir dataset and count tokens in all documents.
    
    Args:
        dataset_name (str): Name of the ir-datasets dataset to analyze
        limit (int): Maximum number of documents to analyze (None for all)
    
    Returns:
        dict: Analysis results
    """
    try:
        # Load the dataset
        print(f"Loading dataset {dataset_name}...")
        dataset = ir_datasets.load(dataset_name)
        print(f"Dataset loaded successfully")
        
        # Get documents iterator
        docs_iter = dataset.docs_iter()
        print(f"Successfully got docs_iter()")
        
        # Initialize counters
        doc_count = 0
        total_tokens = 0
        token_counts = []
        errors = []
        
        # Process documents
        for i, doc in enumerate(docs_iter):
            try:
                if i % 100 == 0:
                    print(f"Processing document {i}...")
                
                # Get document text content
                content = ""
                if hasattr(doc, 'text') and doc.text:
                    content = doc.text
                elif hasattr(doc, 'body') and doc.body:
                    content = doc.body
                
                # Count tokens
                tokens = count_tokens(content)
                token_counts.append(tokens)
                total_tokens += tokens
                
                doc_count += 1
                
                # Check if we've reached the limit
                if limit is not None and doc_count >= limit:
                    print(f"Reached limit of {limit} documents")
                    break
                    
            except Exception as e:
                error_msg = f"Error processing doc at index {i}: {str(e)}"
                print(error_msg)
                errors.append(error_msg)
        
        # Calculate statistics
        avg_tokens = total_tokens / doc_count if doc_count > 0 else 0
        
        # Find min and max token counts
        min_tokens = min(token_counts) if token_counts else 0
        max_tokens = max(token_counts) if token_counts else 0
        
        # Create result dictionary
        result = {
            "dataset": dataset_name,
            "document_count": doc_count,
            "total_tokens": total_tokens,
            "average_tokens_per_doc": avg_tokens,
            "min_tokens": min_tokens,
            "max_tokens": max_tokens,
            "errors": errors[:20] if errors else []  # Limit error reporting
        }
        
        print(f"Analysis complete. Found {doc_count} documents with {total_tokens} total tokens.")
        return result
        
    except Exception as e:
        import traceback
        error_msg = f"Error analyzing dataset: {str(e)}"
        traceback_str = traceback.format_exc()
        print(error_msg)
        print(traceback_str)
        return {
            "status": "error",
            "message": error_msg,
            "traceback": traceback_str
        }

@app.route('/api/analyze/wikir', methods=['POST'])
def analyze_wikir():
    # Get parameters
    dataset_name = request.form.get('dataset_name', 'wikir/en1k/validation')
    
    # Set a default limit (use None for all documents)
    limit = None
    if 'limit' in request.form:
        try:
            limit = int(request.form.get('limit'))
        except ValueError:
            return jsonify({"error": "Limit must be an integer"}), 400
    
    # Generate a job ID
    job_id = str(uuid.uuid4())
    
    # Set initial job status
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "type": "wikir_analysis",
        "dataset_name": dataset_name,
        "limit": limit,
        "log": ["Job started", f"Analyzing dataset: {dataset_name}", f"Document limit: {limit if limit else 'None (all documents)'}"]
    }
    
    # Start the analysis in a background thread
    def process_analysis():
        try:
            # Append to job log
            jobs[job_id]["log"].append(f"Starting analysis of {dataset_name}")
            
            # Analyze dataset
            result = analyze_wikir_dataset(dataset_name, limit)
            
            # Update job status
            if "status" in result and result["status"] == "error":
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = result.get("message", "Unknown error")
                if "traceback" in result:
                    jobs[job_id]["traceback"] = result["traceback"]
                jobs[job_id]["log"].append(f"Analysis failed: {result.get('message')}")
            else:
                jobs[job_id]["status"] = "completed"
                jobs[job_id]["result"] = result
                jobs[job_id]["log"].append(f"Analysis completed successfully")
                jobs[job_id]["log"].append(f"Found {result['document_count']} documents with {result['total_tokens']} total tokens")
                jobs[job_id]["log"].append(f"Average tokens per document: {result['average_tokens_per_doc']:.2f}")
            
        except Exception as e:
            import traceback
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["traceback"] = traceback.format_exc()
            jobs[job_id]["log"].append(f"Job failed with error: {str(e)}")
    
    # Start the processing thread
    thread = threading.Thread(target=process_analysis)
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "running",
        "message": f"Analyzing wikir dataset {dataset_name} in the background"
    })

def fetch_ww2_articles(limit=500):
    """
    Fetch Wikipedia articles related to World War II.
    
    Args:
        limit (int): Maximum number of articles to fetch
        
    Returns:
        list: List of article titles
    """
    print(f"Starting to fetch up to {limit} WW2 articles")
    
    # Connect to Wikipedia with a proper user agent
    user_agent = "WW2ArticlesExtractor/1.0 (papers-python-extractor; contact@example.com)"
    site = mwclient.Site('en.wikipedia.org', clients_useragent=user_agent)
    
    # Get category members for World War II
    category = site.Categories['World_War_II']
    
    titles = []
    errors = []
    
    # Fetch articles from the category
    try:
        for i, page in enumerate(category.members()):
            if i >= limit:
                break
                
            if page.namespace == 0:  # Regular articles only, not talk pages etc.
                titles.append(page.name)
                
            if i % 50 == 0:
                print(f"Fetched {i} articles so far")
    except Exception as e:
        errors.append(f"Error fetching category members: {str(e)}")
        
    print(f"Fetched {len(titles)} article titles from World War II category")
    
    # If we need more articles, try to fetch from related categories
    related_categories = [
        'World_War_II_by_country',
        'Military_equipment_of_World_War_II',
        'Battles_of_World_War_II',
        'Military_operations_of_World_War_II',
        'World_War_II_military_personnel'
    ]
    
    if len(titles) < limit:
        for cat_name in related_categories:
            if len(titles) >= limit:
                break
                
            try:
                category = site.Categories[cat_name]
                for i, page in enumerate(category.members()):
                    if len(titles) >= limit:
                        break
                        
                    if page.namespace == 0 and page.name not in titles:
                        titles.append(page.name)
                        
                    if i % 50 == 0:
                        print(f"Fetched {i} articles from category {cat_name}")
            except Exception as e:
                errors.append(f"Error fetching category {cat_name}: {str(e)}")
    
    print(f"Total articles fetched: {len(titles)}")
    return titles, errors

def download_wiki_article_to_pdf(title, output_dir):
    """
    Download a Wikipedia article and convert it to PDF.
    
    Args:
        title (str): The title of the Wikipedia article
        output_dir (str): Directory to save the PDF
        
    Returns:
        dict: Status of the download and conversion
    """
    try:
        print(f"Starting to download article: {title}")
        
        # Initialize Wikipedia API with a proper user agent
        user_agent = "WW2ArticlesExtractor/1.0 (papers-python-extractor; contact@example.com)"
        wiki_wiki = wikipediaapi.Wikipedia(
            user_agent=user_agent,
            language='en'
        )
        
        # Get the page
        page = wiki_wiki.page(title)
        
        if not page.exists():
            print(f"Page does not exist: {title}")
            return {
                "status": "error",
                "message": f"Page '{title}' does not exist"
            }
            
        # Get content
        content = page.text
        summary = page.summary
        
        print(f"Successfully retrieved content for: {title}")
        print(f"Content length: {len(content)} characters")
        
        # Generate a safe filename
        safe_title = "".join([c if c.isalnum() or c in ' ._-' else '_' for c in title])
        filename = f"{safe_title}.pdf"
        filepath = os.path.join(output_dir, filename)
        
        print(f"Saving to: {filepath}")
        
        try:
            # Create PDF with UTF-8 support
            pdf = FPDF()
            # Add a font with UTF-8 support (using built-in Helvetica)
            pdf.set_font("Helvetica", "", 12)
            pdf.add_page()
            
            # Add title
            pdf.set_font_size(16)
            pdf.set_font("Helvetica", "B")
            
            # Encode title safely
            safe_title_text = title[:80]
            # Remove non-ASCII characters
            safe_title_text = ''.join(c if ord(c) < 128 else ' ' for c in safe_title_text)
            
            pdf.cell(0, 10, txt=safe_title_text, ln=True)
            
            # Add summary
            if summary:
                pdf.set_font_size(12)
                pdf.set_font("Helvetica", "I")
                # Make sure summary text is ASCII-compatible
                safe_summary = ''.join(c if ord(c) < 128 else ' ' for c in summary[:500])
                pdf.multi_cell(0, 10, txt=f"Summary: {safe_summary}...")
            
            # Add content
            pdf.set_font_size(10)
            pdf.set_font("Helvetica", "")
            
            # Split content into smaller chunks to avoid memory issues
            # Using even smaller chunks for more reliability
            content_chunks = [content[i:i+1000] for i in range(0, min(len(content), 50000), 1000)]
            
            print(f"Split content into {len(content_chunks)} chunks")
            
            for i, chunk in enumerate(content_chunks):
                try:
                    # Sanitize content for PDF - replace non-ASCII chars with spaces
                    sanitized_chunk = ''.join(c if ord(c) < 128 else ' ' for c in chunk)
                    # Also replace any control characters
                    sanitized_chunk = ''.join(c if ord(c) >= 32 or c in '\n\r\t' else ' ' for c in sanitized_chunk)
                    pdf.multi_cell(0, 8, txt=sanitized_chunk)
                    
                    if i % 5 == 0:
                        print(f"Processed chunk {i}/{len(content_chunks)}")
                except Exception as chunk_error:
                    print(f"Error processing chunk {i}: {str(chunk_error)}")
                    # Continue with next chunk
            
            # Save PDF
            print(f"Attempting to save PDF to {filepath}")
            try:
                pdf.output(filepath)
                print(f"Successfully saved PDF: {filepath}")
                
                # Double check file exists and has size
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    print(f"Verified file exists with size: {os.path.getsize(filepath)} bytes")
                else:
                    raise Exception(f"File was not saved or has zero size: {filepath}")
                
                return {
                    "status": "success",
                    "message": f"Downloaded '{title}' to '{filename}'",
                    "file": filename
                }
            except Exception as save_error:
                print(f"Error saving PDF file: {str(save_error)}")
                return {
                    "status": "error",
                    "message": f"Error saving PDF file: {str(save_error)}"
                }
            
        except Exception as pdf_error:
            print(f"PDF generation error for {title}: {str(pdf_error)}")
            return {
                "status": "error",
                "message": f"Error creating PDF for '{title}': {str(pdf_error)}"
            }
            
    except Exception as e:
        print(f"Error downloading article {title}: {str(e)}")
        return {
            "status": "error",
            "message": f"Error downloading '{title}': {str(e)}"
        }

@app.route('/api/extract/ww2', methods=['POST'])
def extract_ww2_articles():
    # Get parameters
    output_dir = request.form.get('output_dir', 'ww2_articles')
    limit = int(request.form.get('limit', 200))
    
    # Cap the limit at 1000 for safety
    limit = min(limit, 1000)
    
    # Generate a job ID
    job_id = str(uuid.uuid4())
    
    # Set initial job status
    jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "type": "ww2_wiki",
        "output_dir": output_dir,
        "limit": limit,
        "log": ["Job started", f"Output directory: {output_dir}", f"Article limit: {limit}"]
    }
    
    # Start the extraction in a background thread
    def process_ww2_articles():
        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Make sure the directory exists and is writable
            if not os.path.exists(output_dir):
                error_msg = f"Failed to create output directory: {output_dir}"
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = error_msg
                jobs[job_id]["log"].append(error_msg)
                return
                
            try:
                # Test write access by creating a temporary file
                test_file = os.path.join(output_dir, "test_write.txt")
                with open(test_file, 'w') as f:
                    f.write("Testing write access")
                os.remove(test_file)
                jobs[job_id]["log"].append(f"Successfully verified write access to {output_dir}")
            except Exception as write_error:
                error_msg = f"Output directory {output_dir} is not writable: {str(write_error)}"
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = error_msg
                jobs[job_id]["log"].append(error_msg)
                return
            
            # Fetch article titles
            jobs[job_id]["log"].append(f"Fetching up to {limit} WW2 article titles from Wikipedia")
            titles, fetch_errors = fetch_ww2_articles(limit)
            
            if fetch_errors:
                jobs[job_id]["log"].append(f"Encountered {len(fetch_errors)} errors while fetching titles")
                jobs[job_id]["fetch_errors"] = fetch_errors
            
            if not titles:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = "No article titles were found"
                jobs[job_id]["log"].append("No article titles were found. Job aborted.")
                return
            
            jobs[job_id]["log"].append(f"Found {len(titles)} article titles")
            jobs[job_id]["article_count"] = len(titles)
            jobs[job_id]["titles"] = titles[:100]  # Store first 100 titles for reference
            
            # Download articles and convert to PDF
            total_articles = len(titles)
            successful = 0
            failed = 0
            download_errors = []
            
            jobs[job_id]["log"].append(f"Starting to download and convert {total_articles} articles")
            
            for i, title in enumerate(titles):
                if i % 10 == 0:
                    jobs[job_id]["log"].append(f"Progress: {i}/{total_articles} articles processed")
                
                result = download_wiki_article_to_pdf(title, output_dir)
                
                if result["status"] == "success":
                    successful += 1
                else:
                    failed += 1
                    download_errors.append(f"{title}: {result['message']}")
                
                # Sleep briefly to avoid hammering the Wikipedia API
                time.sleep(1)
            
            # Update job status
            if successful > 0:
                jobs[job_id]["status"] = "completed"
            else:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = "Failed to download any articles successfully"
                
            jobs[job_id]["successful"] = successful
            jobs[job_id]["failed"] = failed
            jobs[job_id]["log"].append(f"Completed: Successfully downloaded {successful} articles, failed: {failed}")
            
            if download_errors:
                jobs[job_id]["download_errors"] = download_errors[:100]  # Store first 100 errors
                jobs[job_id]["log"].append(f"Encountered {len(download_errors)} download errors")
            
            # Count files in output directory
            if os.path.exists(output_dir):
                files = os.listdir(output_dir)
                jobs[job_id]["file_count"] = len(files)
                jobs[job_id]["log"].append(f"Found {len(files)} files in output directory")
                
                if len(files) == 0 and successful > 0:
                    jobs[job_id]["log"].append("WARNING: Directory is empty despite successful downloads reported")
                
        except Exception as e:
            import traceback
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["traceback"] = traceback.format_exc()
            jobs[job_id]["log"].append(f"Job failed with error: {str(e)}")
    
    # Start the processing thread
    thread = threading.Thread(target=process_ww2_articles)
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "status": "running",
        "message": f"Downloading WW2 Wikipedia articles in the background (limit: {limit})"
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4000) 