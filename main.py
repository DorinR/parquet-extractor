import os
import pandas as pd
from tqdm import tqdm
import random
import argparse
from pathlib import Path
import re

def extract_papers(parquet_file, output_dir, num_papers=1000, seed=42):
    """
    Extract papers from a parquet file and save them as markdown files.
    
    Args:
        parquet_file (str): Path to the parquet file
        output_dir (str): Directory to save the markdown files
        num_papers (int): Number of papers to extract
        seed (int): Random seed for reproducibility
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Set random seed for reproducibility
    random.seed(seed)
    
    print(f"Loading parquet file: {parquet_file}")
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    
    # Get the column names to understand the structure
    print(f"Columns in the dataset: {df.columns.tolist()}")
    
    # Determine which column contains the markdown content
    # This is an assumption that needs to be adjusted based on your actual data
    content_column = None
    possible_columns = ['text', 'content', 'markdown', 'mmd', 'body']
    
    for col in possible_columns:
        if col in df.columns:
            content_column = col
            break
    
    if not content_column:
        # If none of the expected columns are found, use the first text-like column
        for col in df.columns:
            if df[col].dtype == 'object':  # Usually string columns are object type
                sample = df[col].iloc[0] if not df.empty else ""
                if isinstance(sample, str) and len(sample) > 100:  # Heuristic for content
                    content_column = col
                    break
    
    if not content_column:
        raise ValueError("Could not identify a column containing paper content. Please specify manually.")
    
    print(f"Using '{content_column}' as the content column")
    
    # Get the title column (again, assumption)
    title_column = None
    for col in ['title', 'name', 'paper_title']:
        if col in df.columns:
            title_column = col
            break
    
    # Sample papers
    num_available = len(df)
    if num_available < num_papers:
        print(f"Warning: Only {num_available} papers available. Extracting all.")
        papers_to_extract = df
    else:
        indices = random.sample(range(num_available), num_papers)
        papers_to_extract = df.iloc[indices]
    
    print(f"Extracting {len(papers_to_extract)} papers to {output_dir}")
    
    # Extract and save papers
    for i, (_, paper) in enumerate(tqdm(papers_to_extract.iterrows(), total=len(papers_to_extract))):
        # Generate filename from title if available, otherwise use index
        if title_column and title_column in paper:
            # Clean title to create a valid filename
            filename = "".join(c if c.isalnum() or c in " -_" else "_" for c in str(paper[title_column]))
            filename = filename.strip().replace(" ", "_")[:100]  # Limit length
            filename = f"{i+1:04d}_{filename}.md"
        else:
            filename = f"paper_{i+1:04d}.md"
        
        # Get content
        content = paper[content_column]
        
        # Add metadata as YAML front matter if available
        metadata = []
        metadata.append("---")
        for col in df.columns:
            if col != content_column and not pd.isna(paper[col]):
                # Skip binary or very long data
                if isinstance(paper[col], str) and len(paper[col]) < 1000:
                    metadata.append(f"{col}: {paper[col]}")
                elif not isinstance(paper[col], (bytes, bytearray)):
                    metadata.append(f"{col}: {paper[col]}")
        metadata.append("---\n")
        
        # Write to file
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
            f.write("\n".join(metadata))
            f.write(content)
    
    print(f"Successfully extracted {len(papers_to_extract)} papers to {output_dir}")

def convert_cisi_to_markdown(cisi_file, output_dir):
    """
    Convert CISI dataset to markdown files.
    
    Args:
        cisi_file (str): Path to the CISI.ALL file
        output_dir (str): Directory to save the markdown files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Processing CISI file: {cisi_file}")
    
    # Read the CISI.ALL file
    with open(cisi_file, 'r', encoding='utf-8', errors='replace') as file:
        content = file.read()
    
    # Split the file by document markers (.I followed by a number)
    documents = re.split(r'\.I\s+(\d+)', content)
    
    # The first element is empty, skip it
    documents = documents[1:]
    
    # Process documents in pairs (id and content)
    num_docs = len(documents) // 2
    print(f"Found {num_docs} documents in CISI dataset")
    
    for i in range(0, len(documents), 2):
        if i+1 >= len(documents):
            break
            
        doc_id = documents[i].strip()
        doc_content = documents[i+1].strip()
        
        # Parse document content
        sections = {}
        current_section = None
        section_content = []
        
        for line in doc_content.split('\n'):
            if line.startswith('.'):
                # Save the previous section
                if current_section:
                    sections[current_section] = '\n'.join(section_content).strip()
                    section_content = []
                
                # Start a new section
                current_section = line[1:].strip()
            else:
                section_content.append(line)
        
        # Save the last section
        if current_section:
            sections[current_section] = '\n'.join(section_content).strip()
        
        # Extract title, author, and content
        title = sections.get('T', f"Document {doc_id}")
        author = sections.get('A', 'Unknown')
        content = sections.get('W', '')
        
        # Create filename
        clean_title = "".join(c if c.isalnum() or c in " -_" else "_" for c in title)
        clean_title = clean_title.strip().replace(" ", "_")[:100]
        filename = f"cisi_{doc_id.zfill(4)}_{clean_title}.md"
        
        # Create markdown content
        markdown = [
            "---",
            f"doc_id: {doc_id}",
            f"title: {title}",
            f"author: {author}"
        ]
        
        # Add other metadata if available
        for key, value in sections.items():
            if key not in ['T', 'A', 'W', 'X']:
                markdown.append(f"{key}: {value}")
        
        markdown.append("---\n")
        markdown.append(content)
        
        # Write to file
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
            f.write('\n'.join(markdown))
    
    print(f"Successfully converted {num_docs} CISI documents to {output_dir}")

def main():
    parser = argparse.ArgumentParser(description='Extract papers from various sources and save as markdown')
    
    # Add subparsers for different modes
    subparsers = parser.add_subparsers(dest='mode', help='Mode of operation')
    
    # Parquet mode parser
    parquet_parser = subparsers.add_parser('parquet', help='Extract papers from a parquet file')
    parquet_parser.add_argument('file', type=str, help='Path to the parquet file')
    parquet_parser.add_argument('--output_dir', type=str, default='extracted_papers',
                      help='Directory to save the markdown files')
    parquet_parser.add_argument('--num_papers', type=int, default=1000,
                      help='Number of papers to extract')
    parquet_parser.add_argument('--seed', type=int, default=42,
                      help='Random seed for reproducibility')
    
    # CISI mode parser
    cisi_parser = subparsers.add_parser('cisi', help='Convert CISI dataset to markdown')
    cisi_parser.add_argument('file', type=str, default='data/cisi/CISI.ALL',
                    help='Path to the CISI.ALL file')
    cisi_parser.add_argument('--output_dir', type=str, default='cisi_papers',
                    help='Directory to save the markdown files')
    
    # For backwards compatibility, if no mode is specified, assume parquet mode
    parser.add_argument('parquet_file', nargs='?', type=str, 
                        help='Path to the parquet file (for backwards compatibility)')
    parser.add_argument('--output_dir', type=str, default='extracted_papers',
                        help='Directory to save the markdown files (for backwards compatibility)')
    parser.add_argument('--num_papers', type=int, default=1000,
                        help='Number of papers to extract (for backwards compatibility)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility (for backwards compatibility)')
    
    args = parser.parse_args()
    
    # Determine mode and run appropriate function
    if args.mode == 'parquet':
        extract_papers(args.file, args.output_dir, args.num_papers, args.seed)
    elif args.mode == 'cisi':
        convert_cisi_to_markdown(args.file, args.output_dir)
    else:
        # Backwards compatibility mode
        if args.parquet_file:
            extract_papers(args.parquet_file, args.output_dir, args.num_papers, args.seed)
        else:
            parser.print_help()

if __name__ == "__main__":
    main()
