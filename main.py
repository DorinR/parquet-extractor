import os
import pandas as pd
from tqdm import tqdm
import random
import argparse
from pathlib import Path

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

def main():
    parser = argparse.ArgumentParser(description='Extract papers from a parquet file and save as markdown')
    parser.add_argument('parquet_file', type=str, help='Path to the parquet file')
    parser.add_argument('--output_dir', type=str, default='extracted_papers',
                        help='Directory to save the markdown files')
    parser.add_argument('--num_papers', type=int, default=1000,
                        help='Number of papers to extract')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    extract_papers(args.parquet_file, args.output_dir, args.num_papers, args.seed)

if __name__ == "__main__":
    main()
