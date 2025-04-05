# Papers Python Extractor

A tool to extract papers from Parquet files into individual Markdown files.

## Overview

This tool allows you to extract academic papers stored in Parquet format (such as those from Hugging Face datasets) and save them as individual Markdown files. It's particularly useful for researchers and data scientists who want to work with paper content in a more readable format.

## Features

- Extract a specified number of papers (default: 1000)
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

1. Copy your Parquet file containing papers to the project directory
2. Activate your virtual environment:
   ```
   source env/bin/activate
   ```
3. Run the extraction script:
   ```
   python main.py your_parquet_file.parquet
   ```

### Optional Parameters

- `--output_dir` - Specify a custom output directory (default: "extracted_papers")
- `--num_papers` - Number of papers to extract (default: 1000)
- `--seed` - Random seed for reproducibility (default: 42)

For example:
```
python main.py your_parquet_file.parquet --output_dir custom_folder --num_papers 500
```

## Output

The extracted papers will be saved in the specified output directory (default: "extracted_papers"). Each file will contain:

- YAML frontmatter with metadata (if available)
- The full content of the paper in Markdown format

Filenames will be based on paper titles when available, otherwise sequential numbering will be used.

## Notes

- The script attempts to automatically detect which column contains the paper content
- If it can't identify the content column, it will raise an error with instructions
- The extraction process may take some time depending on the size of your Parquet file 