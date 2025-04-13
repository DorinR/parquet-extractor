"""
Test script to extract a small number of documents from wikir dataset to PDF
"""
from api import extract_wikir_to_pdf
import os

# Create output directory
output_dir = "wikir_test_output"
os.makedirs(output_dir, exist_ok=True)

# Extract just 2 documents
print("Testing wikir extraction...")
result = extract_wikir_to_pdf(output_dir, dataset_name='wikir/en1k/validation', limit=2)

# Print result
print("\nExtraction result:")
for key, value in result.items():
    if key == 'errors' and isinstance(value, list) and len(value) > 5:
        print(f"errors: [{len(value)} errors, first 5 shown below]")
        for i, error in enumerate(value[:5]):
            print(f"  {i+1}. {error}")
    elif key == 'files' and isinstance(value, list) and len(value) > 5:
        print(f"files: [{len(value)} files]")
    else:
        print(f"{key}: {value}")

# Check output directory
print(f"\nChecking output directory {output_dir}:")
if os.path.exists(output_dir):
    files = os.listdir(output_dir)
    print(f"Found {len(files)} files:")
    for file in files:
        file_size = os.path.getsize(os.path.join(output_dir, file))
        print(f"  - {file} ({file_size} bytes)")
else:
    print(f"Output directory {output_dir} does not exist!") 