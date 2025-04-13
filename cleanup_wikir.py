#!/usr/bin/env python3
"""
Cleanup script to remove all PDF files in the wikir_pdfs directory
"""
import os
import shutil
import argparse

def cleanup_wikir_pdfs(directory="wikir_pdfs", confirm=False):
    """
    Clean up all PDF files in the specified directory
    
    Args:
        directory (str): Directory to clean up
        confirm (bool): Whether to ask for confirmation before deleting
    """
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist!")
        return
    
    # Count files
    files = [f for f in os.listdir(directory) if f.endswith('.pdf')]
    total_files = len(files)
    total_size = sum(os.path.getsize(os.path.join(directory, f)) for f in files)
    
    print(f"Found {total_files} PDF files in {directory}")
    print(f"Total size: {total_size / (1024*1024):.2f} MB")
    
    if total_files == 0:
        print("No files to delete.")
        return
    
    # Ask for confirmation
    if not confirm:
        answer = input(f"Are you sure you want to delete all {total_files} PDF files? (y/N): ")
        if answer.lower() != 'y':
            print("Operation cancelled.")
            return
    
    # Delete files
    print(f"Deleting {total_files} files...")
    
    # Option 1: Remove individual files
    if total_files < 1000:
        for f in files:
            file_path = os.path.join(directory, f)
            try:
                os.remove(file_path)
                if total_files < 100:  # Only print for smaller numbers
                    print(f"Deleted: {f}")
            except Exception as e:
                print(f"Error deleting {f}: {e}")
    # Option 2: For large numbers, remove and recreate the directory
    else:
        try:
            print(f"Removing entire directory {directory} and recreating it...")
            shutil.rmtree(directory)
            os.makedirs(directory)
            print("Directory cleared successfully.")
        except Exception as e:
            print(f"Error clearing directory: {e}")
            
    # Verify
    if os.path.exists(directory):
        remaining = [f for f in os.listdir(directory) if f.endswith('.pdf')]
        if remaining:
            print(f"Warning: {len(remaining)} PDF files still remain in the directory.")
        else:
            print("All PDF files have been successfully removed.")
    else:
        print("Warning: The directory no longer exists!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up wikir PDF files")
    parser.add_argument("--directory", type=str, default="wikir_pdfs",
                       help="Directory containing the PDF files to clean up")
    parser.add_argument("--force", action="store_true",
                       help="Force delete without confirmation")
    
    args = parser.parse_args()
    cleanup_wikir_pdfs(args.directory, args.force) 