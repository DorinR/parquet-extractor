import requests
import time
import argparse
import os

def check_health(api_base):
    response = requests.get(f"{api_base}/api/health")
    return response.json()

def extract_parquet(api_base, file_path, output_dir='extracted_papers', num_papers=1000, seed=42):
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f)}
        data = {
            'output_dir': output_dir,
            'num_papers': str(num_papers),
            'seed': str(seed)
        }
        response = requests.post(f"{api_base}/api/extract/parquet", files=files, data=data)
    
    return response.json()

def extract_cisi(api_base, file_path, output_dir='cisi_papers'):
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f)}
        data = {'output_dir': output_dir}
        response = requests.post(f"{api_base}/api/extract/cisi", files=files, data=data)
    
    return response.json()

def get_job_status(api_base, job_id):
    response = requests.get(f"{api_base}/api/jobs/{job_id}")
    return response.json()

def list_files(api_base, output_dir):
    response = requests.get(f"{api_base}/api/files", params={'output_dir': output_dir})
    return response.json()

def download_file(api_base, filename, output_dir, save_path=None):
    response = requests.get(
        f"{api_base}/api/files/{filename}", 
        params={'output_dir': output_dir},
        stream=True
    )
    
    if response.status_code == 200:
        if save_path is None:
            save_path = filename
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return {"status": "success", "file": save_path}
    else:
        return response.json()

def main():
    parser = argparse.ArgumentParser(description='API client for Papers Python Extractor')
    parser.add_argument('--api_base', type=str, default='http://localhost:4000',
                        help='Base URL for the API server')
    
    # Add subcommands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Health check
    health_parser = subparsers.add_parser('health', help='Check API health')
    
    # Extract parquet
    parquet_parser = subparsers.add_parser('parquet', help='Extract papers from a parquet file')
    parquet_parser.add_argument('file', type=str, help='Path to the parquet file')
    parquet_parser.add_argument('--output_dir', type=str, default='extracted_papers',
                               help='Output directory')
    parquet_parser.add_argument('--num_papers', type=int, default=1000,
                               help='Number of papers to extract')
    parquet_parser.add_argument('--seed', type=int, default=42,
                               help='Random seed')
    parquet_parser.add_argument('--wait', action='store_true',
                               help='Wait for the job to complete')
    
    # Extract CISI
    cisi_parser = subparsers.add_parser('cisi', help='Convert CISI dataset to markdown')
    cisi_parser.add_argument('file', type=str, help='Path to the CISI file')
    cisi_parser.add_argument('--output_dir', type=str, default='cisi_papers',
                            help='Output directory')
    cisi_parser.add_argument('--wait', action='store_true',
                            help='Wait for the job to complete')
    
    # Job status
    job_parser = subparsers.add_parser('job', help='Get job status')
    job_parser.add_argument('job_id', type=str, help='Job ID')
    
    # List files
    list_parser = subparsers.add_parser('list', help='List files in an output directory')
    list_parser.add_argument('output_dir', type=str, help='Output directory')
    
    # Download file
    download_parser = subparsers.add_parser('download', help='Download a file')
    download_parser.add_argument('filename', type=str, help='Filename to download')
    download_parser.add_argument('--output_dir', type=str, required=True,
                                help='Output directory where the file is located')
    download_parser.add_argument('--save_path', type=str, help='Path to save the file locally')
    
    args = parser.parse_args()
    
    if args.command == 'health':
        result = check_health(args.api_base)
        print(result)
    
    elif args.command == 'parquet':
        result = extract_parquet(
            args.api_base, 
            args.file, 
            args.output_dir, 
            args.num_papers, 
            args.seed
        )
        job_id = result.get('job_id')
        print(f"Job started: {result}")
        
        if args.wait and job_id:
            print("Waiting for job to complete...")
            while True:
                status = get_job_status(args.api_base, job_id)
                print(f"Status: {status['status']}")
                if status['status'] in ['completed', 'failed']:
                    print(f"Final status: {status}")
                    break
                time.sleep(2)
    
    elif args.command == 'cisi':
        result = extract_cisi(args.api_base, args.file, args.output_dir)
        job_id = result.get('job_id')
        print(f"Job started: {result}")
        
        if args.wait and job_id:
            print("Waiting for job to complete...")
            while True:
                status = get_job_status(args.api_base, job_id)
                print(f"Status: {status['status']}")
                if status['status'] in ['completed', 'failed']:
                    print(f"Final status: {status}")
                    break
                time.sleep(2)
    
    elif args.command == 'job':
        result = get_job_status(args.api_base, args.job_id)
        print(result)
    
    elif args.command == 'list':
        result = list_files(args.api_base, args.output_dir)
        print(f"Files in {args.output_dir}:")
        if 'files' in result:
            for file in result['files']:
                print(f"- {file}")
            print(f"Total: {result['file_count']} files")
        else:
            print(result)
    
    elif args.command == 'download':
        result = download_file(
            args.api_base, 
            args.filename, 
            args.output_dir, 
            args.save_path
        )
        print(result)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 