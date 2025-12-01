#!/usr/bin/env python3
"""
Paper PDF Download Script
Read JSON files from biology and computer_science folders under output directory
Download all papers where is_oa is true
"""

import json
import os
import requests
import time
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import hashlib
import random

# Try to import cloudscraper, fallback to requests if not available
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False
    logging.warning("cloudscraper not available. Install with: pip install cloudscraper")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_papers.log'),
        logging.StreamHandler()
    ]
)

# Ensure all loggers use the same level and propagate to root logger
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)

class PaperDownloader:
    def __init__(self, output_dir="output", download_dir="/mnt/data/taxonomy/computer_science", max_workers=5, use_cloudscraper=True):
        self.output_dir = Path(output_dir)
        self.download_dir = Path(download_dir)
        self.max_workers = max_workers
        self.use_cloudscraper = use_cloudscraper and CLOUDSCRAPER_AVAILABLE
        
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Enhanced request headers to bypass anti-bot detection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Initialize sessions
        self.requests_session = requests.Session()
        self.requests_session.headers.update(self.headers)
        
        # Initialize cloudscraper session if available
        if self.use_cloudscraper:
            try:
                self.cloudscraper_session = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'darwin',
                        'desktop': True
                    },
                    delay=2
                )
                self.cloudscraper_session.headers.update(self.headers)
                logging.info("Cloudscraper session initialized")
            except Exception as e:
                logging.warning(f"Failed to initialize cloudscraper: {e}")
                self.use_cloudscraper = False
                self.cloudscraper_session = None
        else:
            self.cloudscraper_session = None
        
        # Statistics
        self.stats = {
            'total_files': 0,
            'total_papers': 0,
            'total_cited_by_works': 0,
            'oa_papers': 0,
            'oa_cited_by_works': 0,
            'downloaded': 0,
            'failed': 0,
            'skipped': 0,
            'errors': {
                '403_forbidden': 0,
                '401_unauthorized': 0,
                '404_not_found': 0,
                'other_http_errors': 0,
                'network_errors': 0,
                'integrity_failures': 0
            }
        }
    
    def get_json_files(self):
        """Get all JSON file paths from computer_science folder only"""
        json_files = []
        
        # Only process computer_science folder
        cs_dir = self.output_dir / "computer_science"
        if cs_dir.exists():
            json_files.extend(cs_dir.glob("*.json"))
        
        return json_files
    
    def extract_paper_info(self, json_file):
        """Extract paper information from JSON file including papers and cited_by_works"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            professor_name = data.get('professor_info', {}).get('name', 'Unknown')
            department = data.get('professor_info', {}).get('department', 'unknown')
            papers = data.get('papers', [])
            
            all_oa_papers = []
            
            # Process main papers
            for paper in papers:
                open_access = paper.get('open_access', {})
                if open_access.get('is_oa') and open_access.get('oa_url'):
                    paper_info = self.create_paper_info(paper, professor_name, department, 'main_paper')
                    if paper_info:
                        all_oa_papers.append(paper_info)
                
                # Process cited_by_works for each paper
                cited_by_works = paper.get('cited_by_works', [])
                for cited_work in cited_by_works:
                    cited_open_access = cited_work.get('open_access', {})
                    if cited_open_access.get('is_oa') and cited_open_access.get('oa_url'):
                        cited_paper_info = self.create_paper_info(cited_work, professor_name, department, 'cited_by_work')
                        if cited_paper_info:
                            all_oa_papers.append(cited_paper_info)
            
            return all_oa_papers
            
        except Exception as e:
            logging.error(f"Error parsing file {json_file}: {e}")
            return []
    
    def create_paper_info(self, paper, professor_name, department, paper_type):
        """Create paper info dictionary"""
        open_access = paper.get('open_access', {})
        
        # Extract OpenAlex ID from the id field
        paper_id = paper.get('id', '')
        openalex_id = ''
        
        # Check if id is a URL and extract the W-number
        if paper_id and isinstance(paper_id, str):
            if 'openalex.org/' in paper_id:
                # Extract the W-number from URL like "https://openalex.org/W2766540688"
                openalex_id = paper_id.split('/')[-1]
            else:
                # If id is not a URL, use it as is
                openalex_id = paper_id
        
        if not openalex_id:
            return None
            
        return {
            'title': paper.get('title', 'Unknown Title'),
            'doi': paper.get('doi', ''),
            'oa_url': open_access.get('oa_url'),
            'professor': professor_name,
            'department': department,
            'paper_id': paper.get('id', ''),
            'openalex_id': openalex_id,
            'publication_date': paper.get('publication_date', ''),
            'paper_type': paper_type
        }
    
    def get_domain_specific_headers(self, url):
        """Get domain-specific headers to bypass restrictions"""
        domain = urlparse(url).netloc.lower()
        headers = self.headers.copy()
        
        if 'acm.org' in domain:
            headers.update({
                'Referer': 'https://dl.acm.org/',
                'Origin': 'https://dl.acm.org',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate'
            })
        elif 'ieee.org' in domain:
            headers.update({
                'Referer': 'https://ieeexplore.ieee.org/',
                'Origin': 'https://ieeexplore.ieee.org',
                'Sec-Fetch-Site': 'same-origin'
            })
        elif 'springer.com' in domain:
            headers.update({
                'Referer': 'https://link.springer.com/',
                'Origin': 'https://link.springer.com',
                'Sec-Fetch-Site': 'same-origin'
            })
        elif 'arxiv.org' in domain:
            headers.update({
                'Referer': 'https://arxiv.org/',
                'Origin': 'https://arxiv.org',
                'Sec-Fetch-Site': 'same-origin'
            })
        
        return headers
    
    def download_file(self, url, headers, timeout=60):
        """Download file with simple error handling"""
        try:
            # Use the appropriate session to download
            session = self.get_session_for_domain(url)
            
            # For ACM URLs, try to access the abstract page first to establish session
            if 'dl.acm.org' in url and '/doi/pdf/' in url:
                # Extract DOI from PDF URL
                doi = url.split('/doi/pdf/')[-1]
                abstract_url = f"https://dl.acm.org/doi/{doi}"
                
                try:
                    # Visit abstract page to establish session
                    abstract_response = session.get(
                        abstract_url,
                        headers=headers,
                        timeout=30,
                        allow_redirects=True
                    )
                    if abstract_response.status_code != 200:
                        logging.debug(f"ACM abstract page returned status {abstract_response.status_code}")
                except Exception as e:
                    logging.debug(f"Failed to access ACM abstract page: {e}")
                
                # Add small delay to mimic human behavior
                time.sleep(random.uniform(1, 3))
            
            # Attempt download
            response = session.get(
                url, 
                headers=headers, 
                timeout=timeout, 
                stream=True,
                allow_redirects=True
            )
            
            # Handle specific HTTP error codes
            if response.status_code == 403:
                return None, '403_forbidden'
            elif response.status_code == 401:
                return None, '401_unauthorized'
            elif response.status_code == 404:
                return None, '404_not_found'
            elif response.status_code >= 400:
                return None, f'http_{response.status_code}'
            
            response.raise_for_status()
            return response, None
            
        except requests.exceptions.RequestException as e:
            return None, 'network_errors'
    
    def verify_file_integrity(self, file_path, expected_size=None):
        """Verify downloaded file integrity and return detailed analysis"""
        if not file_path.exists():
            return False, "File does not exist"
        
        file_size = file_path.stat().st_size
        
        # Check if file is too small (likely incomplete)
        if file_size < 1024:  # Less than 1KB
            return False, f"File too small ({file_size} bytes), likely incomplete or error page"
        
        # Check if file appears to be a valid document
        try:
            with open(file_path, 'rb') as f:
                # Read more content for better analysis
                content = f.read(1024)  # Read first 1KB for analysis
                
                # Check for PDF
                if content.startswith(b'%PDF'):
                    return True, "Valid PDF file"
                
                # Check for HTML content
                if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html') or content.startswith(b'<HTML'):
                    # Try to determine what kind of HTML page this is
                    content_str = content.decode('utf-8', errors='ignore').lower()
                    
                    if 'error' in content_str or 'not found' in content_str:
                        return False, "HTML error page (likely 'file not found' or server error)"
                    elif 'access denied' in content_str or 'forbidden' in content_str:
                        return False, "HTML access denied page (likely permission issue)"
                    elif 'captcha' in content_str or 'robot' in content_str:
                        return False, "HTML captcha/anti-bot page (likely blocked by anti-bot protection)"
                    elif 'login' in content_str or 'sign in' in content_str:
                        return False, "HTML login page (likely requires authentication)"
                    elif 'redirect' in content_str or 'moved' in content_str:
                        return False, "HTML redirect page (likely URL has changed)"
                    else:
                        return False, "HTML page (likely not a PDF document)"
                
                # Check for plain text error messages
                if content.startswith(b'error') or content.startswith(b'Error'):
                    return False, "Plain text error message"
                
                # Check for XML content
                if content.startswith(b'<?xml') or content.startswith(b'<xml'):
                    return False, "XML content (likely API response or error message)"
                
                # Check for JSON content
                if content.startswith(b'{') or content.startswith(b'['):
                    try:
                        json.loads(content[:100])  # Try to parse as JSON
                        return False, "JSON content (likely API response or error message)"
                    except:
                        pass
                
                # Check for other binary formats
                if content.startswith(b'\x89PNG') or content.startswith(b'GIF8'):
                    return False, "Image file (PNG/GIF), not a document"
                
                # If we can't determine the format
                return False, f"Unknown file format (first 4 bytes: {content[:4].hex()})"
                
        except Exception as e:
            return False, f"Error reading file: {str(e)}"
    
    def convert_arxiv_url_to_pdf(self, url):
        """Convert arXiv abstract URL to PDF download URL"""
        if 'arxiv.org/abs/' in url:
            # Convert from abstract page to PDF
            pdf_url = url.replace('/abs/', '/pdf/')
            return pdf_url
        return url

    def download_paper(self, paper_info):
        """Download a single paper PDF with simplified error handling"""
        try:
            url = paper_info['oa_url']
            title = paper_info['title']
            professor = paper_info['professor']
            openalex_id = paper_info.get('openalex_id', '')
            paper_type = paper_info.get('paper_type', 'unknown')
            
            # Convert arXiv abstract URLs to PDF URLs
            original_url = url
            url = self.convert_arxiv_url_to_pdf(url)
            
            # Use OpenAlex ID as filename
            if not openalex_id:
                logging.error(f"SKIP: No OpenAlex ID for paper: {title[:50]}")
                self.stats['skipped'] += 1
                return False
            
            filename = f"{openalex_id}.pdf"
            
            # Save directly to the download directory (no subdirectories)
            save_path = self.download_dir / filename
            
            # Skip if file already exists and is valid
            if save_path.exists():
                is_valid, reason = self.verify_file_integrity(save_path)
                if is_valid:
                    logging.info(f"SKIP: {openalex_id} - File already exists ({paper_type})")
                    self.stats['skipped'] += 1
                    return True
                else:
                    save_path.unlink(missing_ok=True)  # Remove invalid file
            
            # Get domain-specific headers
            headers = self.get_domain_specific_headers(url)
            
            # Download file
            response, error_type = self.download_file(url, headers)
            
            if not response:
                logging.error(f"FAIL: {openalex_id} - {error_type} ({paper_type})")
                self.stats['failed'] += 1
                if error_type in self.stats['errors']:
                    self.stats['errors'][error_type] += 1
                return False
            
            # Get expected file size if available
            expected_size = response.headers.get('content-length')
            if expected_size:
                expected_size = int(expected_size)
            
            # Save file with integrity check
            temp_path = save_path.with_suffix(save_path.suffix + '.tmp')
            
            with open(temp_path, 'wb') as f:
                downloaded_size = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
            
            # Verify file integrity
            is_valid, reason = self.verify_file_integrity(temp_path, expected_size)
            if is_valid:
                # Move temp file to final location
                temp_path.rename(save_path)
                logging.info(f"SUCCESS: {openalex_id} ({paper_type})")
                self.stats['downloaded'] += 1
                return True
            else:
                # File is invalid, analyze the reason
                logging.error(f"FAIL: {openalex_id} - Integrity check failed: {reason} ({paper_type})")
                
                # Remove invalid file
                temp_path.unlink(missing_ok=True)
                
                self.stats['failed'] += 1
                self.stats['errors']['integrity_failures'] += 1
                return False
            
        except Exception as e:
            openalex_id = paper_info.get('openalex_id', 'Unknown')
            paper_type = paper_info.get('paper_type', 'unknown')
            logging.error(f"FAIL: {openalex_id} - Exception: {e} ({paper_type})")
            self.stats['failed'] += 1
            return False
    
    def process_all_files(self):
        """Process all JSON files"""
        json_files = self.get_json_files()
        self.stats['total_files'] = len(json_files)
        
        logging.info(f"Found {len(json_files)} JSON files")
        
        all_oa_papers = []
        
        # Extract all open access paper information
        for json_file in json_files:
            oa_papers = self.extract_paper_info(json_file)
            all_oa_papers.extend(oa_papers)
        
        # Count papers by type
        main_papers = [p for p in all_oa_papers if p.get('paper_type') == 'main_paper']
        cited_by_works = [p for p in all_oa_papers if p.get('paper_type') == 'cited_by_work']
        
        self.stats['oa_papers'] = len(main_papers)
        self.stats['oa_cited_by_works'] = len(cited_by_works)
        
        logging.info(f"Found {len(main_papers)} open access main papers")
        logging.info(f"Found {len(cited_by_works)} open access cited_by_works")
        logging.info(f"Total open access papers to download: {len(all_oa_papers)}")
        
        # Remove duplicates based on OpenAlex ID
        unique_papers = {}
        for paper in all_oa_papers:
            openalex_id = paper.get('openalex_id')
            if openalex_id and openalex_id not in unique_papers:
                unique_papers[openalex_id] = paper
        
        all_oa_papers = list(unique_papers.values())
        logging.info(f"After removing duplicates: {len(all_oa_papers)} unique papers")
        
        # Use multi-threading for downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_paper = {
                executor.submit(self.download_paper, paper): paper 
                for paper in all_oa_papers
            }
            
            # Process completed tasks
            for future in as_completed(future_to_paper):
                paper = future_to_paper[future]
                try:
                    future.result()
                except Exception as e:
                    openalex_id = paper.get('openalex_id', 'Unknown')
                    logging.error(f"FAIL: {openalex_id} - Task exception: {e}")
        
        # Print statistics
        self.print_stats()
    
    def print_stats(self):
        """Print statistics"""
        logging.info("=" * 50)
        logging.info("Download completed! Statistics:")
        logging.info(f"JSON files processed: {self.stats['total_files']}")
        logging.info(f"Open access main papers: {self.stats['oa_papers']}")
        logging.info(f"Open access cited_by_works: {self.stats['oa_cited_by_works']}")
        logging.info(f"Total unique papers to download: {self.stats['oa_papers'] + self.stats['oa_cited_by_works']}")
        logging.info(f"Successfully downloaded: {self.stats['downloaded']}")
        logging.info(f"Download failed: {self.stats['failed']}")
        logging.info(f"Skipped (already exists): {self.stats['skipped']}")
        logging.info(f"Download directory: {self.download_dir}")
        logging.info("=" * 50)
        logging.info("🎉 All papers processed! Download script finished.")

    def get_session_for_domain(self, url):
        """Get the appropriate session for a given domain"""
        domain = urlparse(url).netloc.lower()
        
        # Use cloudscraper for ACM and other Cloudflare-protected sites
        if self.use_cloudscraper and ('acm.org' in domain or 'cloudflare' in domain):
            if hasattr(self, 'cloudscraper_session') and self.cloudscraper_session:
                return self.cloudscraper_session
            else:
                return self.requests_session
        else:
            return self.requests_session
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'requests_session'):
                self.requests_session.close()
        except Exception as e:
            logging.warning(f"Error closing requests session: {e}")
        
        try:
            if hasattr(self, 'cloudscraper_session') and self.cloudscraper_session:
                self.cloudscraper_session.close()
        except Exception as e:
            logging.warning(f"Error closing cloudscraper session: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.cleanup()

def main():
    parser = argparse.ArgumentParser(description='Download open access paper PDFs')
    parser.add_argument('--output-dir', default='/mnt/ssd/yirui/openalexdata/data/output', help='JSON files directory')
    parser.add_argument('--download-dir', default='/mnt/data/taxonomy/computer_science', help='PDF download directory')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum concurrent downloads')
    parser.add_argument('--single-professor', help='Process only specified professor JSON file (filename)')
    parser.add_argument('--single-professor-download-dir', help='Custom download directory for single professor downloads (overrides --download-dir)')
    parser.add_argument('--use-cloudscraper', action='store_true', help='Use cloudscraper for downloads (requires cloudscraper library)')
    parser.add_argument('--no-cloudscraper', action='store_true', help='Disable cloudscraper and use only requests')
    
    args = parser.parse_args()
    
    # Determine download directory
    if args.single_professor and args.single_professor_download_dir:
        # Use custom directory for single professor
        download_dir = args.single_professor_download_dir
        logging.info(f"Using custom download directory for single professor: {download_dir}")
    else:
        # Use default or specified download directory
        download_dir = args.download_dir
    
    # Determine whether to use cloudscraper
    use_cloudscraper = True
    if args.no_cloudscraper:
        use_cloudscraper = False
        logging.info("Cloudscraper disabled by user request")
    elif args.use_cloudscraper:
        use_cloudscraper = True
        logging.info("Cloudscraper enabled by user request")
    else:
        # Default behavior: use cloudscraper if available
        use_cloudscraper = True
        logging.info("Using default cloudscraper behavior")
    
    downloader = None
    try:
        downloader = PaperDownloader(
            output_dir=args.output_dir,
            download_dir=download_dir,
            max_workers=args.max_workers,
            use_cloudscraper=use_cloudscraper
        )
        
        if args.single_professor:
            # Process only specified professor file
            json_files = downloader.get_json_files()
            target_file = None
            
            for json_file in json_files:
                if args.single_professor in json_file.name:
                    target_file = json_file
                    break
            
            if target_file:
                logging.info(f"Processing single professor file: {target_file.name}")
                oa_papers = downloader.extract_paper_info(target_file)
                downloader.stats['oa_papers'] = len(oa_papers)
                downloader.stats['total_files'] = 1
                
                with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                    future_to_paper = {
                        executor.submit(downloader.download_paper, paper): paper 
                        for paper in oa_papers
                    }
                    
                    for future in as_completed(future_to_paper):
                        paper = future_to_paper[future]
                        try:
                            future.result()
                        except Exception as e:
                            openalex_id = paper.get('openalex_id', 'Unknown')
                            logging.error(f"FAIL: {openalex_id} - Task exception: {e}")
                
                downloader.print_stats()
            else:
                logging.error(f"File containing '{args.single_professor}' not found")
        else:
            # Process all files
            downloader.process_all_files()
    
    except KeyboardInterrupt:
        logging.info("Download interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Ensure cleanup happens
        if downloader:
            downloader.cleanup()

if __name__ == "__main__":
    main() 