#!/usr/bin/env python3
"""
Script to fetch all papers for professors using OpenAlex API
Handles pagination and saves results in JSON files
"""

import json
import requests
import time
import os
from typing import Dict, List, Any
from urllib.parse import urlencode

def load_professor_lists() -> Dict[str, Dict[str, str]]:
    """
    Load professor lists from JSON files
    Returns: Dictionary with department names as keys and professor lists as values
    """
    professor_lists = {}
    
    # Load biology professors
    # with open('data/bio_prof_list.json', 'r', encoding='utf-8') as f:
    #     professor_lists['biology'] = json.load(f)
    
    # Load computer science professors
    with open('data/cs_prof_list.json', 'r', encoding='utf-8') as f:
        professor_lists['computer_science'] = json.load(f)
    
    return professor_lists

def fetch_papers_for_professor(author_id: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch all papers for a specific professor using OpenAlex API
    Handles pagination and retries
    
    Args:
        author_id: OpenAlex author ID
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of paper dictionaries
    """
    base_url = "https://api.openalex.org/works"
    all_papers = []
    page = 1
    per_page = 200  # Maximum allowed by OpenAlex API
    
    while True:
        # Prepare query parameters
        params = {
            'filter': f'author.id:{author_id}',
            'per_page': per_page,
            'page': page
        }
        
        # Build URL with parameters
        url = f"{base_url}?{urlencode(params)}"
        
        # Make request with retry logic
        for attempt in range(max_retries):
            try:
                print(f"  Fetching page {page} (attempt {attempt + 1})...")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                break
                
            except requests.exceptions.RequestException as e:
                print(f"    Error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    print(f"    Failed to fetch page {page} after {max_retries} attempts")
                    return all_papers
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Extract papers from response
        papers = data.get('results', [])
        if not papers:
            break
            
        all_papers.extend(papers)
        
        # Check if we've reached the end
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        
        print(f"    Retrieved {len(papers)} papers (total so far: {len(all_papers)}/{total_count})")
        
        # If we've got all papers, break
        if len(all_papers) >= total_count:
            break
            
        page += 1
        
        # Rate limiting - be respectful to the API
        time.sleep(1)
    
    return all_papers

def fetch_cited_works(work_id: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch works that cite the given work (cited_by relationship)
    
    Args:
        work_id: OpenAlex work ID (e.g., W2153066044)
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of works that cite this work
    """
    # Extract work ID from full URL if needed
    if work_id.startswith('https://openalex.org/'):
        work_id = work_id.split('/')[-1]
    
    base_url = "https://api.openalex.org/works"
    all_works = []
    page = 1
    per_page = 200
    
    while True:
        params = {
            'filter': f'cites:{work_id}',
            'per_page': per_page,
            'page': page
        }
        
        url = f"{base_url}?{urlencode(params)}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"        Error fetching cited works page {page} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return all_works
                time.sleep(1)
        
        works = data.get('results', [])
        if not works:
            break
            
        all_works.extend(works)
        
        # Check if we've reached the end
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        
        if total_count > 0:
            print(f"        Cited works: page {page}, got {len(works)} works ({len(all_works)}/{total_count} total)")
        
        if len(all_works) >= total_count:
            break
            
        page += 1
        time.sleep(0.5)  # Rate limiting
    
    return all_works

def fetch_citing_works(work_id: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch works cited by the given work (cited_by relationship)
    
    Args:
        work_id: OpenAlex work ID (e.g., W2153066044)
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of works cited by this work
    """
    # Extract work ID from full URL if needed
    if work_id.startswith('https://openalex.org/'):
        work_id = work_id.split('/')[-1]
    
    base_url = "https://api.openalex.org/works"
    all_works = []
    page = 1
    per_page = 200
    
    while True:
        params = {
            'filter': f'cited_by:{work_id}',
            'per_page': per_page,
            'page': page
        }
        
        url = f"{base_url}?{urlencode(params)}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"        Error fetching citing works page {page} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return all_works
                time.sleep(1)
        
        works = data.get('results', [])
        if not works:
            break
            
        all_works.extend(works)
        
        # Check if we've reached the end
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        
        if total_count > 0:
            print(f"        Citing works: page {page}, got {len(works)} works ({len(all_works)}/{total_count} total)")
        
        if len(all_works) >= total_count:
            break
            
        page += 1
        time.sleep(0.5)  # Rate limiting
    
    return all_works

def convert_inverted_index_to_abstract(inverted_index: Dict[str, List[int]]) -> str:
    """
    Convert abstract inverted index to readable text
    
    Args:
        inverted_index: Dictionary where keys are words and values are lists of positions
        
    Returns:
        String containing the readable abstract
    """
    if not inverted_index:
        return ""
    
    # Create a list to hold words in their correct positions
    word_positions = []
    
    # Extract all word-position pairs
    for word, positions in inverted_index.items():
        for position in positions:
            word_positions.append((position, word))
    
    # Sort by position
    word_positions.sort(key=lambda x: x[0])
    
    # Extract just the words in order
    words = [word for position, word in word_positions]
    
    # Join words with spaces
    return ' '.join(words)

def filter_paper_fields(papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter papers to keep only the specified fields
    
    Args:
        papers: List of paper dictionaries from OpenAlex API
        
    Returns:
        List of filtered paper dictionaries with only specified fields
    """
    filtered_papers = []
    total_papers = len(papers)
    
    for idx, paper in enumerate(papers, 1):
        print(f"    Processing paper {idx}/{total_papers}: {paper.get('title', 'Untitled')[:50]}...")
        
        filtered_paper = {}
        
        # Keep only the specified fields
        if 'id' in paper:
            filtered_paper['id'] = paper['id']
        if 'doi' in paper:
            filtered_paper['doi'] = paper['doi']
        if 'title' in paper:
            filtered_paper['title'] = paper['title']
        if 'publication_date' in paper:
            filtered_paper['publication_date'] = paper['publication_date']
        if 'open_access' in paper:
            filtered_paper['open_access'] = paper['open_access']
        if 'primary_topic' in paper:
            filtered_paper['primary_topic'] = paper['primary_topic']
        if 'abstract_inverted_index' in paper:
            filtered_paper['abstract'] = convert_inverted_index_to_abstract(paper['abstract_inverted_index'])
        
        # Get citation information
        if 'id' in paper:
            work_id = paper['id']
            print(f"      [{idx}/{total_papers}] Fetching citations for: {work_id}")
            
            # Get works that cite this paper (cited_by)
            print(f"      [{idx}/{total_papers}] Getting citing works...")
            citing_works = fetch_citing_works(work_id)
            filtered_paper['cited_by_works'] = filter_citation_fields(citing_works)
            print(f"      [{idx}/{total_papers}] Found {len(citing_works)} citing works")
            
            # Get works cited by this paper (cites)
            print(f"      [{idx}/{total_papers}] Getting cited works...")
            cited_works = fetch_cited_works(work_id)
            filtered_paper['cited_works'] = filter_citation_fields(cited_works)
            print(f"      [{idx}/{total_papers}] Found {len(cited_works)} cited works")
            
            filtered_paper['cited_by_count'] = len(citing_works)
            filtered_paper['cited_count'] = len(cited_works)
            
            print(f"      [{idx}/{total_papers}] Citation processing complete (cited_by: {len(citing_works)}, cites: {len(cited_works)})")
        
        filtered_papers.append(filtered_paper)
    
    return filtered_papers

def filter_citation_fields(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter citation works to keep only specified fields
    
    Args:
        works: List of work dictionaries from OpenAlex API
        
    Returns:
        List of filtered work dictionaries
    """
    filtered_works = []
    
    for work in works:
        filtered_work = {}
        
        if 'id' in work:
            filtered_work['id'] = work['id']
        if 'doi' in work:
            filtered_work['doi'] = work['doi']
        if 'title' in work:
            filtered_work['title'] = work['title']
        if 'publication_date' in work:
            filtered_work['publication_date'] = work['publication_date']
        if 'open_access' in work:
            filtered_work['open_access'] = work['open_access']
        if 'primary_topic' in work:
            filtered_work['primary_topic'] = work['primary_topic']
        if 'abstract_inverted_index' in work:
            filtered_work['abstract'] = convert_inverted_index_to_abstract(work['abstract_inverted_index'])
        
        filtered_works.append(filtered_work)
    
    return filtered_works

def save_professor_papers(professor_name: str, author_id: str, papers: List[Dict[str, Any]], department: str):
    """
    Save professor's papers to a JSON file
    
    Args:
        professor_name: Name of the professor
        author_id: OpenAlex author ID
        papers: List of paper dictionaries
        department: Department name for organizing files
    """
    # Create output directory if it doesn't exist
    output_dir = f"data/output/{department}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename with professor name and ID
    # Replace special characters that might cause issues in filenames
    safe_name = professor_name.replace('/', '_').replace('\\', '_').replace(':', '_')
    filename = f"{safe_name}_{author_id}_detail.json"
    filepath = os.path.join(output_dir, filename)
    
    # Filter papers to keep only specified fields
    filtered_papers = filter_paper_fields(papers)
    
    # Prepare data structure
    output_data = {
        "professor_info": {
            "name": professor_name,
            "author_id": author_id,
            "department": department,
            "total_papers": len(papers),
            "fetch_date": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "papers": filtered_papers
    }
    
    # Save to file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"    Saved {len(papers)} papers to {filepath} (filtered to {len(filtered_papers)} fields per paper)")

def main():
    """
    Main function to process all professors
    """
    print("Starting to fetch papers for all professors...")
    
    # Load professor lists
    professor_lists = load_professor_lists()
    
    total_professors = sum(len(profs) for profs in professor_lists.values())
    processed_count = 0
    
    # Process each department
    for department, professors in professor_lists.items():
        print(f"\nProcessing {department} department ({len(professors)} professors)...")
        
        # Process each professor in the department
        for professor_name, author_id in professors.items():
            processed_count += 1
            print(f"\n[{processed_count}/{total_professors}] Processing {professor_name} ({author_id})...")
            
            try:
                # Fetch all papers for this professor
                print(f"    Step 1/2: Fetching papers for {professor_name}...")
                papers = fetch_papers_for_professor(author_id)
                print(f"    Found {len(papers)} papers for {professor_name}")
                
                # Save papers to file
                print(f"    Step 2/2: Processing citations and saving data...")
                save_professor_papers(professor_name, author_id, papers, department)
                
                print(f"    ✓ Successfully processed {professor_name}: {len(papers)} papers")
                
            except Exception as e:
                print(f"    ✗ Error processing {professor_name}: {e}")
                continue
            
            # Rate limiting between professors
            time.sleep(2)
    
    print(f"\nCompleted! Processed {processed_count} professors.")
    print("Check the 'output' directory for results.")

if __name__ == "__main__":
    main()