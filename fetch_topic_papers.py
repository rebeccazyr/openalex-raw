#!/usr/bin/env python3
"""
Script to fetch papers for topics from computer_science_entities.json using OpenAlex API
Handles pagination and saves results in JSON files
"""

import json
import requests
import time
import os
from typing import Dict, List, Any
from urllib.parse import urlencode

def load_topics() -> List[Dict[str, Any]]:
    """
    Load topics from computer_science_entities.json file
    Returns: List of topic dictionaries with type "topic"
    """
    topics = []
    
    with open('data/computer_science_entities.json', 'r', encoding='utf-8') as f:
        entities = json.load(f)
    
    # Filter for topics only
    for entity in entities:
        if entity.get('type') == 'topic':
            topics.append(entity)
    
    return topics

def fetch_papers_for_topic(topic_id: str, max_retries: int = 3, max_papers: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch papers for a specific topic using OpenAlex API
    Uses cursor pagination to handle large result sets
    
    Args:
        topic_id: OpenAlex topic ID (e.g., T10007)
        max_retries: Maximum number of retry attempts
        max_papers: Maximum number of papers to fetch (default 200)
        
    Returns:
        List of paper dictionaries
    """
    # Extract topic ID from full URL if needed
    if topic_id.startswith('https://openalex.org/'):
        topic_id = topic_id.split('/')[-1]
    
    base_url = "https://api.openalex.org/works"
    all_papers = []
    cursor = None
    per_page = min(200, max_papers)  # Maximum allowed by OpenAlex API
    
    while len(all_papers) < max_papers:
        # Prepare query parameters
        params = {
            'filter': f'topics.id:{topic_id}',
            'per_page': per_page
        }
        
        # Add cursor for pagination if we have one
        if cursor:
            params['cursor'] = cursor
        
        # Build URL with parameters
        url = f"{base_url}?{urlencode(params)}"
        
        # Make request with retry logic
        for attempt in range(max_retries):
            try:
                print(f"  Fetching papers (attempt {attempt + 1})...")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                break
                
            except requests.exceptions.RequestException as e:
                print(f"    Error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    print(f"    Failed to fetch papers after {max_retries} attempts")
                    return all_papers
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Extract papers from response
        papers = data.get('results', [])
        if not papers:
            break
        
        # Limit to max_papers
        remaining = max_papers - len(all_papers)
        papers_to_add = papers[:remaining]
        all_papers.extend(papers_to_add)
        
        # Check if we've reached the end or max_papers
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        next_cursor = meta.get('next_cursor')
        
        print(f"    Retrieved {len(papers_to_add)} papers (total so far: {len(all_papers)}/{min(max_papers, total_count)})")
        
        # If no next cursor or we've reached max_papers, break
        if not next_cursor or len(all_papers) >= max_papers:
            break
            
        cursor = next_cursor
        
        # Rate limiting - be respectful to the API
        time.sleep(1)
    
    return all_papers

def fetch_cited_works(work_id: str, max_retries: int = 3, max_works: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch works that cite the given work (cited_by relationship)
    Uses cursor pagination to handle large result sets
    
    Args:
        work_id: OpenAlex work ID (e.g., W2153066044)
        max_retries: Maximum number of retry attempts
        max_works: Maximum number of works to fetch (default 1000 to limit API calls)
        
    Returns:
        List of works that cite this work
    """
    # Extract work ID from full URL if needed
    if work_id.startswith('https://openalex.org/'):
        work_id = work_id.split('/')[-1]
    
    base_url = "https://api.openalex.org/works"
    all_works = []
    cursor = None
    per_page = 200
    
    while len(all_works) < max_works:
        params = {
            'filter': f'cites:{work_id}',
            'per_page': per_page
        }
        
        # Add cursor for pagination if we have one
        if cursor:
            params['cursor'] = cursor
        
        url = f"{base_url}?{urlencode(params)}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"        Error fetching cited works (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return all_works
                time.sleep(1)
        
        works = data.get('results', [])
        if not works:
            break
        
        # Limit to max_works
        remaining = max_works - len(all_works)
        works_to_add = works[:remaining]
        all_works.extend(works_to_add)
        
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        next_cursor = meta.get('next_cursor')
        
        if total_count > 0:
            print(f"        Cited works: got {len(works_to_add)} works ({len(all_works)}/{min(max_works, total_count)} total)")
        
        # If no next cursor or we've reached max_works, stop
        if not next_cursor or len(all_works) >= max_works:
            break
            
        cursor = next_cursor
        time.sleep(0.5)  # Rate limiting
    
    return all_works

def fetch_citing_works(work_id: str, max_retries: int = 3, max_works: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch works cited by the given work (cited_by relationship)
    Uses cursor pagination to handle large result sets
    
    Args:
        work_id: OpenAlex work ID (e.g., W2153066044)
        max_retries: Maximum number of retry attempts
        max_works: Maximum number of works to fetch (default 1000 to limit API calls)
        
    Returns:
        List of works cited by this work
    """
    # Extract work ID from full URL if needed
    if work_id.startswith('https://openalex.org/'):
        work_id = work_id.split('/')[-1]
    
    base_url = "https://api.openalex.org/works"
    all_works = []
    cursor = None
    per_page = 200
    
    while len(all_works) < max_works:
        params = {
            'filter': f'cited_by:{work_id}',
            'per_page': per_page
        }
        
        # Add cursor for pagination if we have one
        if cursor:
            params['cursor'] = cursor
        
        url = f"{base_url}?{urlencode(params)}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"        Error fetching citing works (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return all_works
                time.sleep(1)
        
        works = data.get('results', [])
        if not works:
            break
        
        # Limit to max_works
        remaining = max_works - len(all_works)
        works_to_add = works[:remaining]
        all_works.extend(works_to_add)
        
        meta = data.get('meta', {})
        total_count = meta.get('count', 0)
        next_cursor = meta.get('next_cursor')
        
        if total_count > 0:
            print(f"        Citing works: got {len(works_to_add)} works ({len(all_works)}/{min(max_works, total_count)} total)")
        
        # If no next cursor or we've reached max_works, stop
        if not next_cursor or len(all_works) >= max_works:
            break
            
        cursor = next_cursor
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
            
            # Get works that cite this paper (cited_by) - limited to 1000 to avoid excessive API calls
            print(f"      [{idx}/{total_papers}] Getting citing works...")
            citing_works = fetch_citing_works(work_id, max_works=1000)
            filtered_paper['cited_by_works'] = filter_citation_fields(citing_works)
            print(f"      [{idx}/{total_papers}] Found {len(citing_works)} citing works")
            
            # Get works cited by this paper (cites) - limited to 1000 to avoid excessive API calls
            print(f"      [{idx}/{total_papers}] Getting cited works...")
            cited_works = fetch_cited_works(work_id, max_works=1000)
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

def save_topic_papers(topic_name: str, topic_id: str, papers: List[Dict[str, Any]]):
    """
    Save topic papers to a JSON file in the domain-level directory
    
    Args:
        topic_name: Name of the topic
        topic_id: OpenAlex topic ID
        papers: List of paper dictionaries
    """
    # Create output directory if it doesn't exist
    output_dir = "data/domain-level"
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract topic ID from URL if needed
    if topic_id.startswith('https://openalex.org/'):
        topic_id = topic_id.split('/')[-1]
    
    # Create filename with topic name and ID
    # Replace special characters that might cause issues in filenames
    safe_name = topic_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace(' ', '_')
    filename = f"{safe_name}_{topic_id}_papers.json"
    filepath = os.path.join(output_dir, filename)
    
    # Filter papers to keep only specified fields
    filtered_papers = filter_paper_fields(papers)
    
    # Prepare data structure
    output_data = {
        "topic_info": {
            "name": topic_name,
            "topic_id": topic_id,
            "total_papers": len(papers),
            "fetch_date": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "papers": filtered_papers
    }
    
    # Save to file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"    Saved {len(papers)} papers to {filepath} (filtered to {len(filtered_papers)} papers with full details)")

def main():
    """
    Main function to process all topics
    """
    print("Starting to fetch papers for all topics...")
    
    # Load topics from computer_science_entities.json
    topics = load_topics()
    
    total_topics = len(topics)
    processed_count = 0
    
    print(f"Found {total_topics} topics to process...")
    
    # Process each topic
    for topic in topics:
        processed_count += 1
        topic_name = topic.get('name', 'Unknown')
        topic_id = topic.get('id', '')
        
        print(f"\n[{processed_count}/{total_topics}] Processing topic: {topic_name} ({topic_id})...")
        
        try:
            # Fetch papers for this topic (limited to 200 papers)
            print(f"    Step 1/2: Fetching papers for topic {topic_name}...")
            papers = fetch_papers_for_topic(topic_id, max_papers=200)
            print(f"    Found {len(papers)} papers for {topic_name}")
            
            if not papers:
                print(f"    No papers found for {topic_name}, skipping...")
                continue
            
            # Save papers to file
            print(f"    Step 2/2: Processing citations and saving data...")
            save_topic_papers(topic_name, topic_id, papers)
            
            print(f"    ✓ Successfully processed {topic_name}: {len(papers)} papers")
            
        except Exception as e:
            print(f"    ✗ Error processing {topic_name}: {e}")
            continue
        
        # Rate limiting between topics
        time.sleep(2)
    
    print(f"\nCompleted! Processed {processed_count} topics.")
    print("Check the 'data/domain-level' directory for results.")

if __name__ == "__main__":
    main()
