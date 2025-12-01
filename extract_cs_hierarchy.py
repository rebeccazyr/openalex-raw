#!/usr/bin/env python3
"""
Script to extract taxonomy hierarchy for any given node and generate:
1. Entity list with OpenAlex IDs and classification labels
2. Parent-child relationship list

Based on the field.txt structure:
[0] topic_id, [1] topic_name, [2] subfield_id, [3] subfield_name, 
[4] field_id, [5] field_name, [6] domain_id, [7] domain_name, 
[8] keywords, [9] summary, [10] link

Usage: python extract_hierarchy.py <node_name>
Example: python extract_hierarchy.py "Computer Science"
"""

import json
import sys
import os
from collections import defaultdict, OrderedDict

def extract_hierarchy(target_node):
    """Extract hierarchy starting from the target node"""
    entities = []
    relationships = []
    
    # Track unique entities to avoid duplicates
    seen_entities = set()
    
    # Determine what level the target node is at
    target_level = None
    target_match_field = None
    
    print(f"Searching for node: '{target_node}'...")
    
    # First pass: determine the target node level and matching field
    with open('data/field.txt', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                row = line.strip().split('\t')
                if len(row) < 11:
                    continue
                
                domain_name = row[7]
                field_name = row[5]
                subfield_name = row[3]
                
                if domain_name == target_node:
                    target_level = 'domain'
                    target_match_field = 'domain_name'
                    break
                elif field_name == target_node:
                    target_level = 'field'
                    target_match_field = 'field_name'
                    break
                elif subfield_name == target_node:
                    target_level = 'subfield'
                    target_match_field = 'subfield_name'
                    break
            except Exception as e:
                continue
    
    if target_level is None:
        print(f"Error: Node '{target_node}' not found in the data!")
        return [], []
    
    print(f"Found '{target_node}' at level: {target_level}")
    
    # Second pass: extract hierarchy
    with open('data/field.txt', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                row = line.strip().split('\t')
                if len(row) < 11:
                    continue
                
                # Extract data from row
                topic_id = row[0]
                topic_name = row[1]
                subfield_id = row[2]
                subfield_name = row[3]
                field_id = row[4]
                field_name = row[5]
                domain_id = row[6]
                domain_name = row[7]
                keywords = row[8]
                summary = row[9]
                link = row[10]
                
                # Filter based on target node level
                should_process = False
                if target_level == 'domain' and domain_name == target_node:
                    should_process = True
                elif target_level == 'field' and field_name == target_node:
                    should_process = True
                elif target_level == 'subfield' and subfield_name == target_node:
                    should_process = True
                
                if not should_process:
                    continue
                
                print(f"Processing line {line_num}: {topic_name}")
                
                # Create OpenAlex ID format
                domain_openalex_id = f"https://openalex.org/domains/{domain_id}"
                field_openalex_id = f"https://openalex.org/fields/{field_id}"
                subfield_openalex_id = f"https://openalex.org/subfields/{subfield_id}"
                topic_openalex_id = f"https://openalex.org/T{topic_id}"
                
                # Add entities based on target level
                if target_level == 'domain':
                    # Add domain entity (only once)
                    if domain_openalex_id not in seen_entities:
                        entities.append({
                            'id': domain_openalex_id,
                            'name': domain_name,
                            'original_id': domain_id,
                            'type': 'domain'
                        })
                        seen_entities.add(domain_openalex_id)
                
                # Add field entity (if target is domain or field)
                if target_level in ['domain', 'field']:
                    if field_openalex_id not in seen_entities:
                        entities.append({
                            'id': field_openalex_id,
                            'name': field_name,
                            'original_id': field_id,
                            'type': 'field'
                        })
                        seen_entities.add(field_openalex_id)
                        
                        # Add domain -> field relationship if domain exists
                        if target_level == 'domain':
                            relationships.append({
                                'parent_id': domain_openalex_id,
                                'parent_name': domain_name,
                                'child_id': field_openalex_id,
                                'child_name': field_name,
                                'relationship_type': 'domain_to_field'
                            })
                
                # Add subfield entity (always if we're processing this row)
                if subfield_openalex_id not in seen_entities:
                    entities.append({
                        'id': subfield_openalex_id,
                        'name': subfield_name,
                        'original_id': subfield_id,
                        'type': 'subfield'
                    })
                    seen_entities.add(subfield_openalex_id)
                    
                    # Add parent -> subfield relationship
                    if target_level in ['domain', 'field']:
                        relationships.append({
                            'parent_id': field_openalex_id,
                            'parent_name': field_name,
                            'child_id': subfield_openalex_id,
                            'child_name': subfield_name,
                            'relationship_type': 'field_to_subfield'
                        })
                
                # Add topic entity (always)
                entities.append({
                    'id': topic_openalex_id,
                    'name': topic_name,
                    'original_id': topic_id,
                    'type': 'topic',
                    'keywords': keywords,
                    'summary': summary,
                    'link': link
                })
                
                # Add subfield -> topic relationship
                relationships.append({
                    'parent_id': subfield_openalex_id,
                    'parent_name': subfield_name,
                    'child_id': topic_openalex_id,
                    'child_name': topic_name,
                    'relationship_type': 'subfield_to_topic'
                })
                
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    return entities, relationships

def save_to_files(entities, relationships, node_name):
    """Save entities and relationships to JSON files in data folder"""
    
    # Create data folder if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Clean node name for filename (replace spaces and special characters)
    clean_name = node_name.lower().replace(' ', '_').replace('&', 'and').replace(',', '').replace('(', '').replace(')', '')
    
    entities_filename = f"data/{clean_name}_entities.json"
    relationships_filename = f"data/{clean_name}_relationships.json"
    
    # Save entities to JSON
    with open(entities_filename, 'w', encoding='utf-8') as f:
        json.dump(entities, f, indent=2, ensure_ascii=False)
    
    # Save relationships to JSON
    with open(relationships_filename, 'w', encoding='utf-8') as f:
        json.dump(relationships, f, indent=2, ensure_ascii=False)
    
    return entities_filename, relationships_filename

def generate_summary_stats(entities, relationships):
    """Generate summary statistics"""
    
    # Count entities by type
    entity_counts = defaultdict(int)
    for entity in entities:
        entity_counts[entity['type']] += 1
    
    # Count relationships by type
    rel_counts = defaultdict(int)
    for rel in relationships:
        rel_counts[rel['relationship_type']] += 1
    
    print("\n=== SUMMARY STATISTICS ===")
    print("\nEntity Counts:")
    for entity_type, count in entity_counts.items():
        print(f"  {entity_type}: {count}")
    print(f"  Total entities: {len(entities)}")
    
    print("\nRelationship Counts:")
    for rel_type, count in rel_counts.items():
        print(f"  {rel_type}: {count}")
    print(f"  Total relationships: {len(relationships)}")
    
    # Show some sample entities for each type
    print("\n=== SAMPLE ENTITIES ===")
    by_type = defaultdict(list)
    for entity in entities:
        by_type[entity['type']].append(entity)
    
    for entity_type in ['domain', 'field', 'subfield', 'topic']:
        if entity_type in by_type:
            print(f"\nSample {entity_type} entities:")
            for i, entity in enumerate(by_type[entity_type][:3]):
                print(f"  {i+1}. {entity['name']} ({entity['id']})")

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python extract_hierarchy.py <node_name>")
        print("Example: python extract_hierarchy.py 'Computer Science'")
        print("Example: python extract_hierarchy.py 'Medicine'")
        print("Example: python extract_hierarchy.py 'Artificial Intelligence'")
        sys.exit(1)
    
    target_node = sys.argv[1]
    
    print(f"Extracting taxonomy hierarchy for '{target_node}' from field.txt...")
    entities, relationships = extract_hierarchy(target_node)
    
    if not entities:
        print(f"No data found for node '{target_node}'")
        sys.exit(1)
    
    print(f"\nExtracted {len(entities)} entities and {len(relationships)} relationships")
    
    print("Saving to files...")
    entities_file, relationships_file = save_to_files(entities, relationships, target_node)
    
    generate_summary_stats(entities, relationships)
    
    print("\n=== FILES CREATED ===")
    print(f"1. {entities_file} - All entities with OpenAlex IDs and metadata")
    print(f"2. {relationships_file} - Parent-child relationships")

if __name__ == "__main__":
    main()
