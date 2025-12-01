#!/usr/bin/env python3
"""
脚本功能：分析教授的研究主题，生成包含primary_topic和相关文章的输出
输入：教授详细信息的JSON文件
输出：教授的研究主题分析结果，包含实体和关系的taxonomy
"""

import json
import sys
import os
from collections import defaultdict, Counter
from pathlib import Path

def load_cs_relationships(relationships_file):
    """加载计算机科学领域的层次关系"""
    try:
        with open(relationships_file, 'r', encoding='utf-8') as f:
            relationships = json.load(f)
        return relationships
    except Exception as e:
        print(f"Error loading relationships file: {e}")
        return []

def analyze_professor_topics(professor_file, relationships_file=None):
    """分析教授的研究主题"""
    
    # 加载教授数据
    try:
        with open(professor_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading professor file: {e}")
        return None
    
    # 获取教授基本信息
    prof_info = data.get('professor_info', {})
    papers = data.get('papers', [])
    
    # 统计primary_topic和相关文章
    topic_papers = defaultdict(list)
    topic_stats = defaultdict(int)
    
    # 收集所有相关的实体信息
    fields = set()
    subfields = set()
    topics = set()
    domains = set()
    
    for paper in papers:
        # 确保 paper 本身不是 None 并且是字典
        if paper is None or not isinstance(paper, dict):
            continue
            
        primary_topic = paper.get('primary_topic')
        # 检查 primary_topic 是否为 None 或空字典
        if primary_topic is None or not isinstance(primary_topic, dict) or not primary_topic:
            continue
            
        topic_id = primary_topic.get('id')
        topic_name = primary_topic.get('display_name')
        
        if topic_id and topic_name:
            # 添加文章到对应主题
            paper_info = {
                'id': paper.get('id'),
                'title': paper.get('title'),
                'publication_date': paper.get('publication_date'),
                'doi': paper.get('doi'),
                'cited_by_count': paper.get('cited_by_count', 0),
                'primary_topic_score': primary_topic.get('score', 0)
            }
            
            topic_papers[topic_id].append(paper_info)
            topic_stats[topic_id] += 1
            
            # 收集层次信息
            topics.add((topic_id, topic_name))
            
            subfield = primary_topic.get('subfield')
            if subfield and isinstance(subfield, dict):
                subfield_id = subfield.get('id')
                subfield_name = subfield.get('display_name')
                if subfield_id and subfield_name:
                    subfields.add((subfield_id, subfield_name))
            
            field = primary_topic.get('field')
            if field and isinstance(field, dict):
                field_id = field.get('id')
                field_name = field.get('display_name')
                if field_id and field_name:
                    fields.add((field_id, field_name))
            
            domain = primary_topic.get('domain')
            if domain and isinstance(domain, dict):
                domain_id = domain.get('id')
                domain_name = domain.get('display_name')
                if domain_id and domain_name:
                    domains.add((domain_id, domain_name))
    
    # 生成输出结果
    result = {
        'professor_info': prof_info,
        'topic_analysis': {
            'total_topics': len(topic_papers),
            'total_papers_analyzed': len(papers),
            'topics_with_papers': {}
        }
    }
    
    # 为每个主题添加详细信息
    for topic_id, paper_list in topic_papers.items():
        # 查找包含该主题的文章获取主题详细信息
        topic_detail = None
        
        for paper in papers:
            if (paper and isinstance(paper, dict) and 
                paper.get('primary_topic') and 
                isinstance(paper.get('primary_topic'), dict) and
                paper.get('primary_topic', {}).get('id') == topic_id):
                topic_detail = paper['primary_topic']
                break
        
        if topic_detail:
            result['topic_analysis']['topics_with_papers'][topic_id] = {
                'topic_info': {
                    'id': topic_id,
                    'display_name': topic_detail.get('display_name'),
                    'subfield': topic_detail.get('subfield'),
                    'field': topic_detail.get('field'),
                    'domain': topic_detail.get('domain')
                },
                'paper_count': len(paper_list),
                'papers': paper_list,
                'avg_citations': sum(p.get('cited_by_count', 0) for p in paper_list) / len(paper_list) if paper_list else 0
            }
    
    # 生成实体和关系的taxonomy
    entities = []
    relations = []
    
    # 添加教授实体
    prof_entity = {
        'id': f"professor_{prof_info.get('author_id', '')}",
        'type': 'professor',
        'name': prof_info.get('name'),
        'properties': {
            'author_id': prof_info.get('author_id'),
            'department': prof_info.get('department'),
            'total_papers': prof_info.get('total_papers', 0),
            'total_topics': len(topic_papers)
        }
    }
    entities.append(prof_entity)
    
    # 添加领域实体
    for domain_id, domain_name in domains:
        if domain_id and domain_name:
            paper_count = 0
            for paper in papers:
                if (paper and isinstance(paper, dict) and 
                    paper.get('primary_topic') and 
                    isinstance(paper.get('primary_topic'), dict) and
                    paper.get('primary_topic', {}).get('domain') and
                    isinstance(paper.get('primary_topic', {}).get('domain'), dict) and
                    paper.get('primary_topic', {}).get('domain', {}).get('id') == domain_id):
                    paper_count += 1
            
            entities.append({
                'id': domain_id,
                'type': 'domain',
                'name': domain_name,
                'properties': {
                    'paper_count': paper_count
                }
            })
    
    # 添加字段实体
    for field_id, field_name in fields:
        if field_id and field_name:
            paper_count = 0
            for paper in papers:
                if (paper and isinstance(paper, dict) and 
                    paper.get('primary_topic') and 
                    isinstance(paper.get('primary_topic'), dict) and
                    paper.get('primary_topic', {}).get('field') and
                    isinstance(paper.get('primary_topic', {}).get('field'), dict) and
                    paper.get('primary_topic', {}).get('field', {}).get('id') == field_id):
                    paper_count += 1
            
            entities.append({
                'id': field_id,
                'type': 'field',
                'name': field_name,
                'properties': {
                    'paper_count': paper_count
                }
            })
    
    # 添加子字段实体
    for subfield_id, subfield_name in subfields:
        if subfield_id and subfield_name:
            paper_count = 0
            for paper in papers:
                if (paper and isinstance(paper, dict) and 
                    paper.get('primary_topic') and 
                    isinstance(paper.get('primary_topic'), dict) and
                    paper.get('primary_topic', {}).get('subfield') and
                    isinstance(paper.get('primary_topic', {}).get('subfield'), dict) and
                    paper.get('primary_topic', {}).get('subfield', {}).get('id') == subfield_id):
                    paper_count += 1
            
            entities.append({
                'id': subfield_id,
                'type': 'subfield',
                'name': subfield_name,
                'properties': {
                    'paper_count': paper_count
                }
            })
    
    # 添加主题实体
    for topic_id, topic_name in topics:
        paper_count = topic_stats.get(topic_id, 0)
        entities.append({
            'id': topic_id,
            'type': 'topic',
            'name': topic_name,
            'properties': {
                'paper_count': paper_count,
                'papers': [p['id'] for p in topic_papers.get(topic_id, [])]
            }
        })
    
    # 生成关系
    # 教授与主题的关系
    for topic_id in topic_papers:
        relations.append({
            'source': prof_entity['id'],
            'target': topic_id,
            'type': 'works_on',
            'properties': {
                'paper_count': topic_stats[topic_id],
                'papers': [p['id'] for p in topic_papers[topic_id]]
            }
        })
    
    # 如果有关系文件，添加层次关系
    if relationships_file and os.path.exists(relationships_file):
        cs_relationships = load_cs_relationships(relationships_file)
        
        # 过滤出与教授相关的关系
        relevant_entity_ids = set([e['id'] for e in entities])
        
        for rel in cs_relationships:
            parent_id = rel.get('parent_id')
            child_id = rel.get('child_id')
            
            if parent_id in relevant_entity_ids and child_id in relevant_entity_ids:
                relations.append({
                    'source': parent_id,
                    'target': child_id,
                    'type': rel.get('relationship_type', 'hierarchical'),
                    'properties': {
                        'parent_name': rel.get('parent_name'),
                        'child_name': rel.get('child_name')
                    }
                })
    
    # 添加taxonomy到结果
    result['taxonomy'] = {
        'entities': entities,
        'relations': relations
    }
    
    return result

def save_results(result, output_file):
    """保存结果到文件"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Results saved to: {output_file}")
    except Exception as e:
        print(f"Error saving results: {e}")

def process_folder(folder_path, relationships_file, output_folder):
    """处理文件夹中的所有教授文件"""
    if not os.path.exists(folder_path):
        print(f"Error: Folder '{folder_path}' not found")
        return
    
    # 创建输出文件夹
    os.makedirs(output_folder, exist_ok=True)
    
    # 获取所有教授详细文件
    professor_files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('_detail.json'):
            professor_files.append(os.path.join(folder_path, filename))
    
    if not professor_files:
        print(f"No professor detail files found in {folder_path}")
        return
    
    print(f"Found {len(professor_files)} professor files to process")
    print(f"Output will be saved to: {output_folder}")
    
    successful = 0
    failed = 0
    
    for i, professor_file in enumerate(professor_files, 1):
        try:
            print(f"\n[{i}/{len(professor_files)}] Processing: {os.path.basename(professor_file)}")
            
            # 分析教授主题
            result = analyze_professor_topics(professor_file, relationships_file)
            
            if result is None:
                print(f"  Error: Failed to analyze {professor_file}")
                failed += 1
                continue
            
            # 生成输出文件名
            prof_name = result['professor_info'].get('name', 'unknown')
            prof_id = result['professor_info'].get('author_id', 'unknown')
            safe_name = prof_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            output_file = os.path.join(output_folder, f"topics_analysis_{safe_name}_{prof_id}.json")
            
            # 保存结果
            save_results(result, output_file)
            
            # 打印简要统计信息
            print(f"  ✓ Professor: {prof_name}")
            print(f"    Topics: {result['topic_analysis']['total_topics']}, Papers: {result['professor_info'].get('total_papers', 0)}")
            
            successful += 1
            
        except Exception as e:
            print(f"  ✗ Error processing {professor_file}: {str(e)}")
            # 打印更详细的错误信息用于调试
            import traceback
            print(f"  Debug info: {traceback.format_exc()}")
            failed += 1
    
    print(f"\n=== Processing Complete ===")
    print(f"Successfully processed: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(professor_files)}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_professor_topics.py <folder_path_or_file> [relationships_file] [output_folder]")
        print("Example: python analyze_professor_topics.py /mnt/ssd/yirui/openalexdata/data/output/computer_science")
        print("         python analyze_professor_topics.py data/output/computer_science data/computer_science_relationships.json output_topics")
        print("         python analyze_professor_topics.py data/output/computer_science/professor_detail.json")
        sys.exit(1)
    
    input_path = sys.argv[1]
    relationships_file = sys.argv[2] if len(sys.argv) > 2 else "data/computer_science_relationships.json"
    output_folder = sys.argv[3] if len(sys.argv) > 3 else "professor_topics_output"
    
    if os.path.exists(relationships_file):
        print(f"Using relationships from: {relationships_file}")
    else:
        print(f"Warning: Relationships file '{relationships_file}' not found, proceeding without hierarchical relationships")
    
    # 检查输入是文件还是文件夹
    if os.path.isfile(input_path) and input_path.endswith('_detail.json'):
        # 处理单个文件
        print(f"Processing single file: {input_path}")
        os.makedirs(output_folder, exist_ok=True)
        
        try:
            result = analyze_professor_topics(input_path, relationships_file)
            if result:
                prof_name = result['professor_info'].get('name', 'unknown')
                prof_id = result['professor_info'].get('author_id', 'unknown')
                safe_name = prof_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                output_file = os.path.join(output_folder, f"topics_analysis_{safe_name}_{prof_id}.json")
                
                save_results(result, output_file)
                print(f"✓ Professor: {prof_name}")
                print(f"  Topics: {result['topic_analysis']['total_topics']}, Papers: {result['professor_info'].get('total_papers', 0)}")
            else:
                print("Error: Failed to analyze the file")
        except Exception as e:
            print(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
    
    elif os.path.isdir(input_path):
        # 处理整个文件夹
        process_folder(input_path, relationships_file, output_folder)
    else:
        print(f"Error: '{input_path}' is not a valid file or directory")
        sys.exit(1)

if __name__ == "__main__":
    main()
