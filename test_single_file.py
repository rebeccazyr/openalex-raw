#!/usr/bin/env python3
import json
import traceback

def test_single_file(file_path):
    """测试单个文件的处理"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        prof_info = data.get('professor_info', {})
        papers = data.get('papers', [])
        
        print(f"Professor: {prof_info.get('name')}")
        print(f"Total papers: {len(papers)}")
        
        # 检查每篇论文
        problematic_papers = []
        for i, paper in enumerate(papers):
            if paper is None:
                problematic_papers.append(f"Paper {i}: None")
                continue
            
            if not isinstance(paper, dict):
                problematic_papers.append(f"Paper {i}: Not a dict, type={type(paper)}")
                continue
                
            primary_topic = paper.get('primary_topic')
            if primary_topic is None:
                problematic_papers.append(f"Paper {i}: primary_topic is None")
                continue
                
            if not isinstance(primary_topic, dict):
                problematic_papers.append(f"Paper {i}: primary_topic not a dict, type={type(primary_topic)}")
                continue
                
            # 检查嵌套结构
            topic_id = primary_topic.get('id')
            if topic_id is None:
                problematic_papers.append(f"Paper {i}: topic_id is None")
                
        if problematic_papers:
            print("Problematic papers found:")
            for prob in problematic_papers[:10]:  # 只显示前10个
                print(f"  {prob}")
        else:
            print("No obvious problems found")
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_single_file("data/output/computer_science/Zhenkai Liang_A5084611756_detail.json")
