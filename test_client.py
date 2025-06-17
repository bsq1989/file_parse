#!/usr/bin/env python3
"""
文件解析状态客户端测试脚本
演示CRUD操作的使用方法
"""

import sys
import os
import logging
from datetime import datetime

# 添加当前目录到路径
sys.path.append(os.path.dirname(__file__))

try:
    from file_parse_client import FileParseStatusClient
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保已安装 psycopg2: pip install psycopg2-binary")
    sys.exit(1)

def test_crud_operations():
    """测试CRUD操作"""
    # 设置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 创建客户端
    print("=== 创建数据库客户端 ===")
    client = FileParseStatusClient()
    
    # 测试数据
    test_tasks = [
        {
            'task_id': 'test_001',
            'file_name': 'document.pdf',
            'file_path': '/uploads/document.pdf',
            'file_type': 'pdf'
        },
        {
            'task_id': 'test_002',
            'file_name': 'data.xlsx',
            'file_path': '/uploads/data.xlsx',
            'file_type': 'xlsx'
        },
        {
            'task_id': 'test_003',
            'file_name': 'presentation.pptx',
            'file_path': '/uploads/presentation.pptx',
            'file_type': 'pptx'
        }
    ]
    
    print("\n=== 创建任务 (CREATE) ===")
    for task in test_tasks:
        success = client.create_task(**task)
        print(f"创建任务 {task['task_id']}: {'成功' if success else '失败'}")
    
    print("\n=== 查询单个任务 (READ) ===")
    task = client.get_task('test_001')
    if task:
        print(f"任务信息: {task['task_id']} - {task['file_name']} - {task['status']}")
    else:
        print("任务不存在")
    
    print("\n=== 更新任务状态 (UPDATE) ===")
    # 模拟任务处理流程
    task_id = 'test_001'
    
    # 开始处理
    client.update_task_status(task_id, 'processing', 0)
    print(f"任务 {task_id}: 开始处理")
    
    # 处理中
    client.update_task_status(task_id, 'processing', 50)
    print(f"任务 {task_id}: 处理进度 50%")
    
    # 完成
    client.update_task_status(task_id, 'completed', 100, result_path='/results/document_parsed.json')
    print(f"任务 {task_id}: 处理完成")
    
    # 失败示例
    client.update_task_status('test_002', 'processing', 30)
    client.update_task_status('test_002', 'failed', 30, error_message='文件格式不支持')
    print(f"任务 test_002: 处理失败")
    
    print("\n=== 查询任务列表 (READ) ===")
    
    # 所有任务
    all_tasks = client.list_tasks(limit=10)
    print(f"所有任务: {len(all_tasks)} 个")
    for task in all_tasks:
        print(f"  - {task['task_id']}: {task['status']} ({task['progress']}%)")
    
    # 按状态查询
    completed_tasks = client.list_tasks(status='completed')
    print(f"\n已完成任务: {len(completed_tasks)} 个")
    
    failed_tasks = client.list_tasks(status='failed')
    print(f"失败任务: {len(failed_tasks)} 个")
    
    # 活跃任务
    active_tasks = client.get_active_tasks()
    print(f"活跃任务: {len(active_tasks)} 个")
    
    # 按文件类型查询
    pdf_tasks = client.get_tasks_by_file_type('pdf')
    print(f"PDF任务: {len(pdf_tasks)} 个")
    
    print("\n=== 统计信息 ===")
    stats = client.get_statistics()
    print(f"总任务数: {stats.get('total_tasks', 0)}")
    print(f"待处理: {stats.get('pending_tasks', 0)}")
    print(f"处理中: {stats.get('processing_tasks', 0)}")
    print(f"已完成: {stats.get('completed_tasks', 0)}")
    print(f"失败: {stats.get('failed_tasks', 0)}")
    print(f"平均处理时间: {stats.get('avg_processing_time_seconds', 0):.2f} 秒")
    
    print("\n=== 任务计数 ===")
    total_count = client.count_tasks()
    pending_count = client.count_tasks('pending')
    print(f"总任务数: {total_count}")
    print(f"待处理任务数: {pending_count}")
    
    print("\n=== 清理测试 (可选) ===")
    # 注意：这会删除测试数据
    # 如果需要保留测试数据，请注释掉下面的代码
    
    choice = input("是否删除测试数据? (y/N): ").strip().lower()
    if choice == 'y':
        print("\n=== 删除任务 (DELETE) ===")
        for task in test_tasks:
            success = client.delete_task(task['task_id'])
            print(f"删除任务 {task['task_id']}: {'成功' if success else '失败'}")
    else:
        print("保留测试数据")

def test_error_handling():
    """测试错误处理"""
    print("\n=== 错误处理测试 ===")
    client = FileParseStatusClient()
    
    # 尝试创建重复任务
    task_id = 'duplicate_test'
    client.create_task(task_id, 'test.pdf', '/test.pdf', 'pdf')
    success = client.create_task(task_id, 'test2.pdf', '/test2.pdf', 'pdf')
    print(f"创建重复任务: {'失败(符合预期)' if not success else '意外成功'}")
    
    # 尝试更新不存在的任务
    success = client.update_task_status('nonexistent_task', 'completed')
    print(f"更新不存在的任务: {'失败(符合预期)' if not success else '意外成功'}")
    
    # 尝试删除不存在的任务
    success = client.delete_task('nonexistent_task')
    print(f"删除不存在的任务: {'失败(符合预期)' if not success else '意外成功'}")
    
    # 清理测试任务
    client.delete_task('duplicate_test')

if __name__ == "__main__":
    try:
        test_crud_operations()
        test_error_handling()
        print("\n=== 测试完成 ===")
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
