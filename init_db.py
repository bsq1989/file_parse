#!/usr/bin/env python3
"""
数据库初始化脚本
用于创建文件解析状态相关的数据库表和索引
"""

import psycopg2
import json
import os
import sys

def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 配置文件未找到: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"错误: 配置文件格式错误: {config_path}")
        sys.exit(1)

def read_sql_file():
    """读取SQL文件内容"""
    sql_path = os.path.join(os.path.dirname(__file__), 'file_parse_status.sql')
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"错误: SQL文件未找到: {sql_path}")
        sys.exit(1)

def init_database():
    """初始化数据库"""
    print("正在初始化文件解析状态数据库...")
    
    # 加载配置
    config = load_config()
    
    # 数据库连接配置
    db_config = {
        'host': config.get('postgresIP', 'localhost'),
        'port': config.get('postgresPort', 5432),
        'user': config.get('postgresUser', 'postgres'),
        'password': config.get('postgresPassword', ''),
        'database': config.get('postgresDB', 'llm_file_parser')
    }
    
    print(f"连接到数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    try:
        # 连接数据库
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 读取并执行SQL文件
        sql_content = read_sql_file()
        cursor.execute(sql_content)
        
        print("数据库表创建成功!")
        print("示例数据插入成功!")
        print("初始化完成!")
        
        # 验证表是否创建成功
        cursor.execute("SELECT COUNT(*) FROM file_parse_status;")
        count = cursor.fetchone()[0]
        print(f"当前表中有 {count} 条记录")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"数据库错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"执行错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()
