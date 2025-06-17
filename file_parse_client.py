"""
文件解析状态数据库客户端
提供完整的CRUD操作接口
"""

import psycopg2
import psycopg2.extras
import json
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

class FileParseStatusClient:
    """文件解析状态数据库客户端"""
    
    def __init__(self, config_file='../config.json'):
        """初始化数据库连接配置"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            # 如果找不到配置文件，使用默认配置
            config = {
                'postgresIP': 'localhost',
                'postgresPort': 5432,
                'postgresUser': 'postgres',
                'postgresPassword': '',
                'postgresDB': 'llm_file_parser'
            }
            
        self.db_config = {
            'host': config.get('postgresIP', 'localhost'),
            'port': config.get('postgresPort', 5432),
            'user': config.get('postgresUser', 'postgres'),
            'password': config.get('postgresPassword', ''),
            'database': config.get('postgresDB', 'llm_file_parser')
        }
        
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    def create_task(self, task_id: str, file_name: str, file_path: str, 
                   file_type: str = None) -> bool:
        """
        创建新的解析任务记录
        
        Args:
            task_id: 任务唯一标识
            file_name: 文件名
            file_path: 文件路径
            file_type: 文件类型
            
        Returns:
            bool: 创建是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO file_parse_status 
                        (task_id, file_name, file_path, file_type, status, progress)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (task_id, file_name, file_path, file_type, 'pending', 0))
                    
                conn.commit()
                self.logger.info(f"创建任务记录成功: {task_id}")
                return True
                
        except psycopg2.IntegrityError as e:
            self.logger.error(f"任务ID已存在: {task_id}")
            return False
        except Exception as e:
            self.logger.error(f"创建任务记录失败: {e}")
            return False
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        根据任务ID获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict: 任务信息，如果不存在返回None
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM file_parse_status WHERE task_id = %s
                    """, (task_id,))
                    
                    result = cur.fetchone()
                    return dict(result) if result else None
                    
        except Exception as e:
            self.logger.error(f"获取任务信息失败: {e}")
            return None
    
    def update_task_status(self, task_id: str, status: str, progress: int = None,
                          error_message: str = None, result_path: str = None) -> bool:
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            status: 状态 (pending, processing, completed, failed)
            progress: 进度百分比 (0-100)
            error_message: 错误信息
            result_path: 结果路径
            
        Returns:
            bool: 更新是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # 构建更新字段
                    update_fields = ["status = %s"]
                    params = [status]
                    
                    if progress is not None:
                        update_fields.append("progress = %s")
                        params.append(progress)
                    
                    if error_message is not None:
                        update_fields.append("error_message = %s")
                        params.append(error_message)
                    
                    if result_path is not None:
                        update_fields.append("result_path = %s")
                        params.append(result_path)
                    
                    # 根据状态更新时间戳
                    if status == 'processing':
                        update_fields.append("start_time = %s")
                        params.append(datetime.now())
                    elif status in ['completed', 'failed']:
                        update_fields.append("end_time = %s")
                        params.append(datetime.now())
                    
                    params.append(task_id)
                    
                    cur.execute(f"""
                        UPDATE file_parse_status 
                        SET {', '.join(update_fields)}
                        WHERE task_id = %s
                    """, params)
                    
                    if cur.rowcount == 0:
                        self.logger.warning(f"任务不存在: {task_id}")
                        return False
                    
                conn.commit()
                self.logger.info(f"更新任务状态成功: {task_id} -> {status}")
                return True
                
        except Exception as e:
            self.logger.error(f"更新任务状态失败: {e}")
            return False
    
    def delete_task(self, task_id: str) -> bool:
        """
        删除任务记录
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 删除是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM file_parse_status WHERE task_id = %s
                    """, (task_id,))
                    
                    if cur.rowcount == 0:
                        self.logger.warning(f"任务不存在: {task_id}")
                        return False
                    
                conn.commit()
                self.logger.info(f"删除任务成功: {task_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"删除任务失败: {e}")
            return False
    
    def list_tasks(self, status: str = None, limit: int = 100, 
                   offset: int = 0) -> List[Dict[str, Any]]:
        """
        获取任务列表
        
        Args:
            status: 按状态过滤 (可选)
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            List[Dict]: 任务列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if status:
                        cur.execute("""
                            SELECT * FROM file_parse_status 
                            WHERE status = %s
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (status, limit, offset))
                    else:
                        cur.execute("""
                            SELECT * FROM file_parse_status 
                            ORDER BY created_at DESC
                            LIMIT %s OFFSET %s
                        """, (limit, offset))
                    
                    results = cur.fetchall()
                    return [dict(row) for row in results]
                    
        except Exception as e:
            self.logger.error(f"获取任务列表失败: {e}")
            return []
    
    def count_tasks(self, status: str = None) -> int:
        """
        统计任务数量
        
        Args:
            status: 按状态过滤 (可选)
            
        Returns:
            int: 任务数量
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if status:
                        cur.execute("""
                            SELECT COUNT(*) FROM file_parse_status WHERE status = %s
                        """, (status,))
                    else:
                        cur.execute("""
                            SELECT COUNT(*) FROM file_parse_status
                        """)
                    
                    result = cur.fetchone()
                    return result[0] if result else 0
                    
        except Exception as e:
            self.logger.error(f"统计任务数量失败: {e}")
            return 0
    
    def get_tasks_by_file_type(self, file_type: str, 
                              limit: int = 100) -> List[Dict[str, Any]]:
        """
        根据文件类型获取任务
        
        Args:
            file_type: 文件类型
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 任务列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM file_parse_status 
                        WHERE file_type = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (file_type, limit))
                    
                    results = cur.fetchall()
                    return [dict(row) for row in results]
                    
        except Exception as e:
            self.logger.error(f"根据文件类型获取任务失败: {e}")
            return []
    
    def get_active_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取活跃任务 (pending 和 processing 状态)
        
        Args:
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 活跃任务列表
        """
        return self.list_tasks_by_status(['pending', 'processing'], limit)
    
    def get_completed_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取已完成任务 (completed 和 failed 状态)
        
        Args:
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 已完成任务列表
        """
        return self.list_tasks_by_status(['completed', 'failed'], limit)
    
    def list_tasks_by_status(self, status_list: List[str], 
                            limit: int = 100) -> List[Dict[str, Any]]:
        """
        根据状态列表获取任务
        
        Args:
            status_list: 状态列表
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 任务列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    placeholders = ','.join(['%s'] * len(status_list))
                    cur.execute(f"""
                        SELECT * FROM file_parse_status 
                        WHERE status IN ({placeholders})
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, status_list + [limit])
                    
                    results = cur.fetchall()
                    return [dict(row) for row in results]
                    
        except Exception as e:
            self.logger.error(f"根据状态列表获取任务失败: {e}")
            return []
    
    def cleanup_old_tasks(self, days: int = 30, 
                         status_list: List[str] = None) -> int:
        """
        清理旧任务
        
        Args:
            days: 保留天数
            status_list: 要清理的状态列表，默认清理已完成和失败的任务
            
        Returns:
            int: 清理的任务数量
        """
        if status_list is None:
            status_list = ['completed', 'failed']
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    placeholders = ','.join(['%s'] * len(status_list))
                    cur.execute(f"""
                        DELETE FROM file_parse_status 
                        WHERE status IN ({placeholders})
                        AND created_at < CURRENT_DATE - INTERVAL '%s days'
                    """, status_list + [days])
                    
                    deleted_count = cur.rowcount
                    conn.commit()
                    
                    self.logger.info(f"清理了 {deleted_count} 条旧任务记录")
                    return deleted_count
                    
        except Exception as e:
            self.logger.error(f"清理旧任务失败: {e}")
            return 0
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取任务统计信息
        
        Returns:
            Dict: 统计信息
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_tasks,
                            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_tasks,
                            COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_tasks,
                            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_tasks,
                            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_tasks,
                            AVG(CASE WHEN status = 'completed' AND end_time IS NOT NULL AND start_time IS NOT NULL 
                                     THEN EXTRACT(EPOCH FROM (end_time - start_time)) END) as avg_processing_time_seconds
                        FROM file_parse_status
                    """)
                    
                    result = cur.fetchone()
                    if result:
                        return {
                            'total_tasks': result[0],
                            'pending_tasks': result[1],
                            'processing_tasks': result[2],
                            'completed_tasks': result[3],
                            'failed_tasks': result[4],
                            'avg_processing_time_seconds': float(result[5]) if result[5] else 0
                        }
                    return {}
                    
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {e}")
            return {}

# 使用示例
if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO)
    
    # 创建客户端
    client = FileParseStatusClient()
    
    # 创建任务
    task_id = "test_task_001"
    success = client.create_task(
        task_id=task_id,
        file_name="test.pdf",
        file_path="/files/test.pdf",
        file_type="pdf"
    )
    print(f"创建任务: {success}")
    
    # 查询任务
    task = client.get_task(task_id)
    print(f"任务信息: {task}")
    
    # 更新任务状态
    client.update_task_status(task_id, 'processing', 50)
    client.update_task_status(task_id, 'completed', 100, result_path="/results/test.json")
    
    # 获取任务列表
    tasks = client.list_tasks(limit=10)
    print(f"任务列表: {len(tasks)} 个任务")
    
    # 获取统计信息
    stats = client.get_statistics()
    print(f"统计信息: {stats}")
    
    # 清理任务
    # client.delete_task(task_id)
    # print(f"删除任务: {task_id}")
