from celery import Celery
import time
import json
import redis
import datetime
import uuid
import minio
import os
import logging
import logging.handlers
from pathlib import Path

def setup_logger(log_dir_path=None, app_log_name='parse_task.log', error_log_name='error.log'):
    """设置logger，支持控制台和轮转文件输出
    
    Args:
        log_dir_path (str, optional): 日志目录路径，默认为 '/home/ubuntu/app/logs'
        app_log_name (str, optional): 应用日志文件名，默认为 'app.log'
        error_log_name (str, optional): 错误日志文件名，默认为 'error.log'
    
    Returns:
        logging.Logger: 配置好的logger实例
    """
    # 创建logs目录
    if log_dir_path is None:
        log_dir = Path('/home/ubuntu/app/logs')
    else:
        log_dir = Path(log_dir_path)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建logger
    logger = logging.getLogger('file_parse')
    logger.setLevel(logging.INFO)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 创建格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 轮转文件处理器 - 所有日志
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / app_log_name,
        maxBytes=20*1024*1024,  # 20MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 轮转文件处理器 - 错误日志
    error_handler = logging.handlers.RotatingFileHandler(
        log_dir / error_log_name,
        maxBytes=20*1024*1024,  # 20MB
        backupCount=10,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    return logger

log_dir_path = None
with open('./config.json', 'r') as f:
    config = json.load(f)
    log_dir_path = config.get('logPath', None)
# 创建全局logger实例
logger = setup_logger(log_dir_path=log_dir_path)

# 创建 Celery 应用
app = Celery('file_parse')

# Celery 配置
app.conf.update(
    broker_url='redis://parser_redis:6379/0',  # 消息代理
    result_backend='redis://parser_redis:6379/0',  # 结果存储
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='Asia/Shanghai',
    enable_utc=True,
)

@app.task
def pull_task():
    """拉取任务的任务"""
    with open('./config.json', 'r') as f:
        task_queue_cfg = json.load(f)
        redis_host = task_queue_cfg.get('taskQueueRedisIP', 'localhost')
        redis_port = task_queue_cfg.get('taskQueueRedisPort', 6379)
        db = task_queue_cfg.get('taskQueueRedisDB', 0)

        task_queue_key = task_queue_cfg.get('taskQueueName', 'default_queue')

        # 连接到 Redisq
        #  使用 redis-py 库
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=db)
        # 检查连接
        try:
            redis_client.ping()
            logger.info("成功连接到 Redis")
            # redis list 任务队列弹出任务
            task = redis_client.lpop(task_queue_key)
            if task:
                logger.info(f"拉取到任务: {task.decode('utf-8')}")
                return {
                    "status": "success",
                    "message": "拉取任务成功",
                    "task": json.loads(task.decode('utf-8'))  # 返回任务内容
                }
            else:
                logger.info("没有可用的任务")
                return {
                    "status": "success",
                    "message": "没有可用的任务",
                }
        except redis.ConnectionError:
            logger.error("无法连接到 Redis")
            return {
                "status": "error",
                "message": "无法连接到 Redis",  
            }

    return {
        "status": "error",
        "message": "配置文件读取失败",
    }

@app.task
def push_task():
    """推送任务的任务"""
    with open('./config.json', 'r') as f:
        task_queue_cfg = json.load(f)
        redis_host = task_queue_cfg.get('taskQueueRedisIP', 'localhost')
        redis_port = task_queue_cfg.get('taskQueueRedisPort', 6379)
        db = task_queue_cfg.get('taskQueueRedisDB', 0)

        task_queue_key = task_queue_cfg.get('taskQueueName', 'default_queue')

        # 连接到 Redis
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=db)
        try:
            redis_client.ping()
            logger.info("成功连接到 Redis")
            # 将任务推送到队列
            task = {
                "task_id": uuid.uuid4().hex,  # 使用 UUID 作为任务 ID
                "task_name": "example_task",  # 示例任务名称
                "ts": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 当前时间戳
                "file_key": "Mac 电脑环境配置.docx",  # 示例任务数据
                "file_bucket": "file-sync"  # 示例存储桶
            }

            redis_client.rpush(task_queue_key, json.dumps(task))
            logger.info(f"任务 {task} 已推送到队列")
            return {
                "status": "success",
                "message": f"任务 {task} 已推送到队列",
            }
        except redis.ConnectionError:
            logger.error("无法连接到 Redis")
            return {
                "status": "error",
                "message": "无法连接到 Redis",
            }

    return {
        "status": "error",
        "message": "配置文件读取失败",
    }


@app.task()
def download_file(file_key, file_bucket):
    """下载文件的任务"""
    with open('./config.json', 'r') as f:
        minio_cfg = json.load(f)
        minio_host = minio_cfg.get('minioIP', 'localhost')
        minio_port = minio_cfg.get('minioPort', 9000)
        minio_access_key = minio_cfg.get('minioAccessKey', 'minioadmin')
        minio_secret_key = minio_cfg.get('minioSecretKey', 'minioadmin')

        # 创建 MinIO 客户端
        client = minio.Minio(
            f'{minio_host}:{minio_port}',
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )

        try:
            # 检查连接
            client.list_buckets()
            logger.info("成功连接到 MinIO")

            # 下载文件
            file_folder = f'/tmp/file_parse_cache/downloads/{uuid.uuid4().hex}'
            file_path = f'{file_folder}/{file_key}'
            # 确保文件夹存在
            os.makedirs(file_folder, exist_ok=True)
            client.fget_object(file_bucket, file_key, file_path)
            logger.info(f"文件 {file_key} 已下载到 {file_path}")
            return {
                "status": "success",
                "message": f"文件 {file_key} 已下载到 {file_path}",
                "file_path": file_path
            }
        except Exception as e:
            logger.error(f"下载文件失败: {e}")
            return {
                "status": "error",
                "message": str(e),
            }

@app.task()
def process():
    new_task = pull_task()
    if new_task.get("status") == "success" and new_task.get("task"):
        file_key = new_task.get("task").get("file_key")
        file_bucket = new_task.get("task").get("file_bucket")
        result = download_file(file_key, file_bucket)
        if result.get("status") == "success":
            file_path = result.get("file_path")
            if os.path.exists(file_path):
                logger.info(f"文件 {file_path} 下载成功，开始处理...")
                time.sleep(2)
                logger.info(f"文件 {file_path} 处理完成")
            else:
                return {
                    "status": "error",
                    "message": f"文件 {file_path} 不存在，处理失败",
                }
