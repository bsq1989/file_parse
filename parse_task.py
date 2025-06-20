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
import requests
from utils import submit_convert_task, get_convert_task_status,identify_office_file
from core import * 
from file_parse_client import FileParseStatusClient
from core.object_spliter import text_spliter, table_spliter, image_spliter
from core.utils import get_embedding
from milvus_client import MilvusVectorDB
import hashlib


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
                "file_key": "test.docx",  # 示例任务数据
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
def doc_convert(file_path):
    try:
        with open('./config.json', 'r') as f:
            convert_cfg = json.load(f)
            converter_host = convert_cfg.get('officeConverterUrl', 'http://localhost:8000')
          

        if converter_host is None:
            return {
                "status": "error",
                "message": "转换服务 URL 未配置"
            }
        if not os.path.exists(file_path):
            return {
                "status": "error",
                "message": f"文件 {file_path} 不存在"
            }
        # 提交转换任务
        response = submit_convert_task(file_path, f"{converter_host}/convert")
        if response is None:
            return {
                "status": "error",
                "message": "文件转换请求失败"
            }
        task_id = response.get('task_id')
        if not task_id:
            return {
                "status": "error",
                "message": "转换任务 ID 未返回"
            }
        logger.info(f"文件转换任务已提交，任务 ID: {task_id}")
        # 查询转换任务状态
        status_url = f"{converter_host}/status/{task_id}"
        status_response = get_convert_task_status(status_url)
        # logger.info(f"查询转换任务状态: {status_response}")
        max_query_attempts = 10
        query_attempts = 0
        while status_response and query_attempts < max_query_attempts:
            status =  status_response.get('status')
            if status == 'completed':
                download_url = status_response.get('minio_object')
                if not download_url:
                    raise ValueError(f"转换结果中没有下载URL: {file_path}")
                # 下载转换后的文件
                new_file_name = f"{download_url.split('/')[-1]}"
                bucket_name = status_response.get('bucket')
                return {
                    "status": "success",
                    "message": f"文件转换成功",
                    "object_name": download_url,
                    "file_name": new_file_name,
                    "bucket_name": bucket_name
                }
            elif status == 'failed':
                return {
                    "status": "error",
                    "message": f"文件转换失败: {status_response.get('message', '未知错误')}"
                }
            else:
                logger.info(f"转换任务 {task_id} 仍在处理中，等待 5 秒后重试...")
                time.sleep(5)
                query_attempts += 1
                status_response = get_convert_task_status(status_url)
    except Exception as e:
        return {
            "status": "error",
            "message": f"文件转换失败: {e}"
        }
    finally:
        pass

@app.task()
def embedding_test():
    try:
        with open('./config.json', 'r') as f:
            embedding_cfg = json.load(f)
            embedding_model = embedding_cfg.get('embeddingModel', 'bge-m3')
            embedding_base_url = embedding_cfg.get('embeddingBaseUrl', 'http://localhost:6001/v1')
            embedding_api_key = embedding_cfg.get('embeddingToken', None)

            inputs = [
                "这是一个测试文本，用于验证嵌入模型是否正常工作。",
                "嵌入模型可以将文本转换为向量表示，便于后续的相似度计算和检索。",
                "如果嵌入模型工作正常，则应该能够返回有效的向量表示。",
                "请确保配置文件中的 embeddingModel 和 embeddingBaseUrl 正确无误。",
                "如果有任何问题，请检查日志文件以获取更多信息。"
            ]
            _, results, msg = get_embedding(inputs, model=embedding_model, base_url=embedding_base_url, api_key=embedding_api_key)
            if results:
                for idx, result in enumerate(results):
                    logger.info(f"文本: {inputs[idx]}\n嵌入向量: {result[:10]}... (总长度: {len(result)})")
            
            return {
                "status": "success",
                "message": "嵌入模型测试成功"
            }
        
    except Exception as e:
        logger.error(f"读取配置文件失败: {e}")
        return {
            "status": "error",
            "message": f"读取配置文件失败: {str(e)}"
        }

@app.task()
def process():
    try:
        with open('./config.json', 'r') as f:
            parse_cfg = json.load(f)
        
        new_task = pull_task()
        if new_task.get("status") == "success" and new_task.get("task"):
            db_client = FileParseStatusClient('./config.json')
            task_id = new_task.get("task").get("task_id")
            task_name = new_task.get("task").get("task_name")
            ts = new_task.get("task").get("ts")
            file_key = new_task.get("task").get("file_key")
            file_bucket = new_task.get("task").get("file_bucket")
            db_client.create_task(
                task_id=task_id,
                file_name=file_key,
                file_path= file_key,
                file_type=os.path.splitext(file_key)[1].lower(),
            )
            db_client.update_task_status(task_id, 'downloading',progress=10)
            result = download_file(file_key, file_bucket)
            if result.get("status") == "success":
                db_client.update_task_status(task_id, 'downloaded',progress=20)
                file_path = result.get("file_path")
                if os.path.exists(file_path):
                    target_file_path = file_path
                    file_ext = os.path.splitext(file_path)[1].lower()
                    if file_ext in ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']:
                    # 处理 Office 文件
                        office_type = identify_office_file(file_path)
                        if office_type in ['doc','ppt','xls']:
                            logger.info(f"识别到文件类型: {office_type}")
                            # 提交转换任务
                            db_client.update_task_status(task_id, 'converting')
                            convert_result = doc_convert(file_path)
                            if convert_result.get("status") == "success":
                                # 成功转换文件
                                object_name = convert_result.get("object_name")
                                file_name = convert_result.get("file_name")
                                bucket_name = convert_result.get("bucket_name")
                                result = download_file(object_name, bucket_name)
                                target_file_path = result.get("file_path")
                            else:
                                return {
                                    "status": "error",
                                    "message": f"文件转换失败: {convert_result.get('message')}"
                                }
                    # 开始文件解析
                    db_client.update_task_status(task_id, 'parsing', progress=50)
                    parser = create_parser(target_file_path)
                    if parser:
                        parsed_content = parser.parse(target_file_path)
                        # 更新任务状态为已解析
                        
                        docs = []
                        total_part_count = len(parsed_content.content_list)

                        for idx, content in enumerate(parsed_content.content_list):
                            tmp_docs = []
                            if isinstance(content, TextProperty):
                                tmp_docs = text_spliter(content)

                            elif isinstance(content, TableProperty):
                                tmp_docs = table_spliter(content)

                            elif isinstance(content, ImageProperty):
                                tmp_docs = image_spliter(content)

                            else:
                                logger.info(f"其他内容类型: {type(content)} : {content}")
                            for sub_idx,doc in enumerate(tmp_docs):
                                doc.metadata['file_name'] = parsed_content.file_name
                                doc.metadata['file_type'] =  parsed_content.file_type
                                doc.metadata['total_part_count'] = total_part_count
                                doc.metadata['part_index'] = idx
                                doc.metadata['part_sub_index'] = sub_idx
                                doc.metadata['part_sub_count'] = len(tmp_docs)
                                doc.metadata['total_txt_len'] = parsed_content.total_text_length
                                doc.metadata['total_token_count'] = parsed_content.total_token_length
                                doc.metadata['token_length'] = content.content_token_length
                                docs.append(doc)

                        db_client.update_task_status(task_id, 'embedding', progress=80)

                        # 计算embedding
                        if parse_cfg:
                            embedding_model = parse_cfg.get('embeddingModel', 'bge-m3')
                            embedding_base_url = parse_cfg.get('embeddingBaseUrl', 'http://localhost:6001/v1')
                            embedding_api_key = parse_cfg.get('embeddingToken', None)
                            batch_size = parse_cfg.get('embeddingBatchSize', 32)
                            # make batch data, group docs by  batch_size
                            group_docs = [docs[i:i + batch_size] for i in range(0, len(docs), batch_size)]
                            embedding_result = []
                            for batch_doc in group_docs:
                                embedding_input = [doc.page_content for doc in batch_doc]
                                flag, result, msg = get_embedding(text=embedding_input, model=embedding_model, base_url=embedding_base_url, api_key=embedding_api_key)
                                if flag:
                                    embedding_result.extend(result)
                                else:
                                    logger.error(f"获取嵌入失败: {msg}")
                                    db_client.update_task_status(task_id, 'failed', progress=0, error_message='向量化失败')
                                    return {
                                        "status": "error",
                                        "message": f"获取嵌入失败: {msg}"
                                    }
                            for idx, doc in enumerate(docs):
                                doc.metadata['embeddings'] = embedding_result[idx]
                            # insert into milvus
                            milvus_uri = parse_cfg.get('milvusUri', 'localhost:19530')
                            milvus_client = MilvusVectorDB(milvus_uri)
                            milvus_cache = []
                            doc_id_prefix = f"{file_bucket}_{file_key}"
                            # 计算md5
                            doc_id_prefix = hashlib.md5(doc_id_prefix.encode()).hexdigest()
                            for idx, doc in enumerate(docs):
                                doc_id = f"{doc_id_prefix}_{idx}"
                                doc_meta = doc.metadata.copy()
                                # 删除不需要的属性
                                doc_meta.pop('embeddings', None)

                                milvus_cache.append(
                                    {
                                        "doc_id": doc_id,
                                        "text": doc.page_content,
                                        "text_dense":doc.metadata['embeddings'],
                                        "doc_meta": doc_meta,
                                        "image_dense": [0.2] * 512,
                                    }
                                )
                            milvus_client.insert_data(data = milvus_cache)
                            db_client.update_task_status(task_id, 'done', progress=100)

                        else:
                            db_client.update_task_status(task_id, 'failed', progress=0, error_message='配置文件无效')
                            return {
                                "status": "error",
                                "message": "解析配置无效"
                            }
                        db_client.update_task_status(task_id, 'done', progress=100)
                        return {
                            "status": "success",
                            "message": "文件解析成功",
                            "file_path": target_file_path,
                            "file_name": os.path.basename(target_file_path),
                            "file_type": file_ext
                        }
                    else:
                        db_client.update_task_status(task_id, 'failed', progress=0, error_message='无法创建解析器')
                        return {
                            "status": "error",
                            "message": f"无法创建解析器，文件类型可能不受支持: {file_ext}"
                        }
                else:
                    db_client.update_task_status(task_id, 'failed', progress=0, error_message='文件不存在')
                    return {
                        "status": "error",
                        "message": f"文件 {file_path} 不存在，处理失败",
                    }
            else:
                return {
                    "status": "error",
                    "message": f"文件下载失败: {result.get('message')}",
                }
        else:
            return {
                "status": "error",
                "message": f"拉取任务失败: {new_task.get('message')}",
            }
    except Exception as e:
        if task_id:
            db_client.update_task_status(task_id, 'failed', progress=0, error_message=str(e))
        logger.error(f"处理任务时发生错误: {e}")
        return {
            "status": "error",
            "message": f"处理任务失败: {str(e)}"
        }

@app.task
def test_sparse_search():
    try:
        with open('./config.json', 'r') as f:
            parse_cfg = json.load(f)
        milvus_uri = parse_cfg.get('milvusUri', 'localhost:19530')
        milvus_client = MilvusVectorDB(milvus_uri)
        query_text = "MOJITO"
        results = milvus_client.search_by_text_sparse([query_text])
        logger.info(f"查询结果: {results}")
        return {
            "status": "success",
            "message": "稀疏搜索测试成功",
            "results": results
        }
    except Exception as e:
        logger.error(f"稀疏搜索测试失败: {e}")
        return {
            "status": "error",
            "message": f"稀疏搜索测试失败: {str(e)}"
        }
    
@app.task
def test_dense_search():
    try:
        with open('./config.json', 'r') as f:
            parse_cfg = json.load(f)
        milvus_uri = parse_cfg.get('milvusUri', 'localhost:19530')
        embedding_model = parse_cfg.get('embeddingModel', 'bge-m3')
        embedding_base_url = parse_cfg.get('embeddingBaseUrl', 'http://localhost:6001/v1')
        embedding_api_key = parse_cfg.get('embeddingToken', None)
        milvus_client = MilvusVectorDB(milvus_uri)
        query_text = "MOJITO"
        _,embedding,_ = get_embedding([query_text], model=embedding_model, base_url=embedding_base_url, api_key=embedding_api_key)

        results = milvus_client.search_by_text_dense(embedding)
        logger.info(f"查询结果: {results}")
        return {
            "status": "success",
            "message": "稠密搜索测试成功",
            "results": results
        }
    except Exception as e:
        logger.error(f"稠密搜索测试失败: {e}")
        return {
            "status": "error",
            "message": f"稠密搜索测试失败: {str(e)}"
        }
    
@app.task
def test_milvus_del():
    file_bucket = 'file-sync'
    file_key = 'test.docx'
    doc_id_prefix = f"{file_bucket}_{file_key}"
    # 计算md5
    doc_id_prefix = hashlib.md5(doc_id_prefix.encode()).hexdigest()
    try:
        with open('./config.json', 'r') as f:
            parse_cfg = json.load(f)
        milvus_uri = parse_cfg.get('milvusUri', 'localhost:19530')
        milvus_client = MilvusVectorDB(milvus_uri)
        # 删除所有以 doc_id_prefix 开头的文档
        milvus_client.delete_by_doc_id_prefix(doc_id_prefix)
        logger.info(f"成功删除以 {doc_id_prefix} 开头的所有文档")
        return {
            "status": "success",
            "message": f"成功删除以 {doc_id_prefix} 开头的所有文档"
        }
    except Exception as e:
        logger.error(f"删除文档失败: {e}")
        return {
            "status": "error",
            "message": f"删除文档失败: {str(e)}"
        }