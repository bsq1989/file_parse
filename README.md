# 文件解析状态数据库客户端

这是一个简单而完整的PostgreSQL数据库客户端，用于管理文件解析任务的状态。

## 文件说明

- `file_parse_status.sql` - 数据库表结构定义
- `init_db.py` - 数据库初始化脚本
- `file_parse_client.py` - 数据库操作客户端
- `parse_status_manager.py` - 简化的状态管理器
- `test_client.py` - 客户端测试脚本

## 数据库表结构

```sql
CREATE TABLE file_parse_status (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(255) UNIQUE NOT NULL,  -- 任务ID
    file_name VARCHAR(500) NOT NULL,       -- 文件名
    file_path TEXT NOT NULL,               -- 文件路径
    file_type VARCHAR(50),                 -- 文件类型
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 状态
    progress INTEGER DEFAULT 0,            -- 进度百分比
    error_message TEXT,                     -- 错误信息
    result_path TEXT,                       -- 解析结果路径
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 状态说明

- `pending` - 等待处理
- `processing` - 处理中
- `completed` - 处理完成
- `failed` - 处理失败

## 安装依赖

```bash
pip install psycopg2-binary
```

## 使用方法

### 1. 初始化数据库

```bash
python3 init_db.py
```

### 2. 使用客户端

```python
from file_parse_client import FileParseStatusClient

# 创建客户端
client = FileParseStatusClient()

# 创建任务
client.create_task(
    task_id="task_001",
    file_name="document.pdf",
    file_path="/uploads/document.pdf",
    file_type="pdf"
)

# 更新状态
client.update_task_status("task_001", "processing", 50)
client.update_task_status("task_001", "completed", 100, 
                         result_path="/results/document.json")

# 查询任务
task = client.get_task("task_001")
print(task)

# 获取任务列表
tasks = client.list_tasks(status="completed", limit=10)
print(f"已完成任务: {len(tasks)} 个")

# 获取统计信息
stats = client.get_statistics()
print(stats)
```

### 3. 运行测试

```bash
python3 test_client.py
```

## API 方法

### 创建操作 (CREATE)
- `create_task(task_id, file_name, file_path, file_type)` - 创建新任务

### 查询操作 (READ)
- `get_task(task_id)` - 获取单个任务
- `list_tasks(status, limit, offset)` - 获取任务列表
- `get_active_tasks(limit)` - 获取活跃任务
- `get_completed_tasks(limit)` - 获取已完成任务
- `get_tasks_by_file_type(file_type, limit)` - 按文件类型查询
- `count_tasks(status)` - 统计任务数量
- `get_statistics()` - 获取统计信息

### 更新操作 (UPDATE)
- `update_task_status(task_id, status, progress, error_message, result_path)` - 更新任务状态

### 删除操作 (DELETE)
- `delete_task(task_id)` - 删除任务
- `cleanup_old_tasks(days, status_list)` - 清理旧任务

## 配置文件

客户端会读取 `config.json` 文件中的数据库配置：

```json
{
    "postgresIP": "localhost",
    "postgresPort": 5432,
    "postgresUser": "postgres",
    "postgresPassword": "password",
    "postgresDB": "llm_file_parser"
}
```

## 典型使用流程

1. **创建任务**
   ```python
   client.create_task("task_123", "file.pdf", "/path/to/file.pdf", "pdf")
   ```

2. **开始处理**
   ```python
   client.update_task_status("task_123", "processing", 0)
   ```

3. **更新进度**
   ```python
   client.update_task_status("task_123", "processing", 50)
   ```

4. **完成处理**
   ```python
   client.update_task_status("task_123", "completed", 100, 
                           result_path="/results/file_parsed.json")
   ```

5. **处理失败**
   ```python
   client.update_task_status("task_123", "failed", 30, 
                           error_message="文件格式不支持")
   ```

## 错误处理

客户端包含完整的错误处理机制：
- 自动处理数据库连接错误
- 防止重复任务ID
- 处理不存在的任务更新
- 记录详细的日志信息

## 注意事项

1. 确保PostgreSQL服务正在运行
2. 确保数据库和表已正确创建
3. 检查数据库连接配置
4. 建议在生产环境中启用适当的日志级别
