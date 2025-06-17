-- 文件解析状态表
-- 简单记录文件解析过程中的状态

-- 删除已存在的表
DROP TABLE IF EXISTS file_parse_status CASCADE;

-- 文件解析状态表
CREATE TABLE file_parse_status (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(255) UNIQUE NOT NULL,  -- 任务ID
    file_name VARCHAR(500) NOT NULL,       -- 文件名
    file_path TEXT NOT NULL,               -- 文件路径
    file_type VARCHAR(50),                 -- 文件类型 (pdf, docx, xlsx等)
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 状态: pending, processing, completed, failed
    progress INTEGER DEFAULT 0,            -- 进度百分比 (0-100)
    error_message TEXT,                     -- 错误信息
    result_path TEXT,                       -- 解析结果路径
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_task_id ON file_parse_status(task_id);
CREATE INDEX idx_status ON file_parse_status(status);
CREATE INDEX idx_created_at ON file_parse_status(created_at);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_file_parse_status_timestamp
    BEFORE UPDATE ON file_parse_status
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- 示例数据
INSERT INTO file_parse_status (task_id, file_name, file_path, file_type, status, progress) VALUES 
('task_001', 'test.pdf', '/files/test.pdf', 'pdf', 'processing', 50),
('task_002', 'demo.docx', '/files/demo.docx', 'docx', 'completed', 100),
('task_003', 'data.xlsx', '/files/data.xlsx', 'xlsx', 'failed', 30);

