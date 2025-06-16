from abc import ABC, abstractmethod
from core.schema import *
import os


class FileParser(ABC):

    @abstractmethod
    def parse(self, file_path: str) -> FileBaseProperty:
        pass

    def _get_file_type(self, file_path: str) -> str:
        """获取文件类型"""
        _, ext = os.path.splitext(file_path)
        return ext.lower().replace('.', '')
    
    def _calculate_md5(self, file_path: str) -> str:
        """计算文件的MD5哈希值"""
        import hashlib
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def __init__(self, file_path: str = None):
        super().__init__()
        
        if file_path is None:
            raise ValueError("file_path cannot be None")
        self.property = FileBaseProperty(os.path.basename(file_path))
        self.property.file_path = file_path
        self.property.file_size = os.path.getsize(file_path)
        self.property.file_type = self._get_file_type(file_path)
        self.created_at = os.path.getctime(file_path)
        self.updated_at = os.path.getmtime(file_path)
        self.property.md5_hash = self._calculate_md5(file_path)
