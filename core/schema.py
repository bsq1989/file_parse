from enum import Enum

class ContentType(Enum):
    """Content type for file parsing."""
    TEXT = "text"
    TABLE = "table"
    IMAGE = "image"

class BaseProperty:

    def __init__(self, name: str, content_type: ContentType):
        self.content_type = content_type
        self.page_idx = 0
        self.text_content = ""
        self.name = name
        self.content_token_length = 0



class TextProperty(BaseProperty):
    """Text property for file parsing."""
    
    def __init__(self, name: str):
        super().__init__(name, ContentType.TEXT)
        self.text_level = 0

class TableProperty(BaseProperty):
    """Table property for file parsing."""
    
    def __init__(self, name: str):
        super().__init__(name, ContentType.TABLE)
        self.html_content = ""
        self.table_row_count = 0
        self.table_column_count = 0

class ImageProperty(BaseProperty):
    """Image property for file parsing."""
    
    def __init__(self, name: str):
        super().__init__(name, ContentType.IMAGE)
        self.image_path = ""
        self.image_width = 0
        self.image_height = 0
        self.image_format = ""
        self.blob_data = None
        self.ocr_content_list = []


class FileBaseProperty:

    def __init__(self, file_name: str):
        self.file_name = file_name
        self.file_path = ""
        self.file_size = 0
        self.file_type = ""
        self.page_count = 0
        self.content_list = []
        self.created_at = None
        self.updated_at = None
        self.md5_hash = ""
        self.total_text_length = 0
        self.total_token_length = 0
        self.additional_info = {}