from core.docx_parser import DocxParser
from core.pptx_parser import PptxParser
from core.xlsx_parser import XlsxParser
from core.html_parser import HtmlParser
from core.markdown_parser import MarkdownParser
from core.parser import FileParser
from core.txt_parser import TxtParser
from core.pdf_parser import PdfParser
from core.img_parser import ImgParser
import os

    
def create_parser(file_path: str) -> 'FileParser':
    """工厂函数：根据文件类型创建对应的解析器"""
    _, ext = os.path.splitext(file_path)
    file_type = ext.lower().replace('.', '')
    
    # 这里需要根据实际的解析器类来映射
    parser_mapping = {
        'docx': DocxParser,
        'xlsx': XlsxParser,
        'html': HtmlParser,
        'md': MarkdownParser,
        'pptx': PptxParser,
        'txt': TxtParser,
        'pdf':PdfParser,
        'jpg': ImgParser,
        'jpeg': ImgParser,
        'png': ImgParser,
        'bmp': ImgParser
    }
    
    parser_class = parser_mapping.get(file_type)
    if parser_class is None:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    return parser_class(file_path)