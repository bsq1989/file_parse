import mistune


from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser
from core.html_parser import HtmlParser
import os
import uuid

class MarkdownParser(FileParser):

    def __init__(self, file_path:str = None):
        super().__init__(file_path=file_path)

    def parse(self, file_path: str) -> 'FileBaseProperty':
        with open(file_path, 'r', encoding='utf-8') as md_file:
            markdown_content = md_file.read()

        html_content = mistune.html(markdown_content)
        # 在同路径创建一个html文件
        html_file_path = os.path.splitext(file_path)[0] + f'_{uuid.uuid4()}.html'
        with open(html_file_path, 'w', encoding='utf-8') as html_file:
            html_file.write(html_content)

        try:
            return HtmlParser(file_path=html_file_path).parse(html_file_path)
        except Exception as e:
            print(f"Error parsing markdown file {file_path}: {e}")
            raise e
        finally:
            os.unlink(html_file_path)