import xml.etree.ElementTree as ET
from lxml import etree

from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser

from core.utils import num_tokens_from_string
import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import uuid
import base64
from core.ocr_utils import magic_ocr


class HtmlParser(FileParser):

    def __init__(self, file_path: str = None):
        super().__init__(file_path=file_path)
        self.cache_dir = f'/tmp/html_parser_cache/{uuid.uuid4()}'

    def parse(self, file_path: str) -> 'FileBaseProperty':
        """Parse HTML file and extract properties."""
        self.property.file_path = file_path
        self.property.file_name = file_path.split('/')[-1]
        
        # 读取 HTML 文件
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        if content:
            # 使用 BeautifulSoup 解析 HTML 内容
            soup = BeautifulSoup(content, "lxml")
            
            body  = soup.body

            if body:
                
                for elem in body.descendants:
                    if elem.name:
                        tag_name = elem.name
                        if tag_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            # 处理标题标签
                            text_content = elem.get_text(strip=True)
                            if text_content:
                                level = int(tag_name[1])
                                text_property = TextProperty(f'Heading {level}')
                                text_property.text_content = text_content
                                text_property.text_level = level
                                text_property.content_token_length = num_tokens_from_string(text_content)
                                self.property.content_list.append(text_property)
                                self.property.total_text_length += len(text_content)
                                self.property.total_token_length += text_property.content_token_length
                        elif tag_name == 'p':
                            # 处理段落标签
                            text_content = elem.get_text(strip=True)
                            if text_content:
                                text_property = TextProperty('Paragraph')
                                text_property.text_content = text_content
                                text_property.text_level = 0
                                text_property.content_token_length = num_tokens_from_string(text_content)
                                self.property.content_list.append(text_property)
                                self.property.total_text_length += len(text_content)
                                self.property.total_token_length += text_property.content_token_length
                        elif tag_name == 'table':
                            # 处理表格标签
                            rows = elem.find_all('tr')
                            if rows:
                                table_html = str(elem)
                                table_row_count = len(rows)
                                table_column_count = max(len(row.find_all(['td', 'th'])) for row in rows)
                                
                                table_property = TableProperty('Table')
                                table_property.html_content = table_html
                                table_property.table_row_count = table_row_count
                                table_property.table_column_count = table_column_count
                                
                                # 提取表格文本内容
                                total_text = ' '.join(cell.get_text(strip=True) for row in rows for cell in row.find_all(['td', 'th']))
                                table_property.text_content = total_text
                                table_property.content_token_length = num_tokens_from_string(total_text)
                                self.property.total_text_length += len(total_text)
                                self.property.total_token_length += table_property.content_token_length
                                self.property.content_list.append(table_property)
                        elif tag_name == 'img':
                            # 处理图片标签
                            html_file_folder = os.path.dirname(file_path)
                            img_src = elem.get('src')
                            if img_src:
                                # 处理相对路径
                                # 默认存储在本地相对路径
                                image_id = uuid.uuid4()
                                image_property = ImageProperty(f'Image {image_id}')
                                image_property.image_height = 0
                                image_property.image_width = 0
                                if img_src.startswith('data'):
                                    img_data = img_src.split(',')[1]
                                    image_property.blob_data = base64.b64decode(img_data)
                                    image_property.image_format = 'png'  # 默认格式
                                    image_property.image_path = f'{image_id}.png'
                                    cache_file_path = os.path.join(self.cache_dir, image_property.image_path)
                                    os.makedirs(self.cache_dir, exist_ok=True)
                                    with open(cache_file_path, 'wb') as f:
                                        f.write(image_property.blob_data)
                                    print(f"Processing image from base64 data: {cache_file_path}")
                                    image_property.ocr_content_list = magic_ocr(cache_file_path)
                                else:
                                    img_path = os.path.join(html_file_folder, img_src)

                                    image_property.image_format = img_src.split('.')[-1]  # e.g., 'png', 'jpeg'
                                    if os.path.exists(img_path):
                                        with open(img_path, 'rb') as f:
                                            image_property.blob_data = f.read()
                                        image_property.image_path = img_path
                                        print(f"Processing image: {img_path}")
                                        if image_property.image_format.lower() not in ['png', 'jpg', 'jpeg', 'gif', 'bmp']:
                                            print(f"Unsupported image format: {image_property.image_format}")
                                            image_property.blob_data = None
                                            image_property.image_path = img_src
                                        else:
                                            image_property.ocr_content_list = magic_ocr(img_path)
                                        
                                    else:
                                        image_property.blob_data = None
                                        image_property.image_path = img_src  # 保留原始路径
                                
                                self.property.content_list.append(image_property)
                        elif tag_name == 'div':
                            if not elem.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'table', 'img']):
                                # 处理没有子元素的 div
                                spans = elem.find_all('span')
                                if spans:
                                    total_text = ' '.join(span.get_text(strip=True) for span in spans)
                                    if len(total_text) > 0:
                                        text_property = TextProperty('Div Text')
                                        text_property.text_content = total_text
                                        text_property.text_level = 0
                                        text_property.content_token_length = num_tokens_from_string(total_text)
                                        self.property.total_text_length += len(total_text)
                                        self.property.total_token_length += text_property.content_token_length
                                        self.property.content_list.append(text_property)

        return self.property