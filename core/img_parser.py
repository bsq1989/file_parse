import os
from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser
import uuid
import json
from core.utils import num_tokens_from_string
from bs4 import BeautifulSoup
from PIL import Image
import json

from magic_pdf.data.data_reader_writer import FileBasedDataWriter
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.data.read_api import read_local_images


class ImgParser(FileParser):

    def __init__(self, file_path=None):
        super().__init__(file_path)

    def parse(self, file_path):
        task_id = uuid.uuid4()
        # prepare env
        local_image_dir, local_md_dir = f"/tmp/pdf_parse_cache/{task_id}/images", f"/tmp/pdf_parse_cache/{task_id}"
        image_dir = str(os.path.basename(local_image_dir))
        image_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(local_md_dir)
        input_file_name = file_path.split(".")[0]
        ds = read_local_images(file_path)[0]

        ds.apply(doc_analyze, ocr=True).pipe_ocr_mode(image_writer).dump_content_list(
            md_writer, f"{input_file_name}.json", image_dir
        )
        with open(os.path.join(local_md_dir, f"{input_file_name}.json"), 'r') as f:
            text_index = 0
            table_index = 0
            image_index = 0
            content_list = json.load(f)
            for item in content_list:
                if item['type'] == 'text':
                    text_property = TextProperty(name=item['type'] + f'{text_index}')
                    text_property.page_idx = item['page_idx']
                    text_property.text_content = item['text']
                    if 'text_level' in item:
                        text_property.text_level = item['text_level']
                    self.property.total_text_length += len(item['text'])
                    self.property.total_token_length += num_tokens_from_string(item['text'])
                    self.property.content_list.append(text_property)
                    text_index += 1
                elif item['type'] == 'table':
                    table_property = TableProperty(name=item['type'] + f'{table_index}' + 
                                                   '_'.join(item['table_caption']))
                    table_property.page_idx = item['page_idx']
                    table_property.html_content = item['table_body']
                    soup = BeautifulSoup(item['table_body'], 'html.parser')
                    table = soup.find('table')
                    if table:
                        rows = table.find_all('tr')
                        table_property.table_row_count = len(rows)
                        if rows:
                            # Count columns from the first row
                            first_row_cells = rows[0].find_all(['td', 'th'])
                            table_property.table_column_count = len(first_row_cells)
                        else:
                            table_property.table_column_count = 0
                    else:
                        table_property.table_row_count = 0
                        table_property.table_column_count = 0
                    table_property.text_content = soup.get_text(separator=' ', strip=True)
                    table_property.content_token_length = num_tokens_from_string(table_property.text_content)
                    self.property.total_text_length += len(table_property.text_content)
                    self.property.total_token_length += table_property.content_token_length
                    self.property.content_list.append(table_property)
                    table_index += 1
                elif item['type'] == 'image':
                    image_property = ImageProperty(name=item['type'] + f'{image_index}' + 
                                                   '_'.join(item['img_caption']))
                    image_property.page_idx = item['page_idx']
                    image_property.image_path = os.path.join(local_md_dir , item['img_path'])
                    with Image.open(image_property.image_path) as img:
                        image_property.image_width, image_property.image_height = img.size
                        img.load()
                        with open(image_property.image_path, 'rb') as f:
                            image_property.blob_data = f.read()
                    image_property.image_format = 'RGB'
                    self.property.content_list.append(image_property)
                    image_index += 1
        return self.property