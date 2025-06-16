import os

from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser
import uuid
import json
from core.utils import num_tokens_from_string
from bs4 import BeautifulSoup
from PIL import Image



class PdfParser(FileParser):

    def __init__(self, file_path = None):
        super().__init__(file_path)

    def parse(self, file_path):
        task_id = uuid.uuid4()
        # prepare env
        local_image_dir, local_md_dir = f"/tmp/pdf_parse_cache/{task_id}/images", f"/tmp/pdf_parse_cache/{task_id}"
        image_dir = str(os.path.basename(local_image_dir))
        pdf_file_name = os.path.basename(file_path)
        name_without_suff = pdf_file_name.split(".")[0]
        os.makedirs(local_image_dir, exist_ok=True)
        image_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(
            local_md_dir
        )
        image_dir = str(os.path.basename(local_image_dir))

        # read bytes
        reader1 = FileBasedDataReader("")
        pdf_bytes = reader1.read(file_path)  # read the pdf content
        # proc
        ## Create Dataset Instance
        ds = PymuDocDataset(pdf_bytes)

        ## inference
        if ds.classify() == SupportedPdfParseMethod.OCR:
            infer_result = ds.apply(doc_analyze, ocr=True)

            ## pipeline
            pipe_result = infer_result.pipe_ocr_mode(image_writer)

        else:
            infer_result = ds.apply(doc_analyze, ocr=False)

            ## pipeline
            pipe_result = infer_result.pipe_txt_mode(image_writer)

        # ### draw model result on each page
        # infer_result.draw_model(os.path.join(local_md_dir, f"{name_without_suff}_model.pdf"))
# 
        # ### draw layout result on each page
        # pipe_result.draw_layout(os.path.join(local_md_dir, f"{name_without_suff}_layout.pdf"))
# 
        # ### draw spans result on each page
        # pipe_result.draw_span(os.path.join(local_md_dir, f"{name_without_suff}_spans.pdf"))
# 
        # ### dump markdown
        # pipe_result.dump_md(md_writer, f"{name_without_suff}.md", image_dir)

        ### dump content list
        pipe_result.dump_content_list(md_writer, f"{name_without_suff}_content_list.json", image_dir)

        with open(os.path.join(local_md_dir, f"{name_without_suff}_content_list.json"), 'r') as f:
            content_list = json.load(f)
            text_index = 0
            table_index = 0
            image_index = 0
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
                            image_property.image_data = f.read()
                    image_property.image_format = 'RGB'
                    self.property.content_list.append(image_property)
                    image_index += 1
        return self.property







