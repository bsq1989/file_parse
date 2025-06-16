from docx import Document
from docx.text.paragraph import Paragraph
from docx.table import Table


from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser
import uuid
from io import StringIO
import html
from bs4 import BeautifulSoup
from core.utils import num_tokens_from_string
from core.ocr_utils import magic_ocr
import os

class DocxParser(FileParser):

    def __init__(self, file_path: str = None):
        super().__init__(file_path=file_path)
        self.cache_dir = f'/tmp/doc_parser_cache/{uuid.uuid4()}'


    """Parser for DOCX files."""
    def is_horizontally_merged(self,cell):
        """判断是否为横向合并的单元格"""
        tc = cell._tc
        grid_span = tc.get_or_add_tcPr().gridSpan
        return grid_span is not None and int(grid_span.val) > 1

    def get_colspan(self, cell):
        """获取横向合并的列跨度"""
        tc = cell._tc
        tc_pr = tc.get_or_add_tcPr()
        grid_span = tc.get_or_add_tcPr().gridSpan
        return int(grid_span.val) if grid_span is not None else 1

    def is_vertically_merged(self, cell):
        """判断是否为纵向合并的单元格"""
        tc = cell._tc
        vMerge = tc.get_or_add_tcPr().vMerge
        return vMerge is not None and (vMerge.val is None or vMerge.val == 'continue')

    def get_rowspan(self, table, row_idx, col_idx):
        """计算纵向合并的行跨度"""
        rowspan = 1
        for r in range(row_idx + 1, len(table.rows)):
            cell = table.cell(r, col_idx)
            if self.is_vertically_merged(cell):
                rowspan += 1
            else:
                break
        return rowspan

    def parse_paragraph(self, para: Paragraph, doc_obj) -> BaseProperty:
        """Parse a docx paragraph and return its properties."""
        if 'graphicData' in para._p.xml:
            xml_content = para._element.xml
            if xml_content.find('r:embed') != -1:
                left = xml_content.find('r:embed="')
                right = xml_content.find('"', left + len('r:embed="'))

                pic_id = xml_content[left + len('r:embed="'):right]
                image_part = doc_obj.part.related_parts[pic_id]

                image_property = ImageProperty(para.text)
                image_property.blob_data = image_part._blob
                
                image_property.image_format = image_part.content_type.split('/')[1]  # e.g., 'png', 'jpeg'
                image_property.image_path = f'image_{uuid.uuid4()}.{image_property.image_format}'
                
                cache_file_path = f'{self.cache_dir}/{image_property.image_path}'
                os.makedirs(self.cache_dir, exist_ok=True)
                with open(cache_file_path, 'wb') as f:
                    f.write(image_property.blob_data)
                
                magic_ocr(cache_file_path)
                image_property.ocr_content_list = magic_ocr(cache_file_path)
                # currently, we cannot get the image width and height from docx directly
                # so we set them to 0
                image_property.image_width = 0
                image_property.image_height = 0
                return image_property
            else:
                return TextProperty(para.text)
        elif para.style.name.startswith('Heading'):
             heading_level = int(para.style.name.split(' ')[1])
             text_property = TextProperty('Heading ' + str(heading_level))
             text_property.text_level = heading_level
             text_property.text_content = para.text
             text_property.content_token_length = num_tokens_from_string(text_property.text_content)
             return text_property
        else:
            text_property = TextProperty('Plain text')
            text_property.text_content = para.text
            text_property.content_token_length = num_tokens_from_string(text_property.text_content)
            return text_property

    def table_to_html(self, table):

        """将 python-docx 的 table 对象转换为 HTML"""
        output = StringIO()
        # output.write("\n<table class=\"table table-striped table-bordered\">")
        output.write("\n<table>")
        merged_cells = set()  # 记录已经处理过的合并单元格
        total_text = ''
        for row_idx, row in enumerate(table.rows):
            output.write("<tr>")
            
            for col_idx, cell in enumerate(row.cells):
                if (row_idx, col_idx) in merged_cells:
                    continue  # 跳过已经合并处理过的单元格

                colspan = self.get_colspan(cell)
                rowspan = self.get_rowspan(table, row_idx, col_idx) if self.is_vertically_merged(cell) else 1

                # 生成单元格的 HTML
                cell_html = f"<td"
                if colspan > 1:
                    cell_html += f" colspan='{colspan}'"
                if rowspan > 1:
                    cell_html += f" rowspan='{rowspan}'"
                cell_html += f">{html.escape(cell.text.strip())}</td>"
                output.write(cell_html)
                total_text += cell.text.strip() + ' '
    
                # 标记合并单元格以避免重复处理
                for r in range(row_idx, row_idx + rowspan):
                    for c in range(col_idx, col_idx + colspan):
                        merged_cells.add((r, c))
    
            output.write("</tr>")
        
        output.write("</table>\n")
        return output.getvalue(), total_text

    def parse(self, file_path: str) -> 'FileBaseProperty':
        """Parse the DOCX file and return its properties."""
        doc = Document(file_path)
        body = doc.element.body
        for i , element in enumerate(body):
            if element.tag.endswith('p'):
                para = Paragraph(element, doc)
                prop = self.parse_paragraph(para, doc)
                self.property.content_list.append(prop)
                self.property.total_text_length += len(prop.text_content)
                self.property.total_token_length += prop.content_token_length
            elif element.tag.endswith('tbl'):
                table_obj = Table(element, doc)
                table_property = TableProperty(f'Table_{i}')
                table_property.html_content, table_property.text_content = self.table_to_html(table_obj)
                table_property.table_row_count = len(table_obj.rows)
                table_property.table_column_count = len(table_obj.columns)
                table_property.content_token_length = num_tokens_from_string(table_property.text_content)
                self.property.total_text_length += len(table_property.text_content)
                self.property.total_token_length += table_property.content_token_length
                self.property.content_list.append(table_property)
            else:
                pass

        return self.property