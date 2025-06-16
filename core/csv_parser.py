from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser

from core.utils import num_tokens_from_string


class CsvParser(FileParser):
    """CSV文件解析器"""

    def __init__(self, file_path: str = None):
        super().__init__(file_path)

    def parse(self) -> FileBaseProperty:
        """解析CSV文件并返回其属性"""
        import csv

        with open(self.file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            rows = list(reader)

        if not rows:
            raise ValueError("CSV文件内容为空")

        # 获取表头
        headers = rows[0]
        data_rows = rows[1:]

        # 生成HTML内容
        html_content = "<table border='1'>"
        html_content += "<tr>" + "".join(f"<th>{header}</th>" for header in headers) + "</tr>"
        for row in data_rows:
            html_content += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        html_content += "</table>"

        # 生成纯文本内容
        pure_text = "\n".join([", ".join(row) for row in rows])

        table_property = TableProperty(name=self.file_name)
        table_property.html_content = html_content
        table_property.text_content = pure_text.strip()
        table_property.table_row_count = len(data_rows)
        table_property.table_column_count = len(headers)
        table_property.content_token_length = num_tokens_from_string(pure_text.strip())

        self.property.content_list.append(table_property)
        self.property.total_text_length = len(pure_text)
        self.property.total_token_length = table_property.content_token_length

        return self.property