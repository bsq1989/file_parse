from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.parser import FileParser

from core.utils import num_tokens_from_string


class TxtParser(FileParser):

    def __init__(self, file_path: str = None):
        super().__init__(file_path)

    def parse(self) -> FileBaseProperty:
        """Parse the text file and return its properties."""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        text_property = TextProperty(name=self.file_name)
        text_property.text_content = content
        text_property.content_type = ContentType.TEXT
        text_property.content_token_length = num_tokens_from_string(content)

        self.property.content_list.append(text_property)
        self.property.total_text_length = len(content)
        self.property.total_token_length = text_property.content_token_length

        return self.property