from core.schema import ContentType, BaseProperty, TextProperty, TableProperty, ImageProperty, FileBaseProperty
from core.img_parser import ImgParser

def magic_ocr(path: str) ->list[BaseProperty]:
    """Performs OCR on the given file path and returns a list of BaseProperty objects."""
    parser = ImgParser(file_path=path)
    parser.parse(file_path=path)
    return parser.property.content_list