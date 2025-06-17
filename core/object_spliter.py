from core.splitter import ChineseTextSplitter, zh_title_enhance
from core.schema import TextProperty,TableProperty,ImageProperty
from langchain_community.docstore.document import Document
from bs4 import BeautifulSoup


def text_spliter(txt:TextProperty,sentence_size=100):
    if txt.text_level != 0:
        if len(txt.text_content):
            doc_tmp = Document(txt.text_content)
            doc_tmp.metadata['html_content'] = f'<h{txt.text_level}>{txt.text_content}</h{txt.text_level}>'
            doc_tmp.metadata['type'] = 'Title'
            return [doc_tmp]
    else:
        if len(txt.text_content):
            # Split the text into smaller chunks
            text_splitter = ChineseTextSplitter(False, sentence_size=sentence_size)
            chunks = text_splitter.split_text(txt.text_content)
            # append html content to metadata
            doc_tmp = [Document(chunk) for chunk in chunks]
            for i, chunk in enumerate(doc_tmp):
                chunk.metadata['html_content'] =  f'<p>{chunk.page_content}</p>'
                chunk.metadata['type'] = 'Text'
            return doc_tmp
    return []


def table_spliter(tbl:TableProperty):
    html_content = tbl.html_content
    soup = BeautifulSoup(html_content, "lxml")
    table = soup.find('table')
    if not table:
        return []
    rows = table.find_all('tr')
    if not rows:
        return []
    table_data = []
    for row in rows:
        cells = row.find_all(['td', 'th'])
        # Extract text from each cell
        cell_texts = [cell.get_text(strip=True) for cell in cells]
        # filter out empty cells, if len(text) == 0
        cell_texts = [text for text in cell_texts if len(text) > 0]
        if len(cell_texts) > 0:
            # Join the cell texts with a separator (e.g., comma)
            row_text = ', '.join(cell_texts)
            # Create a Document for each row
            doc = Document(row_text)
            doc.metadata['html_content'] = str(row)
            doc.metadata['type'] = 'TableRow'
            table_data.append(doc)

    return table_data

def image_spliter(img:ImageProperty):
    if len(img.ocr_content_list):
        content = ''
        html_content = ''
        for ocr_content in img.ocr_content_list:
            if isinstance(ocr_content, TextProperty):
                content += ocr_content.text_content + '\n'
                html_content += f'<p>{ocr_content.text_content}</p>'
            elif isinstance(ocr_content, TableProperty):
                content += ocr_content.text_content + '\n'
                html_content += ocr_content.html_content
            else:
                pass
        if len(content.strip()) > 0:
            doc =  Document(content.strip())
            doc.metadata['html_content'] = html_content
            doc.metadata['type'] = 'ImageOCR'
            return [doc]
    return []