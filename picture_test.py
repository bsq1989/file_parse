import os

from magic_pdf.data.data_reader_writer import FileBasedDataWriter
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.data.read_api import read_local_images

# prepare env
local_image_dir, local_md_dir = "ocr_test/images", "ocr_test"
os.makedirs(local_image_dir, exist_ok=True)

image_dir = str(os.path.basename(local_image_dir))

image_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(
    local_md_dir
)

# proc
## Create Dataset Instance
input_file = "pic_test4.jpg"       # replace with real image file

input_file_name = input_file.split(".")[0]
ds = read_local_images(input_file)[0]

ds.apply(doc_analyze, ocr=True).pipe_ocr_mode(image_writer).dump_content_list(
    md_writer, f"{input_file_name}.json", image_dir
)