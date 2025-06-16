from core import * 

import os

if __name__ == "__main__":
    # Example usage
    # test docx
    if 0:
        file_path = "/home/ubuntu/app/file_parse/test.docx"  # Replace with your actual file path
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content,content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print('ocr _content_list =============================')
                    for  ocr_content in content.ocr_content_list:
                        print(ocr_content.text_content, ocr_content.content_token_length)
                        print(type(ocr_content))
                    print("ocr _content_list end =============================")
        else:
            print("No suitable parser found for the given file type.")
    if 0:
        file_dir = '/home/ubuntu/app/file_parse'
        file_name = '成方金信-信息系统网络安全事件定级标准.xlsx'
        file_path = os.path.join(file_dir, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content,content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
        else:
            print("No suitable parser found for the given file type.")
    if 0:
        folder = '/home/ubuntu/app/file_parse'
        file_name = '智能运维在成方金信的应用情况-20250112.pptx'
        file_path = os.path.join(folder, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content,content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print('ocr _content_list =============================')
                    for ocr_content in content.ocr_content_list:
                        print(ocr_content.text_content, ocr_content.content_token_length)
                        print(type(ocr_content))
                    print("ocr _content_list end =============================")
        else:
            print("No suitable parser found for the given file type.")

    if 1:
        folder = '/home/ubuntu/app/file_parse/html_test'
        file_name = '输出文件格式介绍 — MinerU 1.3.12 文档.html'
        file_path = os.path.join(folder, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content, content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print('ocr _content_list =============================')
                    for ocr_content in content.ocr_content_list:
                        print(ocr_content.text_content, ocr_content.content_token_length)
                        print(type(ocr_content))
                    print("ocr _content_list end =============================")
        else:
            print("No suitable parser found for the given file type.")

    if 0:
        folder = '/home/ubuntu/app/file_parse/test/test/txt'
        file_name = 'test.md'
        file_path = os.path.join(folder, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content, content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print(content.image_path)
        else:
            print("No suitable parser found for the given file type.")
    
    if 0:
        folder = '/home/ubuntu/app/file_parse/'
        file_name = 'test.pdf'
        file_path = os.path.join(folder, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content, content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print(content.image_path)
        
        else:
            print("No suitable parser found for the given file type.")
    
    if 0:
        folder = '/home/ubuntu/app/file_parse/'
        file_name = 'pic_test4.jpg'
        file_path = os.path.join(folder, file_name)
        parser = create_parser(file_path)
        if parser:
            parsed_content = parser.parse(file_path)
            # print base properties
            print(parsed_content.page_count, parsed_content.file_size, parsed_content.file_type)
            print(parsed_content.md5_hash)
            print(parsed_content.total_text_length, parsed_content.total_token_length)
            for content in parsed_content.content_list:
                print(type(content))
                print('====================================================')
                print(content.name)
                print(content.content_type, content.text_content, content.content_token_length)
                if (type(content) == TableProperty):
                    print(content.html_content, content.table_row_count, content.table_column_count)
                elif (type(content) == TextProperty):
                    print(content.text_level)
                elif (type(content) == ImageProperty):
                    print(content.image_path)
        else:
            print("No suitable parser found for the given file type.")