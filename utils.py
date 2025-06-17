import requests
import os
import magic

def submit_convert_task(file_path, url):

    # 查询参数
    params = {
        'keep_local': 'false'
    }

    # 请求头
    headers = {
        'accept': 'application/json'
    }

    file_name = os.path.basename(file_path)
    # 准备文件
    with open(file_path, 'rb') as f:
        files = {
            'file': (file_name, f)
        }
        
        # 发送POST请求
        response = requests.post(
            url,
            params=params,
            headers=headers,
            files=files
        )
        if response.status_code == 200:
            print("文件转换成功")
            return response.json()
        else:
            return None

def get_convert_task_status(url):
    # 请求头
    headers = {
        'accept': 'application/json'
    }

    # 发送GET请求
    response = requests.get(
        url,
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        return None
    

def identify_office_file(file_path):
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    
    # print(f"文件: {os.path.basename(file_path)}")
    # print(f"MIME 类型: {file_type}")

    # 解释文件类型
    if "msword" in file_type:
        return "doc"
    elif "vnd.openxmlformats-officedocument.wordprocessingml" in file_type:
        return "docx"
    elif "vnd.ms-excel" in file_type:
        return "xls"
    elif "vnd.openxmlformats-officedocument.spreadsheetml" in file_type:
        return "xlsx"
    elif "vnd.ms-powerpoint" in file_type:
        return "ppt"
    elif "vnd.openxmlformats-officedocument.presentationml" in file_type:
        return "pptx"
    else:
        return None