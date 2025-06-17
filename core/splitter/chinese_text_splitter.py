from langchain.text_splitter import CharacterTextSplitter
import re
from typing import List


class ChineseTextSplitter(CharacterTextSplitter):
    def __init__(self, pdf: bool = False, sentence_size: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.pdf = pdf
        self.sentence_size = sentence_size

    def split_text1(self, text: str) -> List[str]:
        if self.pdf:
            text = re.sub(r"\n{3,}", "\n", text)
            text = re.sub('\s', ' ', text)
            text = text.replace("\n\n", "")
        sent_sep_pattern = re.compile('([﹒﹔﹖﹗．。！？]["’”」』]{0,2}|(?=["‘“「『]{1,2}|$))')  # del ：；
        sent_list = []
        for ele in sent_sep_pattern.split(text):
            if sent_sep_pattern.match(ele) and sent_list:
                sent_list[-1] += ele
            elif ele:
                sent_list.append(ele)
        return sent_list

    # def split_text(self, text: str) -> List[str]:   ##此处需要进一步优化逻辑
    #     if self.pdf:
    #         text = re.sub(r"\n{3,}", r"\n", text)
    #         text = re.sub('\s', " ", text)
    #         text = re.sub("\n\n", "", text)

    #     text = re.sub(r'([;；.!?。！？\?])([^”’])', r"\1\n\2", text)  # 单字符断句符
    #     text = re.sub(r'(\.{6})([^"’”」』])', r"\1\n\2", text)  # 英文省略号
    #     text = re.sub(r'(\…{2})([^"’”」』])', r"\1\n\2", text)  # 中文省略号
    #     text = re.sub(r'([;；!?。！？\?]["’”」』]{0,2})([^;；!?，。！？\?])', r'\1\n\2', text)
    #     # 如果双引号前有终止符，那么双引号才是句子的终点，把分句符\n放到双引号后，注意前面的几句都小心保留了双引号
    #     text = text.rstrip()  # 段尾如果有多余的\n就去掉它
    #     # 很多规则中会考虑分号;，但是这里我把它忽略不计，破折号、英文双引号等同样忽略，需要的再做些简单调整即可。
    #     ls = [i for i in text.split("\n") if i]
    #     for ele in ls:
    #         if len(ele) > self.sentence_size:
    #             ele1 = re.sub(r'([,，.]["’”」』]{0,2})([^,，.])', r'\1\n\2', ele)
    #             ele1_ls = ele1.split("\n")
    #             for ele_ele1 in ele1_ls:
    #                 if len(ele_ele1) > self.sentence_size:
    #                     ele_ele2 = re.sub(r'([\n]{1,}| {2,}["’”」』]{0,2})([^\s])', r'\1\n\2', ele_ele1)
    #                     ele2_ls = ele_ele2.split("\n")
    #                     for ele_ele2 in ele2_ls:
    #                         if len(ele_ele2) > self.sentence_size:
    #                             ele_ele3 = re.sub('( ["’”」』]{0,2})([^ ])', r'\1\n\2', ele_ele2)
    #                             ele2_id = ele2_ls.index(ele_ele2)
    #                             ele2_ls = ele2_ls[:ele2_id] + [i for i in ele_ele3.split("\n") if i] + ele2_ls[
    #                                                                                                    ele2_id + 1:]
    #                     ele_id = ele1_ls.index(ele_ele1)
    #                     ele1_ls = ele1_ls[:ele_id] + [i for i in ele2_ls if i] + ele1_ls[ele_id + 1:]

    #             id = ls.index(ele)
    #             ls = ls[:id] + [i for i in ele1_ls if i] + ls[id + 1:]
    #     return ls
    def split_text(self, text: str) -> List[str]:
        # 检查是否处理 PDF 文本
        if self.pdf:
            text = re.sub(r"\n{3,}", "\n", text)  # 处理连续换行符
            text = re.sub(r"\s+", " ", text)  # 去除多余的空白
            text = re.sub(r"\n", " ", text)

        # Step 1: 使用正则表达式识别 IP地址
        # 替换IP地址为占位符
        ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
        ip_addresses = re.findall(ip_pattern, text)
        for i, ip in enumerate(ip_addresses):
            text = text.replace(ip, f"__IP{i}__", 1)  # 替换为占位符
        # Step 1.1: 使用正则表达式识别 URL 并替换为占位符
        url_pattern = r'\bhttps?://[^\s"’”」』]+'
        urls = re.findall(url_pattern, text)
        for i, url in enumerate(urls):
            # 使用非贪婪匹配，避免截断 URL
            text = text.replace(url, f"__URL{i}__", 1)  # 替换为占位符
        # Step 2: 使用正则表达式识别浮点数并替换为占位符
        float_pattern = r'(?<!\d)[+-]?(?:\d+\.\d+|\.\d+)(?!\d)'  # 匹配浮点数
        float_numbers = re.findall(float_pattern, text)
        for i, number in enumerate(float_numbers):
            text = text.replace(number, f"__FLOAT{i}__", 1)  # 替换为占位符

        # Step 3: 使用正则表达式识别章节引用并替换为占位符
        chapter_pattern = r'(?<!\d)\d+(?:\.\d+)+(?!\d)'  # 匹配章节格式
        chapter_references = re.findall(chapter_pattern, text)
        for i, chapter in enumerate(chapter_references):
            text = text.replace(chapter, f"__CHAPTER{i}__", 1)  # 替换为占位符 

        text = re.sub(r'([;；.!?。！？\?])([^”’])', r"\1\n\2", text)  # 单字符断句符
        text = re.sub(r'(\.{6})([^"’”」』])', r"\1\n\2", text)  # 英文省略号
        text = re.sub(r'(\…{2})([^"’”」』])', r"\1\n\2", text)  # 中文省略号
        text = re.sub(r'([;；!?。！？\?]["’”」』]{0,2})([^;；!?，。！？\?])', r'\1\n\2', text)
        # 如果双引号前有终止符，那么双引号才是句子的终点，把分句符\n放到双引号后，注意前面的几句都小心保留了双引号
        text = text.rstrip()  # 段尾如果有多余的\n就去掉它
        # 很多规则中会考虑分号;，但是这里我把它忽略不计，破折号、英文双引号等同样忽略，需要的再做些简单调整即可。
        ls = [i for i in text.split("\n") if i]
        for ele in ls:
            if len(ele) > self.sentence_size:
                ele1 = re.sub(r'([,，.]["’”」』]{0,2})([^,，.])', r'\1\n\2', ele)
                ele1_ls = ele1.split("\n")
                for ele_ele1 in ele1_ls:
                    if len(ele_ele1) > self.sentence_size:
                        ele_ele2 = re.sub(r'([\n]{1,}| {2,}["’”」』]{0,2})([^\s])', r'\1\n\2', ele_ele1)
                        ele2_ls = ele_ele2.split("\n")
                        for ele_ele2 in ele2_ls:
                            if len(ele_ele2) > self.sentence_size:
                                ele_ele3 = re.sub('( ["’”」』]{0,2})([^ ])', r'\1\n\2', ele_ele2)
                                ele2_id = ele2_ls.index(ele_ele2)
                                ele2_ls = ele2_ls[:ele2_id] + [i for i in ele_ele3.split("\n") if i] + ele2_ls[
                                                                                                       ele2_id + 1:]
                        ele_id = ele1_ls.index(ele_ele1)
                        ele1_ls = ele1_ls[:ele_id] + [i for i in ele2_ls if i] + ele1_ls[ele_id + 1:]

                id = ls.index(ele)
                ls = ls[:id] + [i for i in ele1_ls if i] + ls[id + 1:]

        # Step 7: 将占位符替换回原来的章节引用
        for i, chapter in enumerate(chapter_references):
            ls = [sentence.replace(f"__CHAPTER{i}__", chapter) for sentence in ls]

        # Step 6: 将占位符替换回原来的浮点数
        for i, number in enumerate(float_numbers):
            ls = [sentence.replace(f"__FLOAT{i}__", number) for sentence in ls]

        # Step 5: 将占位符替换回原来的 URL
        for i, url in enumerate(urls):
            ls = [sentence.replace(f"__URL{i}__", url) for sentence in ls]

        
        # Step 4: 将占位符替换回原来的 IP 地址
        for i, ip in enumerate(ip_addresses):
            ls = [sentence.replace(f"__IP{i}__", ip) for sentence in ls]

        
        return ls