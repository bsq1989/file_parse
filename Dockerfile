FROM mineru:1.2
RUN pip install --upgrade pip setuptools wheel
RUN pip install requests python-magic minio psycopg2-binary python-docx html2text beautifulsoup4 python-pptx openpyxl pymupdf pymupdf4llm mistune==3.1.2 -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U "celery[redis]" -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U pymilvus -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install openai -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install tiktoken -i https://pypi.tuna.tsinghua.edu.cn/simple
WORKDIR /app
RUN pip install bpython -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U ipython -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install langchain -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install langchain-community -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install openai -i https://pypi.tuna.tsinghua.edu.cn/simple