FROM mineru:1.2
RUN pip install --upgrade pip setuptools wheel
RUN pip install requests python-magic minio psycopg2-binary python-docx html2text beautifulsoup4 python-pptx openpyxl pymupdf pymupdf4llm mistune==3.1.2 -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U "celery[redis]" -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U pymilvus -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install openai -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install tiktoken -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install bpython -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -U ipython -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install langchain -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install langchain-community -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install openai -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip uninstall numpy -y
RUN pip install numpy==1.26.4 -i https://pypi.tuna.tsinghua.edu.cn/simple
WORKDIR /home/ubuntu/app/
RUN mkdir -p /home/ubuntu/app/models
COPY ./cl100k_base.tiktoken /home/ubuntu/app/models/223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7


