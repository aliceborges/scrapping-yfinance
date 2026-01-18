FROM public.ecr.aws/lambda/python:3.10

# Instala ferramentas de sistema necessárias para compilação
RUN yum install -y gcc-c++ pkgconfig cmake

# Atualiza ferramentas de build do Python
RUN pip install --upgrade pip setuptools wheel

# Copia a definição do projeto
COPY pyproject.toml .

# Instala dependências (o cmake agora permitirá que o pyarrow compile se necessário)
RUN pip install . --no-cache-dir

# Copia o script para a raiz da Lambda
COPY ingest_b3.py ${LAMBDA_TASK_ROOT}

# Define o Handler conforme seu código
CMD [ "ingest_b3.lambda_handler" ]