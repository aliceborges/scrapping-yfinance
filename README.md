# Ingest B3 OHLCV to Parquet

Script para extrair dados diários (Open, High, Low, Close, Volume) de tickers da B3 usando `yfinance`, processar com `pandas`, adicionar a coluna `data_ingestao` (YYYY-MM-DD) e salvar localmente em formato Parquet ou fazer upload para um bucket S3.

## Visão geral

- Código principal: `ingest_b3.py` (na raiz do repositório).
- Objetivo: baixar séries diárias OHLCV de um ou mais tickers, padronizar colunas, adicionar `data_ingestao` e gerar um arquivo Parquet compatível com AWS Glue.
- Formato de saída (local ou em S3): `raw/data_ingestao=YYYY-MM-DD/file.parquet`.

## Requisitos

- Python 3.8+
- Dependências listadas em `requirements.txt` (ex.: `yfinance`, `pandas`, `boto3`, `fastparquet`).

## Instalação (PowerShell)

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; python -m pip install --upgrade pip; pip install -r requirements.txt
```

## Uso local (linha de comando)

Execute o script principal diretamente (arquivo `ingest_b3.py` na raiz):

```powershell
# Exemplo básico (salva localmente em ./raw/data_ingestao=YYYY-MM-DD/file.parquet)
python .\ingest_b3.py --tickers PETR4.SA,VALE3.SA --out-dir .

# Especificando data de ingestão (YYYY-MM-DD) e intervalo/período do yfinance
python .\ingest_b3.py --tickers PETR4.SA --date 2025-12-31 --period 1mo --interval 1d --out-dir .
```

Observações:
- `--out-dir` é a base onde será criada a pasta `raw/data_ingestao=YYYY-MM-DD/` contendo `file.parquet` (padrão: `.`).
- Se não houver dados baixados para os tickers fornecidos, a função `run_ingest` retorna um `statusCode` 204 e nenhum arquivo será gerado.

## Upload direto para S3

O script pode enviar o arquivo Parquet para um bucket S3. Há duas opções para informar o bucket/credenciais:

1. Passar `--s3-bucket` na linha de comando (o script também utiliza as variáveis de ambiente abaixo se não for informado).
2. Não passar `--s3-bucket` e configurar `S3_BUCKET_NAME` no ambiente (ou no `.env` quando usado localmente).

Exemplo (PowerShell):

```powershell
# Usando bucket configurado via argumento
python .\ingest_b3.py --tickers PETR4.SA,VALE3.SA --s3-bucket my-parquet-bucket

# Forçando região
python .\ingest_b3.py --tickers PETR4.SA --s3-bucket my-parquet-bucket --aws-region us-east-1
```

Se `--s3-bucket` não for passado, o script tentará ler `S3_BUCKET_NAME` do ambiente (ver seção `Variáveis de ambiente` abaixo).

O arquivo será escrito em: `s3://<bucket>/raw/data_ingestao=YYYY-MM-DD/file.parquet`.

O upload realiza um teste simples de escrita (put/delete de um objeto pequeno) antes de enviar o arquivo principal para detectar falta de permissão.

## Variáveis de ambiente e `.env`

O projeto usa `boto3` e pode ser executado localmente com credenciais AWS no ambiente. É comum usar `python-dotenv` em projetos semelhantes, mas este repositório não exige automaticamente o carregamento de `.env` — defina as variáveis no seu ambiente ou adapte o carregamento se desejar.

Variáveis úteis:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_SESSION_TOKEN (opcional)
- AWS_REGION (opcional)
- S3_BUCKET_NAME (nome do bucket padrão, ex: `parquet-files-123456789`)

Exemplo rápido (PowerShell) para definir variáveis temporárias na sessão atual:

```powershell
$env:AWS_ACCESS_KEY_ID = 'AKIA...'
$env:AWS_SECRET_ACCESS_KEY = '...'
$env:S3_BUCKET_NAME = 'my-parquet-bucket'
```

## Uso como AWS Lambda

O módulo inclui o handler `lambda_handler(event, context)` para implantação em Lambda. O `event` pode conter as chaves abaixo (todas opcionais, exceto `tickers` que é recomendada):

- `tickers`: string com vírgula-separada (ex: `"PETR4.SA,VALE3.SA"`) ou lista de strings
- `date` ou `ingest_date`: `YYYY-MM-DD`
- `period`: período do yfinance (ex: `1mo`)
- `interval`: intervalo do yfinance (ex: `1d`)
- `s3_bucket` ou `bucket`: nome do bucket para upload
- `aws_region` ou `region`: região AWS

Exemplo de payload para invocar a Lambda localmente ou em testes:

```json
{
  "tickers": "PETR4.SA,VALE3.SA",
  "date": "2025-12-31",
  "s3_bucket": "my-parquet-bucket"
}
```

O handler retorna um dicionário com `statusCode` e `body`, onde `body` pode conter `s3_uri` em caso de upload bem-sucedido.

## Formato e compatibilidade Parquet

- O script converte a coluna `date` para um inteiro (timestamp em segundos) antes de escrever o Parquet para garantir compatibilidade com alguns catálogos/Glue setups.
- Colunas garantidas: `ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `data_ingestao`.

## Observações e troubleshooting

- Se o `yfinance` não retornar dados (ex.: ticker inválido ou período sem negociações), nenhum arquivo será gerado.
- Ao usar S3, garanta que as credenciais/role possuem permissão `s3:PutObject` e `s3:DeleteObject` (o script testa essas permissões).
- O script assume que a data de ingestão está em `YYYY-MM-DD` quando passada manualmente via `--date` ou `date` no evento da Lambda.

## Estrutura do repositório (resumido)

- `ingest_b3.py` — script principal e handler Lambda
- `requirements.txt` — dependências Python
- `B3_ETL_Refine_Job/` — job de exemplo (não obrigatório para a execução principal)
- `s3_trigger/` — exemplo de função Lambda adicional