# Ingest B3 OHLCV to Parquet

Script para extrair dados diários (Open, High, Low, Close, Volume) de tickers da B3 usando `yfinance`, processar com `pandas`, adicionar a coluna `data_ingestao` (YYYY-MM-DD) e salvar localmente em formato Parquet.

## Requisitos

- Python 3.8+
- Dependências listadas em `requirements.txt` (yfinance, pandas, pyarrow)

## Instalação (PowerShell)

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; python -m pip install --upgrade pip; pip install -r requirements.txt
```

## Exemplo de execução (PowerShell)

```powershell
python .\scripts\ingest_b3.py --tickers PETR4.SA,VALE3.SA,^BVSP --out-dir .
```

## Saída

O arquivo Parquet será salvo em:

```
./raw/data_ingestao=YYYY-MM-DD/dados.parquet
```

## Observações

- Em caso de ausência de dados para os tickers fornecidos, o processo termina com código de erro e nenhuma saída é gerada.

## Configurar segredos (AWS)

Este projeto usa `python-dotenv` para carregar variáveis de ambiente de um arquivo `.env` local (não versionado). Copie `.env.template` para `.env` e preencha os valores:

```powershell
copy .env.template .env
# then edit .env in a text editor and add your AWS credentials/bucket
```

Variáveis esperadas no `.env`:

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_SESSION_TOKEN (opcional)
- AWS_REGION (opcional)
- S3_BUCKET_NAME (ex: parquet-files-870946032395)

## Exemplo de execução com upload para S3

```powershell
# opção A: pegar bucket e credenciais do .env
python .\scripts\ingest_b3.py --tickers PETR4.SA,VALE3.SA --s3-bucket parquet-files-870946032395

# opção B: passar credenciais/args explicitamente (não recomendado para produção)
python .\scripts\ingest_b3.py --tickers PETR4.SA,VALE3.SA --s3-bucket parquet-files-870946032395 --aws-access-key-id ABC --aws-secret-access-key XYZ --aws-region us-east-1
```
