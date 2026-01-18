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
