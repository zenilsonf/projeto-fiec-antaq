
import requests
import zipfile
import io
import duckdb as db
import boto3
from typing import List
from botocore.exceptions import NoCredentialsError
from airflow.decorators import dag, task

URL_ORIGEM = 'https://web3.antaq.gov.br/ea/txt/'
ANOS = ['2023', '2024']

# Configuração do cliente MinIO
MINIO_ENDPOINT = "http://192.168.1.100:9000"  # Substitua pelo endpoint do seu MinIO
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "bronze"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def upload_to_minio(file_path: str, bucket_name: str, object_name: str) -> None:
    """Faz upload de um arquivo local para o MinIO."""
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Arquivo {file_path} enviado para o bucket {bucket_name} como {object_name}")
    except NoCredentialsError:
        print("Credenciais do MinIO não encontradas.")
    except Exception as e:
        print(f"Erro ao enviar arquivo para o MinIO: {e}")


def extract(anos: List[str], url: str) -> List[tuple]:
    dataframes = []
    for ano in anos:
        response = requests.get(f'{url}{ano}TemposAtracacao.zip')
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
            # Nomes esperados dentro do arquivo zip
            arquivo_tempo_atracacao = f'{ano}TemposAtracacao.txt'
            arquivo_tempo_atracacao_paralisacao = f'{ano}TemposAtracacaoParalisacao.txt'

            # Leitura dos arquivos
            with zip_ref.open(arquivo_tempo_atracacao) as file:
                df_tempo_atracacao = db.read_csv(file, sep=';', encoding='utf-8')

            with zip_ref.open(arquivo_tempo_atracacao_paralisacao) as file:
                df_tempo_atracacao_paralisacao = db.read_csv(file, sep=';', encoding='utf-8')

        # Print para verificar se os arquivos foram lidos corretamente
        print(df_tempo_atracacao.limit(5).to_df())
        print(df_tempo_atracacao_paralisacao.limit(5).to_df())

        dataframes.append((df_tempo_atracacao, df_tempo_atracacao_paralisacao, ano))

    return dataframes


def transform(df_tempo_atracacao, df_tempo_atracacao_paralisacao, ano: str, output_parquet_dir: str) -> None:

    # Caminhos dos arquivos Parquet
    output_file_tempo_atracacao = f'{output_parquet_dir}/{ano}_TemposAtracacao.parquet'
    output_file_tempo_atracacao_paralisacao = f'{output_parquet_dir}/{ano}_TemposAtracacaoParalisacao.parquet'
    # output_file_tempo_atracacao = f'/temp/{ano}_TemposAtracacao.parquet'
    # output_file_tempo_atracacao_paralisacao = f'/temp/{ano}_TemposAtracacaoParalisacao.parquet'

    # Transformações e salvamento em arquivos Parquet usando DuckDB
    db.query(f"COPY df_tempo_atracacao TO '{output_file_tempo_atracacao}' (FORMAT 'parquet')")
    db.query(f"COPY df_tempo_atracacao_paralisacao TO '{output_file_tempo_atracacao_paralisacao}' (FORMAT 'parquet')")

     # Upload dos arquivos para o MinIO
    upload_to_minio(output_file_tempo_atracacao, BUCKET_NAME, f'{ano}/TemposAtracacao.parquet')
    upload_to_minio(output_file_tempo_atracacao_paralisacao, BUCKET_NAME, f'{ano}/TemposAtracacaoParalisacao.parquet')

    # Mensagem de sucesso
    # print(f'Arquivos Parquet salvos em {output_parquet_dir}')
    print(f'Arquivos Parquet do ano {ano} enviados para o bucket {BUCKET_NAME}')

def etl_anatq_v2():
    dataframes = extract(ANOS, URL_ORIGEM)
    for df_tempo_atracacao, df_tempo_atracacao_paralisacao, ano in dataframes:
        transform(df_tempo_atracacao, df_tempo_atracacao_paralisacao, ano, '/tmp')
    load()


def load() -> None:
    # Função de carga
    print('Função de carga executada com sucesso')


etl_anatq_v2()