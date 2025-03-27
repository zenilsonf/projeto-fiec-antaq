from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from minio import Minio
from minio.error import S3Error
import csv
import io
import pendulum

# Configurações do MinIO
MINIO_URL = "172.26.0.4:9000"  # Remove "http://" prefix
MINIO_ACCESS_KEY = "minioadmin"  # Substitua pela sua chave de acesso
MINIO_SECRET_KEY = "minioadmin"  # Substitua pela sua chave secreta
BUCKET_NAME = "vendas"

# Dados de vendas para o CSV
SALES_DATA = [
    ["id", "produto", "quantidade", "preco"],
    [1, "Produto A", 10, 100.0],
    [2, "Produto B", 5, 50.0],
    [3, "Produto C", 20, 200.0],
]

@dag(schedule=None, start_date=pendulum.today('UTC').add(days=-1), catchup=False, tags=["minio", "teste"])
def teste_comunicacao_minio():
    @task
    def verificar_e_criar_bucket():
        """Verifica se o bucket existe e o cria se necessário."""
        client = Minio(
            MINIO_URL,  # No "http://"
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,  # Use False for HTTP, True for HTTPS
        )
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            return f"Bucket '{BUCKET_NAME}' criado."
        return f"Bucket '{BUCKET_NAME}' já existe."

    @task
    def salvar_csv_no_bucket():
        """Salva um arquivo CSV com dados de vendas no bucket."""
        client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        # Cria o arquivo CSV em memória
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(SALES_DATA)
        csv_buffer.seek(0)

        # Salva o arquivo no bucket
        client.put_object(
            BUCKET_NAME,
            "dados_vendas.csv",
            data=io.BytesIO(csv_buffer.getvalue().encode("utf-8")),
            length=len(csv_buffer.getvalue()),
            content_type="text/csv",
        )
        return "Arquivo 'dados_vendas.csv' salvo no bucket."

    # Encadeamento das tarefas
    criar_bucket = verificar_e_criar_bucket()
    salvar_csv = salvar_csv_no_bucket()
    criar_bucket >> salvar_csv


# Instancia a DAG
dag = teste_comunicacao_minio()