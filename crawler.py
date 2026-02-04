import boto3
from config import AWS_REGION, BUCKET_NAME, BASE_PREFIX

# Configura el cliente de Glue
glue_client = boto3.client('glue', region_name=AWS_REGION)

database_name = "trade_data_imat3a04"  # Base de datos para el proyecto Polkadot Rush 

try:
    response = glue_client.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Base de datos para datos historicos de criptomonedas'
        }
    )
    print(f"Base de datos {database_name} creada exitosamente.")
except glue_client.exceptions.AlreadyExistsException:
    print(f"La base de datos {database_name} ya existe.")


crawler_name = "crypto_history_crawler"
# Nota: Debes configurar el ARN del rol IAM con permisos para S3 y Glue
# Ejemplo: "arn:aws:iam::123456789012:role/AWSGlueServiceRole"
role_arn = "arn:aws:iam::CUENTA_AWS:role/sprint2a04"  # TODO: Reemplazar con tu ARN real
s3_target_path = f"s3://{BUCKET_NAME}/{BASE_PREFIX}/"  # Ruta raíz donde están los datos

try:
    glue_client.create_crawler(
        Name=crawler_name,
        Role=role_arn,
        DatabaseName=database_name, # La base de datos creada en el paso anterior
        Description='Crawler para indexar datos históricos de criptomonedas',
        Targets={
            'S3Targets': [
                {
                    'Path': s3_target_path
                },
            ]
        },
        # Configuración para actualizar el esquema si cambian los datos
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        RecrawlPolicy={
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        }
    )
    print(f"Crawler {crawler_name} creado exitosamente.")
except glue_client.exceptions.AlreadyExistsException:
    print(f"El crawler {crawler_name} ya existe.")

try:
    response = glue_client.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} iniciado correctamente.")
except Exception as e:
    print(f"Error al iniciar el crawler: {e}")