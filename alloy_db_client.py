import subprocess
from typing import Tuple

import pandas as pd
import pg8000
import sqlalchemy
import vertexai
from google.cloud.alloydb.connector import Connector
from google.auth import load_credentials_from_file
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
from vertexai.generative_models import GenerationConfig, GenerativeModel

PROJECT_ID = "model-factor-419419" 
LOCATION = "us-central1"
REGION = "us-central1"
CLUSTER = "hackathon-cluster"
INSTANCE = "hackathon-instance"

vertexai.init(project=PROJECT_ID, location=LOCATION)

SERVICE_ACCOUNT_FILE = 'model-factor-419419-ff066c86a73f.json'

credentials, project = load_credentials_from_file(SERVICE_ACCOUNT_FILE)

def create_sqlalchemy_engine(
    inst_uri: str, user: str, password: str, db: str
) -> Tuple[sqlalchemy.engine.Engine, Connector]:
    
    connector = Connector(credentials=credentials)

    def getconn() -> pg8000.dbapi.Connection:
        conn = connector.connect(
            instance_uri=inst_uri,
            driver="pg8000",
            user=user,
            password=password,
            db=db,
            ip_type="PUBLIC",
        )
        return conn

    # create SQLAlchemy connection pool
    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://", creator=getconn, isolation_level="AUTOCOMMIT"
    )
    engine.dialect.description_encoding = None
    return engine, connector

INSTANCE_URI = f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{CLUSTER}/instances/{INSTANCE}"
DB = "postgres"

engine, connector = create_sqlalchemy_engine(
    inst_uri=INSTANCE_URI,
    user="postgres",
    password=r"S'9i9.%+%:k_rS%d",
    db=DB,
)

# create_table_cmd = sqlalchemy.text(
#         f"CREATE TABLE paper_embeddings ( \
#       paper_id VARCHAR(64) \
#       )",
#     )

# with engine.connect() as conn:
#         print("Creating table...")
#         conn.execute(create_table_cmd)
#         print("Commiting...")
#         conn.commit()
#         print("Done")

#connector.close()

# Add extensions
google_ml_integration_cmd = sqlalchemy.text(
    "CREATE EXTENSION IF NOT EXISTS google_ml_integration CASCADE"
)
vector_cmd = sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS vector")

# Execute the queries
# with engine.connect() as conn:
#     conn.execute(google_ml_integration_cmd)
#     conn.execute(vector_cmd)
#     conn.commit()
# connector.close()


embedding_column = "embedding"
distance_function = "vector_cosine_ops"

# Add column to store embeddings
# add_column_cmd = sqlalchemy.text(
#     f"ALTER TABLE paper_embeddings ADD COLUMN embedding vector({768});"
# )

# # Generate embeddings for `title` and `abstract` columns of the dataset
# embedding_cmd = sqlalchemy.text(
#     f"UPDATE paper_embeddings SET embedding = embedding('textembedding-gecko@003', paper_id);"
# )

# # Create an ivfflat index on the table with embedding column and cosine distance
# index_cmd = sqlalchemy.text(
#     f"CREATE INDEX ON paper_embeddings USING ivfflat (embedding vector_cosine_ops)"
# )

# # Execute the queries
# with engine.connect() as conn:
#     # try:
#     #     conn.execute(add_column_cmd)
#     # except:
#     #     print(f"Column {embedding_column} already exists")
#     print("Creating Embeddings...")
#     conn.execute(embedding_cmd)
#     print("Creating Index...")
#     conn.execute(index_cmd)
#     print("Commiting...")
#     conn.commit()
#     print("Done")
# connector.close()

search_cmd = sqlalchemy.text(
        f"""
    SELECT paper_id FROM paper_embeddings
      ORDER BY embedding
      <-> embedding('textembedding-gecko@003', 'Bricks')::vector
      LIMIT 1
    """
    )

    # Execute the query
with engine.connect() as conn:
    result = conn.execute(search_cmd)
    context = [row._asdict() for row in result]
connector.close()

retrieved_information = "\n".join(
        [
            f"{index+1}. "
            + "\n".join([f"{key}: {value}" for key, value in element.items()])
            for index, element in enumerate(context)
        ]
    )

print(retrieved_information)