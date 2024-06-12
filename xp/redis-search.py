import encode
import numpy as np
import redis
from redis.commands.search.field import TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

r = redis.Redis(host="localhost", port=6379)

INDEX_NAME = "myvectors"
DOC_PREFIX = "vec:"

rows = []
for row in encode.yield_lines("structures.csv"):
    row["source"] = row["source"].replace("-", "_")
    row["sentence"] = f"{row['nom']} à l'adresse {row['adresse']}"
    rows.append(row)

print("File has been read.")


def create_index(vector_dimensions: int):
    try:
        r.ft(INDEX_NAME).info()
    except redis.exceptions.ResponseError:
        r.ft(INDEX_NAME).create_index(
            fields=(
                TextField("id"),
                TextField("source"),
                TextField("code_insee"),
                VectorField(
                    "vector",  # Vector Field Name
                    "HNSW",
                    {
                        "TYPE": "FLOAT32",  # FLOAT32 or FLOAT64
                        "DIM": vector_dimensions,  # Number of Vector Dimensions
                        "DISTANCE_METRIC": "COSINE",  # Vector Search Distance Metric
                    },
                ),
            ),
            definition=IndexDefinition(prefix=[DOC_PREFIX], index_type=IndexType.HASH),
        )


r.ft(INDEX_NAME).dropindex(delete_documents=True)
VECTOR_DIMENSIONS = 384
create_index(vector_dimensions=VECTOR_DIMENSIONS)

pipe = r.pipeline()

embeddings = encode.get_embeddings([row["sentence"] for row in rows])

for i, row in enumerate(rows):
    key = f"vec:{row['source']}:{row['id']}"
    row["vector"] = embeddings[i].astype(np.float32).tobytes()
    pipe.hset(key, mapping=row)

res = pipe.execute()

print("Querying every vector")

# For some reason this never made sense. Finally made the prefilters work but the KNN result is
# super silly and the distance makes no sense (6 ? 3.5 ?)
# The Range request does not seem to work as expected either, and getting the fields is disconcerting.
for i, row in enumerate(rows[:10]):
    query = (
        Query(
            "(@source:-$source @code_insee:$code_insee)=>[KNN 2 @vector $vec as score]"
        )
        .with_scores()
        .sort_by("score", asc=True)
        .return_fields("nom", "commune" "adresse" "score")
        .paging(0, 5)
        .dialect(2)
    )
    query_params = {
        "radius": 0.8,
        "vec": encode.get_embedding(row["sentence"]).astype(np.float32).tobytes(),
        "source": row["source"],
        "code_insee": str(row["code_insee"]),
    }
    print(
        ">>>",
        row["source"],
        row["id"],
        row["nom"],
        row["commune"],
        row["adresse"],
    )
    for doc in r.ft(INDEX_NAME).search(query, query_params).docs:
        print("\t", doc)
