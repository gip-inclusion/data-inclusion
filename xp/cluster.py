import time

import encode
from sentence_transformers import util

sentences = []
for row in encode.yield_lines("structures.csv"):
    sentences.append(f"{row['nom']} à l'adresse {row['adresse']}")

embeddings = encode.get_embeddings(sentences)

print("Start clustering")
start_time = time.time()

clusters = util.community_detection(
    embeddings,
    min_community_size=4,
    threshold=0.9,
)

print("Clustering done after {:.2f} sec".format(time.time() - start_time))

for i, cluster in enumerate(clusters):
    print("\nCluster {}, #{} Elements ".format(i + 1, len(cluster)))
    for sentence_id in cluster:
        print("\t", sentences[sentence_id])

                "WITH subdocs AS ("
                "    SELECT * FROM documents WHERE source != %s AND code_insee = %s"
                ") SELECT * FROM subdocs, plainto_tsquery('french', %s) query WHERE "
                "to_tsvector('french', search) @@ query ORDER BY ts_rank_cd(to_tsvector('french', search), query) DESC"
