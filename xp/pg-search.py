"""
Loaded the structures data using pg_restore:

    ./psql
    ALTER SCHEMA public RENAME to oldpublic;
    CREATE SCHEMA public;

    pg_restore -d data-inclusion --clean --if-exists --no-owner --no-privileges xp/struct.dump

    ./psql
    ALTER SCHEMA public RENAME to public_vector;
    ALTER SCHEMA oldpublic RENAME to public;
    ALTER TABLE public_vector.api__structures ADD COLUMN embedding vector(384);

    then upsert

    INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')
    ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;
"""

# good resources
# https://qdrant.tech/articles/hybrid-search/
# https://www.sbert.net/examples/applications/semantic-search/README.html

import asyncio
import itertools
from asyncio.sslproto import add_flowcontrol_defaults

import encode
import psycopg
from pgvector.psycopg import register_vector_async
from sentence_transformers import SentenceTransformer


async def create_schema(conn):
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await register_vector_async(conn)

    await conn.execute("DROP TABLE IF EXISTS documents")
    await conn.execute(
        "CREATE TABLE documents (pk text PRIMARY KEY, source text, id text, nom text, commune text, code_insee text, adresse text, search text, embedding vector(384))"
    )
    await conn.execute(
        "CREATE INDEX ON documents USING GIN (to_tsvector('french', search))"
    )


async def insert_data(conn, data):
    sql = (
        "INSERT INTO documents (pk, source, id, nom, commune, code_insee, adresse, search, embedding) VALUES "
        + ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s)" for _ in data])
    )
    params = list(
        itertools.chain(
            *zip(
                [row["source"] + ":" + row["id"] for row in data],
                [row["source"] for row in data],
                [row["id"] for row in data],
                [row["nom"] for row in data],
                [row["commune"] for row in data],
                [row["code_insee"] for row in data],
                [row["adresse"] for row in data],
                [row["search"] for row in data],
                [row["embedding"] for row in data],
            )
        )
    )
    await conn.execute(sql, params)


PGURL = "postgresql://data-inclusion:data-inclusion@172.17.0.1:5455/data-inclusion"


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def setup():
    conn = await psycopg.AsyncConnection.connect(conninfo=PGURL, autocommit=True)
    await create_schema(conn)

    rows = []
    sentences = []
    for row in encode.yield_lines("structures.csv"):
        row["search"] = f"{row['nom']} à {row['adresse']}"
        sentences.append(row["search"])
        rows.append(row)

    print("File has been read.")

    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(sentences)

    for i, row in enumerate(rows):
        row["embedding"] = embeddings[i]

    for chunk in chunks(rows, 1000):
        print("Inserting chunk")
        await insert_data(conn, chunk)


async def get_pe_agencies(conn):
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT nom, adresse, commune, source, code_insee, embedding, search FROM documents WHERE source = 'france-travail'"
        )
        return await cur.fetchall()


async def semantic_search(conn, source, code_insee, embedding):
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT *, 1 - (embedding <=> %s) as similarity FROM documents WHERE source != %s AND code_insee = %s ORDER BY similarity DESC",
            (
                embedding,
                source,
                code_insee,
            ),
        )
        return await cur.fetchall()


async def keyword_search(conn, source, code_insee, query):
    async with conn.cursor() as cur:
        await cur.execute(
            (
                "WITH subdocs AS ("
                "    SELECT * FROM documents WHERE source != %s AND code_insee = %s"
                ") SELECT *, 1 - ts_rank_cd(to_tsvector('french', search), query, 32) as rank FROM subdocs, plainto_tsquery('french', %s) query "
                "WHERE to_tsvector('french', search) @@ query "
                "ORDER BY rank DESC"
            ),
            (
                source,
                code_insee,
                query,
            ),
        )
        return await cur.fetchall()


MIN_SCORE = 0.80


async def main():
    # await setup()
    conn = await psycopg.AsyncConnection.connect(conninfo=PGURL, autocommit=True)
    lines = await get_pe_agencies(conn)

    num_agencies = 0
    num_fulltext_matches = 0
    num_semantic_matches = 0

    for line in lines:
        if not line[4]:  # no insee code
            continue
        print(
            f"\n>>> looking for source='{line[3]}' nom='{line[0]}' commune='{line[3]} adresse='{line[1]}'"
        )
        num_agencies += 1
        results = await keyword_search(conn, line[3], line[4], line[6])
        for result in results:
            print(
                f"\t KEYWORD source='{result[1]} nom='{result[3]}' commune='{result[4]}' adresse='{result[6]}' score={result[-1]}"
            )
            num_fulltext_matches += 1
        results = await semantic_search(conn, line[3], line[4], line[5])
        if not results:
            continue
        if results[0][-1] < MIN_SCORE:  # the closest result is not accurate enough
            continue
        for result in results:
            if result[-1] < MIN_SCORE:
                break
            print(
                f"\t SEMANTIC source='{result[1]} nom='{result[3]}' commune='{result[4]}' adresse='{result[6]}' score={result[-1]}"
            )
            num_semantic_matches += 1

    print(f"Found {num_agencies=} {num_fulltext_matches=} {num_semantic_matches=}")


"""
Au final la recherche via NLP fonctionne "mieux" au sens où elle va trouver plus de "positifs".

En particulier, on est capable dans ce cas précis de retrouver des agences "Pole Emploi" à la meme adresse,
là où la recherche fulltext ne trouve rien.

>>> looking for source='france-travail' nom='Agence France Travail SOISSONS' commune='france-travail adresse='24 RUE DE L EMAILLERIE'
         KEYWORD source='dora nom='Agence France Travail SOISSONS' commune='Soissons' adresse='24 RUE DE L EMAILLERIE' score=0.9756097551435232
         SEMANTIC source='dora nom='Agence France Travail SOISSONS' commune='Soissons' adresse='24 RUE DE L EMAILLERIE' score=0.9999999701976825
         SEMANTIC source='mediation-numerique nom='Agence Pôle Emploi SOISSONS' commune='Soissons' adresse='24 rue de l'Emaillerie' score=0.8805425500708094

En revanche la recherche propose trop de "faux" positifs : cf. ci-dessous où l'on groupe ensemble des agences très proches,
mais non identiques.

>>> looking for source='france-travail' nom='Agence France Travail Paris 20ème Piat' commune='france-travail adresse='51 RUE Piat'
         KEYWORD source='dora nom='Agence France Travail Paris 20ème Piat' commune='Paris' adresse='51 RUE Piat' score=0.9523809514939785
         SEMANTIC source='dora nom='Agence France Travail Paris 20ème Piat' commune='Paris' adresse='51 RUE Piat' score=0.9999999403953606
         SEMANTIC source='dora nom='Agence France Travail Paris 5 et 13ème Daviel' commune='Paris' adresse='27 RUE Daviel' score=0.8421943281773148
         SEMANTIC source='dora nom='Agence France Travail Paris 20ème Vitruve' commune='Paris' adresse='60 RUE Vitruve' score=0.8352115350709516

En axes d'amélioration:
- utiliser un modèle plus "malin" chez OpenAI qui nous permette de demander "quelles structure X semble être Y ?"
- nous pouvons entraîner le modèle pour qu'il redécouvre toutes les agences Pole Emploi que l'on connaît : il faut créer un dataset.
- il faudrait étudier dans notre corpus si l'adresse est plus régulièrement fausse que le nom, par exemple ?

"""

asyncio.run(main())
