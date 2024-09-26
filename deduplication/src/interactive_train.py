"""
First attempt at deduplicating our records. This script is a first draft that will
probably be converted to a DAG in the future. It uses the dedupe library to deduplicate
records from our pre-processed api__structures table.

Originally very inspired by
https://dedupeio.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html
"""

import argparse
import csv
import io
import itertools
import os
import time

import datetimetype
import dedupe
import dedupe.variables
import psycopg2
import psycopg2.extras

READ_CURSOR_NAME = "read_cursor"
SETTINGS_FILE = ".deduper.settings"
TRAINING_FILE = ".deduper.training.json"
SLICE_SIZE = 10_000
TRAINING_SAMPLE_SIZE = 15_000  # default is 1500
TRAINING_BLOCKED_PROPORTION = 0.9  # default is 0.9


class ReadableIterator:
    """Provides a `read()` method that can be used to read
    slice by slice (of a given size) from an iterator.
    """

    def __init__(self, iterator):
        self.output = io.StringIO()
        self.writer = csv.writer(self.output)
        self.iterator = iterator

    def read(self, size):
        self.writer.writerows(itertools.islice(self.iterator, size))
        chunk = self.output.getvalue()
        self.output.seek(0)
        self.output.truncate(0)
        return chunk


def record_pairs(result_set):
    for i, row in enumerate(result_set):
        record_a = (row["record_id_a"], row["record_a"])
        record_b = (row["record_id_b"], row["record_b"])

        yield record_a, record_b

        if i % SLICE_SIZE == 0:
            print(f"[clustering] processed n={i} record pairs")


def cluster_ids(clustered_dupes):
    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for structure_id, score in zip(cluster, scores):
            yield structure_id, cluster_id, score


# Attention de bien préprocesser pour avoir des NULL quand la valeur est absente
STRUCTURE_TABLE = "public_intermediate.int_dedupe_preprocessed_structures"
STRUCTURES_SELECT = f"SELECT * FROM {STRUCTURE_TABLE}"


# Same columns as above, without the surrogate ID
def structure_columns(s):
    return f"""
        SELECT
            {s}.source,
            {s}.date_maj,
            {s}.nom,
            {s}.commune,
            {s}.adresse,
            {s}.location,
            {s}.code_postal,
            {s}.code_insee,
            {s}.siret,
            {s}.siren,
            {s}.telephone,
            {s}.courriel
    """


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dbname",
        help="Specify the database connection string",
    )
    args = parser.parse_args()

    read_con = psycopg2.connect(
        dsn=args.dbname,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    write_con = psycopg2.connect(dsn=args.dbname)

    if os.path.exists(SETTINGS_FILE):
        print("[training] reading training data from file=", SETTINGS_FILE)
        with open(SETTINGS_FILE, "rb") as sf:
            deduper = dedupe.StaticDedupe(sf)
    else:
        print("[training] interactive training mode")
        deduper = dedupe.Dedupe(
            [
                # Les noms des structures mériteraient un ptit NLP custom avec Spacy OU
                # une adaptation de https://github.com/datamade/probablepeople
                # qui se cache derrière namevariable.WesternName
                dedupe.variables.String("nom", crf=True),
                dedupe.variables.String("commune", has_missing=True, name="commune"),
                # Comparaison à améliorer avec https://github.com/dedupeio/dedupe-variable-address/blob/master/addressvariable/__init__.py
                # On peut recoder le plugin facilement pour utiliser pypostal
                dedupe.variables.String("adresse", has_missing=True, name="adresse"),
                dedupe.variables.LatLong(
                    "location",
                    has_missing=True,
                    name="location",
                ),  # a définir / préprocesser dans le SELECT ?
                dedupe.variables.ShortString(
                    "code_postal",
                    has_missing=True,
                    name="code_postal",
                ),
                dedupe.variables.ShortString("code_insee", name="code_insee"),
                dedupe.variables.ShortString("siren", has_missing=True, name="siren"),
                dedupe.variables.ShortString("siret", has_missing=True, name="siret"),
                dedupe.variables.ShortString("telephone", has_missing=True),
                dedupe.variables.ShortString("courriel", has_missing=True),
                dedupe.variables.ShortString("source"),
                datetimetype.DateTime("date_maj", fuzzy=False),
                dedupe.variables.Interaction("siret", "siren"),
                dedupe.variables.Interaction(
                    "commune",
                    "adresse",
                    "location",
                    "code_postal",
                    "code_insee",
                ),
            ]
        )

        if os.path.exists(TRAINING_FILE):
            # NOTE(vperron) : this training file could be our "base dataset" against
            # which we can tune our dedupe variables and labeling sessions to improve
            # our accuracy little by little. But for now, it's just probably random
            # samples from a previous training.
            print("[training] reading labeled examples from ", TRAINING_FILE)
            with open(TRAINING_FILE) as tf:
                deduper.prepare_training({}, training_file=tf)
        else:
            print("[training] sampling training data from=", STRUCTURES_SELECT)
            start_time = time.time()
            with read_con.cursor(READ_CURSOR_NAME) as cur:
                cur.execute(STRUCTURES_SELECT)
                training_structures = {i: row for i, row in enumerate(cur)}
            deduper.prepare_training(
                training_structures,
                sample_size=TRAINING_SAMPLE_SIZE,
                blocked_proportion=TRAINING_BLOCKED_PROPORTION,
            )
            del training_structures  # free some memory
            end_time = time.time()
            print("[training] took", end_time - start_time, "seconds")

        print("[training] asking interactive matching from user")
        dedupe.console_label(deduper)

        print("[training] saving user-supplied matches to file=", TRAINING_FILE)
        with open(TRAINING_FILE, "w") as tf:
            deduper.write_training(tf)

        deduper.train(
            recall=1.0,  # always recall ALL trained duplicates
        )

        print("[training] saving trained model to file=", SETTINGS_FILE)
        with open(SETTINGS_FILE, "wb") as sf:
            deduper.write_settings(sf)

        deduper.cleanup_training()

    print("[clustering] creating 'dedupe_predicates' table")
    with write_con.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedupe_predicates")
        cur.execute(
            "CREATE TABLE dedupe_predicates (predicate text, structure_id text)"
        )
        cur.execute("COMMIT")

    print("[clustering] creating in-memory inverted indices for each field")
    for field in deduper.fingerprinter.index_fields:
        with read_con.cursor("field_values") as cur:
            cur.execute(
                f"SELECT DISTINCT %s FROM {STRUCTURE_TABLE}",
                field,
            )
            field_data = (row[field] for row in cur)
            deduper.fingerprinter.index(field_data, field)

    print("[clustering] filling 'dedupe_predicates' table with our clustered data")
    with read_con.cursor(READ_CURSOR_NAME) as read_cur:
        read_cur.execute(STRUCTURES_SELECT)
        all_structures = ((row["id"], row) for row in read_cur)

        # The fingerprinter is an iterable of (predicate, structure_id) pairs
        fingerprinted_data = deduper.fingerprinter(all_structures)

        with write_con.cursor() as write_cur:
            write_cur.copy_expert(
                "COPY dedupe_predicates FROM STDIN WITH CSV",
                ReadableIterator(fingerprinted_data),
                size=SLICE_SIZE,
            )
    deduper.fingerprinter.reset_indices()

    print("[clustering] creating index for 'fingerprint' on 'dedupe_predicates'")
    with write_con.cursor() as cur:
        cur.execute(
            "CREATE UNIQUE INDEX ON dedupe_predicates "
            "(predicate text_pattern_ops, structure_id)"
        )

    with read_con.cursor() as read_cur:
        print("[clustering] generating every possible combination of predicate pairs")
        read_cur.execute(
            f"""
            SELECT
                a.id AS record_id_a,
                row_to_json((select d from ({structure_columns('a')}) d)) AS record_a,
                b.id AS record_id_b,
                row_to_json((select d from ({structure_columns('b')}) d)) AS record_b
            FROM (
                SELECT DISTINCT
                    l.structure_id AS east,
                    r.structure_id AS west
                FROM dedupe_predicates AS l
                INNER JOIN dedupe_predicates AS r
                USING (predicate)
                WHERE l.structure_id < r.structure_id
            ) ids
            INNER JOIN {STRUCTURE_TABLE} a ON ids.east=a.id
            INNER JOIN {STRUCTURE_TABLE} b ON ids.west=b.id
        """
        )

        print("[clustering] clustering and scoring predicate pairs (threshold=0.5)")
        clustered_dupes = deduper.cluster(
            deduper.score(record_pairs(read_cur)), threshold=0.5
        )

        print("[clustering] creating and filling 'dedupe_clusters' table")
        with write_con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dedupe_clusters")
            cur.execute(
                """
                CREATE TABLE dedupe_clusters (
                    cluster_id TEXT,
                    structure_id TEXT,
                    cluster_score FLOAT,
                    PRIMARY KEY(structure_id)
                )
                """
            )
            cur.copy_expert(
                "COPY dedupe_clusters FROM STDIN WITH CSV",
                ReadableIterator(cluster_ids(clustered_dupes)),
                size=SLICE_SIZE,
            )
            cur.execute("DROP INDEX IF EXISTS head_index")
            cur.execute("CREATE INDEX head_index ON dedupe_clusters (cluster_id)")

    read_con.close()
    write_con.close()
