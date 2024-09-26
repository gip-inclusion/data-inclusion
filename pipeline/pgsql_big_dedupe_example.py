#!/usr/bin/env python
import argparse
import csv
import io
import itertools
import logging
import os
import time
from venv import logger

import datetimetype
import dedupe
import dedupe.variables
import numpy
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs, register_adapter

register_adapter(numpy.int32, AsIs)
register_adapter(numpy.int64, AsIs)
register_adapter(numpy.float32, AsIs)
register_adapter(numpy.float64, AsIs)

READ_CURSOR_NAME = "read_cursor"
SETTINGS_FILE = ".pgsql_big_dedupe_example.settings"
TRAINING_FILE = ".pgsql_big_dedupe_example.training.json"


class Readable:
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
        a_record_id, a_record, b_record_id, b_record = row
        record_a = (a_record_id, a_record)
        record_b = (b_record_id, b_record)

        yield record_a, record_b

        if i % 10000 == 0:
            print(i)


def cluster_ids(clustered_dupes):
    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for structure_id, score in zip(cluster, scores):
            yield structure_id, cluster_id, score


def same_or_not_comparator(field_1, field_2):
    if field_1 and field_2:
        if field_1 == field_2:
            return 0
        else:
            return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Increase verbosity")
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (specify multiple times for more)",
    )

    args = parser.parse_args()
    log_level = logging.WARNING
    print("Args: ", args.verbose)

    if args.verbose:
        if args.verbose == 1:
            log_level = logging.INFO
        elif args.verbose >= 2:
            log_level = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(log_level)

    start_time = time.time()

    print("connecting to database", os.environ["AIRFLOW_CONN_PG"])
    read_con = psycopg2.connect(
        dsn=os.environ["AIRFLOW_CONN_PG"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

    write_con = psycopg2.connect(
        dsn=os.environ["AIRFLOW_CONN_PG"],
    )

    # Attention de bien préprocesser pour avoir des NULL quand la valeur est absente
    STRUCTURES_SELECT = (
        "SELECT _di_surrogate_id AS structure_id, "
        "source, to_char(date_maj, 'MM/DD/YYYY') AS date_maj, "
        "nom, commune, adresse, "
        "ARRAY[COALESCE(latitude, 0.0), COALESCE(longitude, 0.0)] AS location, "
        "code_postal, code_insee, "
        "siret, SUBSTRING(siret, 0, 10) AS siren, "
        "telephone, courriel FROM api__structures "
        "WHERE source IN ("
        "'dora', 'emplois-de-linclusion', 'france-travail', "
        "'soliguide', 'mes-aides', 'action-logement'"
        ") AND code_insee IS NOT NULL"
    )

    if os.path.exists(SETTINGS_FILE):
        print("reading from ", SETTINGS_FILE)
        with open(SETTINGS_FILE, "rb") as sf:
            deduper = dedupe.StaticDedupe(sf)
    else:
        deduper = dedupe.Dedupe(
            [
                # Les noms des structures mériteraient un ptit NLP custom avec Spacy OU
                # une adaptation de https://github.com/datamade/probablepeople
                # qui se cache derrière namevariable.WesternName
                dedupe.variables.String("nom"),
                dedupe.variables.String("commune", has_missing=True),
                # Comparaison à améliorer avec https://github.com/dedupeio/dedupe-variable-address/blob/master/addressvariable/__init__.py
                # On peut recoder le plugin facilement pour utiliser pypostal
                dedupe.variables.String("adresse", has_missing=True),
                dedupe.variables.LatLong(
                    "location", has_missing=True
                ),  # a définir / préprocesser dans le SELECT ?
                dedupe.variables.ShortString("code_postal", has_missing=True),
                dedupe.variables.ShortString("code_insee"),
                dedupe.variables.ShortString("siren", has_missing=True),
                dedupe.variables.ShortString("siret", has_missing=True),
                dedupe.variables.ShortString("telephone", has_missing=True),
                dedupe.variables.ShortString("courriel", has_missing=True),
                dedupe.variables.ShortString("source"),
                datetimetype.DateTime("date_maj", fuzzy=False),
            ]
        )

        print("reading from ", STRUCTURES_SELECT)
        with read_con.cursor(READ_CURSOR_NAME) as cur:
            cur.execute(STRUCTURES_SELECT)
            enumerated_structures = {i: row for i, row in enumerate(cur)}

        if os.path.exists(TRAINING_FILE):
            print("reading labeled examples from ", TRAINING_FILE)
            with open(TRAINING_FILE) as tf:
                deduper.prepare_training(enumerated_structures, tf)
        else:
            print("prepare training")
            deduper.prepare_training(enumerated_structures)

        del enumerated_structures

        print("starting active labeling...")
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        dedupe.console_label(deduper)
        # When finished, save our labeled, training pairs to disk
        with open(TRAINING_FILE, "w") as tf:
            deduper.write_training(tf)

        deduper.train(recall=0.90)

        with open(SETTINGS_FILE, "wb") as sf:
            deduper.write_settings(sf)

        deduper.cleanup_training()

    print("blocking...")

    print("creating blocking_map database")
    with write_con:
        with write_con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS blocking_map")
            cur.execute(
                "CREATE TABLE blocking_map " "(block_key text, structure_id text)"
            )

    print("creating inverted indices")
    for field in deduper.fingerprinter.index_fields:
        with read_con.cursor("field_values") as cur:
            cur.execute("SELECT DISTINCT %s FROM api__structures", field)
            field_data = (row[field] for row in cur)
            deduper.fingerprinter.index(field_data, field)

    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, structure_id)` tuples.
    print("writing blocking map")

    with read_con.cursor(READ_CURSOR_NAME) as read_cur:
        read_cur.execute(STRUCTURES_SELECT)

        full_data = ((row["structure_id"], row) for row in read_cur)
        b_data = deduper.fingerprinter(full_data)

        with write_con:
            with write_con.cursor() as write_cur:
                write_cur.copy_expert(
                    "COPY blocking_map FROM STDIN WITH CSV",
                    Readable(b_data),
                    size=10000,
                )

    deduper.fingerprinter.reset_indices()

    logging.info("indexing block_key")
    with write_con:
        with write_con.cursor() as cur:
            cur.execute(
                "CREATE UNIQUE INDEX ON blocking_map "
                "(block_key text_pattern_ops, structure_id)"
            )

    with write_con:
        with write_con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS entity_map")

            print("creating entity_map database")
            cur.execute(
                "CREATE TABLE entity_map "
                "(structure_id TEXT, canon_id TEXT, "
                " cluster_score FLOAT, PRIMARY KEY(structure_id))"
            )

    with read_con.cursor(
        "pairs", cursor_factory=psycopg2.extensions.cursor
    ) as read_cur:
        read_cur.execute(
            """
            select a._di_surrogate_id as record_id,
                    row_to_json((select d from (
                    select
                        a.source,
                        a.date_maj,
                        a.nom,
                        a.commune,
                        a.adresse,
                        ARRAY[COALESCE(a.latitude, 0.0), COALESCE(a.longitude, 0.0)]
                            AS location,
                        a.code_postal,
                        a.code_insee,
                        a.siret,
                        SUBSTRING(a.siret, 0, 10) AS siren,
                        a.telephone,
                        a.courriel) d)),
                    b._di_surrogate_id as record_id,
                    row_to_json((select d from (
                    select
                        b.source,
                        b.date_maj,
                        b.nom,
                        b.commune,
                        b.adresse,
                        ARRAY[COALESCE(b.latitude, 0.0), COALESCE(b.longitude, 0.0)]
                            AS location,
                        b.code_postal,
                        b.code_insee,
                        b.siret,
                        SUBSTRING(b.siret, 0, 10) AS siren,
                        b.telephone,
                        b.courriel) d))
            from (select DISTINCT l.structure_id as east, r.structure_id as west
                    from blocking_map as l
                    INNER JOIN blocking_map as r
                    using (block_key)
                    where l.structure_id < r.structure_id) ids
            INNER JOIN api__structures a on ids.east=a._di_surrogate_id
            INNER JOIN api__structures b on ids.west=b._di_surrogate_id"""
        )

        print("clustering...")
        clustered_dupes = deduper.cluster(
            deduper.score(record_pairs(read_cur)), threshold=0.5
        )

        print("writing results")
        with write_con:
            with write_con.cursor() as write_cur:
                write_cur.copy_expert(
                    "COPY entity_map FROM STDIN WITH CSV",
                    Readable(cluster_ids(clustered_dupes)),
                    size=10000,
                )

    with write_con:
        with write_con.cursor() as cur:
            cur.execute("CREATE INDEX head_index ON entity_map (canon_id)")

    # select canon_id, source, nom, commune, adresse from entity_map
    #     left join api__structures on structure_id = _di_surrogate_id
    #     where cluster_score > 0.8
    #     order by canon_id;

    read_con.close()
    write_con.close()
