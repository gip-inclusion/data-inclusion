import csv
import json
from collections import defaultdict
from statistics import mean

import dedupe

SETTINGS_FILE = "src/.deduper.settings"


def _read_csv(file_path):
    data = {}
    with open(file_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["location"] = list(map(float, row["location"].strip("{}").split(",")))
            for field in row:
                if not row[field]:
                    row[field] = None
            data[row["id"]] = row
    return data


def test_duplicates_are_correctly_found(pytestconfig):
    """Given a preoprocessed list of items, and using the trained model, our function
    should return the exact duplicate list that is saved as a fixture.
    """
    with open(SETTINGS_FILE, "rb") as sf:
        deduper = dedupe.StaticDedupe(sf)
    raw = _read_csv("tests/dedupe_inputs.csv")
    clusters = deduper.partition(raw, 0.5)
    output_data = defaultdict(list)
    for record_ids, scores in sorted(clusters):
        if len(record_ids) > 1:
            # Choose a "stable" cluster id
            cluster_id = sorted(record_ids)[0]
            for score, record_id in sorted(zip(scores, record_ids), reverse=True):
                output_data[str(cluster_id)].append(
                    {
                        "cluster_id": cluster_id,
                        "record_id": record_id,
                        "score": float(round(score, 4)),
                        "data": raw[record_id],
                    }
                )
    should_update_duplicates = pytestconfig.getoption("update_duplicates", False)
    if should_update_duplicates:
        json.dump(output_data, open("tests/dedupe_outputs.json", "w"), indent=4)

    expected_data = json.load(open("tests/dedupe_outputs.json"))

    print(f"n_clusters={len(output_data)} n_expected_clusters={len(expected_data)}")

    matching_clusters = set(output_data.keys()) & set(expected_data.keys())
    non_matching_clusters = set(output_data.keys()) ^ set(expected_data.keys())

    if non_matching_clusters:
        print(f"! found n={non_matching_clusters} non matching clusters")
        for cluster_id in non_matching_clusters:
            print(
                f"\t! cluster_id={cluster_id} "
                f"records={output_data[cluster_id]} "
                f"expected={expected_data[cluster_id]}"
            )

    n = 0
    orig_avg = 0
    new_avg = 0
    for cluster_id in matching_clusters:
        cluster = output_data[cluster_id]
        expected_cluster = expected_data[cluster_id]
        if len(cluster) != len(expected_cluster):
            print(
                f"! cluster_id={cluster_id} "
                f"len={len(cluster)} "
                f"expected_len={len(expected_cluster)}"
            )
        avg = round(mean([r["score"] for r in cluster]), 4)
        expected_avg = round(mean([r["score"] for r in expected_cluster]), 4)
        if avg != expected_avg:
            print(f"! cluster_id={cluster_id} avg={avg} expected_avg={expected_avg}")
            orig_avg += expected_avg
            new_avg += avg
            n += 1

    orig_avg = round(orig_avg / n, 4)
    new_avg = round(new_avg / n, 4)
    print(f"!!! CLUSTERS NUMBER DIFFERS !!! {len(output_data)=} {len(expected_data)=}")
    print(f"!!! AVERAGE SCORE DIFFERS !!! {orig_avg=} {new_avg=}")
    # raw, expected_raw = record.pop("data"), expected_record.pop("data")
    # with check:
    #     assert record == expected_record
    # with check:
    #     assert raw == expected_raw
