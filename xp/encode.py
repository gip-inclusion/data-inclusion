import csv

from sentence_transformers import SentenceTransformer


def yield_lines(filename):
    with open(filename, encoding="utf8") as f:
        reader = csv.DictReader(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            yield row


def get_embedding(sentence):
    model = SentenceTransformer("all-MiniLM-L6-v2")
    return model.encode(
        [sentence],
    )[0]


def get_embeddings(sentences):
    model = SentenceTransformer("all-MiniLM-L6-v2")
    return model.encode(
        sentences,
        batch_size=64,
        show_progress_bar=True,
    )
