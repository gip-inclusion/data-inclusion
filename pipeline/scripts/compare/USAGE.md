# Usage

Compute a diff between files:

```bash
./compare.py compare BEFORE AFTER --meta-columns source > /tmp/diff.jsonl
```

Generate a summary:

```bash
./compare.py summarize /tmp/diff.jsonl
```

Generate a summary with a llm generated comment:

```bash
# requires access to an openai compatible provider
# with `OPENAI_API_KEY` and `OPENAI_BASE_URL` set
./compare.py summarize diff.jsonl --llm --model
```

One-liner with pipes:

```bash
./compare.py compare BEFORE AFTER --meta-columns source | ./compare.py summarize --llm
```

The diff can be filter with `jq`:

```bash
./compare.py compare BEFORE AFTER --meta-columns source | jq -s -c '.[] | select(.source == "dora")' | ./compare.py summarize --llm
```
