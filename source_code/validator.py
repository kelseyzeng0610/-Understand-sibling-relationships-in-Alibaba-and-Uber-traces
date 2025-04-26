
import os
from collections import defaultdict
from itertools import combinations

from classify import parse_traces, group_by_parent, classify_siblings

# only these three file‐prefixes now:
KNOWN_TYPES = {"parallel", "sequential", "inconsistent"}

def extract_expected_type(fname):
    p = fname.split("_",1)[0]
    if p not in KNOWN_TYPES:
        return "unknown"
    return p

def validate_global(trace_dir, global_mode=True):
    # build expected_by_pair using same key‐shape
    expected = defaultdict(set)
    for fn in os.listdir(trace_dir):
        if not fn.endswith(".json"):
            continue
        want = extract_expected_type(fn)
        spans = parse_traces(os.path.join(trace_dir, fn))
        pm    = group_by_parent(spans)
        for pid, sibs in pm.items():
            for a,b in combinations(sibs,2):
                opA = a.get("opKey", a["operationName"])
                opB = b.get("opKey", b["operationName"])
                if global_mode:
                    key = tuple(sorted([opA, opB]))
                else:
                    key = (pid,) + tuple(sorted([opA, opB]))
                expected[key].add(want)

    # run classifier
    results = classify_siblings(trace_dir, global_mode=global_mode)

    # compare
    correct = total = 0
    bad = []
    for key, info in results.items():
        total += 1
        got = info["type"]
        want_set = expected.get(key, {"unknown"})
        if got in want_set:
            correct += 1
        else:
            bad.append((key, want_set, got))

    print(f"\n✅  {correct}/{total} pairs matched\n")
    if bad:
        print("❌ Mismatches:")
        for k,w,g in bad:
            print(f"  {k}: expected {w}, got {g}")

if __name__ == "__main__":
    # Set global_mode=False to test per‐parent, True for global aggregation
    validate_global("/Users/apple/Documents/-Understand-sibling-relationships-in-Alibaba-and-Uber-traces/source_code/synthetic_traces",global_mode=True)
