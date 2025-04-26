
import os
import shutil
import random
import json
from uuid import uuid4

MIN_OVERLAP = 10


def make_span(op_name, start, duration, parent_id=None):
    span = {
        "spanID":        uuid4().hex[:16],
        "operationName": op_name,
        "startTime":     int(start),
        "duration":      int(duration),
        "references":    [],
        "processID":     "service-default",
    }
    if parent_id:
        span["references"].append({"refType": "CHILD_OF", "spanID": parent_id})
    return span


def generate_parallel(trace_id):
    root = make_span("RootOp", 1000, 1000)
    base = 1000 + random.randint(0, 50)
    d1 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    d2 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    s1 = base
    s2 = s1 + random.randint(0, d1 - MIN_OVERLAP)
    spanA = make_span(f"ChildA_{trace_id}", s1, d1, root["spanID"])
    spanB = make_span(f"ChildB_{trace_id}", s2, d2, root["spanID"])
    return {"spans": [root, spanA, spanB]}


def generate_sequential(trace_id):
    root = make_span("RootOp", 1000, 1000)
    base = 1000 + random.randint(0, 50)
    d1 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    gap = random.randint(MIN_OVERLAP, MIN_OVERLAP * 3)
    d2 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    s1 = base
    s2 = s1 + d1 + gap
    spanA = make_span(f"ChildA_{trace_id}", s1, d1, root["spanID"])
    spanB = make_span(f"ChildB_{trace_id}", s2, d2, root["spanID"])
    return {"spans": [root, spanA, spanB]}


def generate_inconsistent_pair(trace_id):
    # Single parent ID reused across both runs
    parent_id = uuid4().hex[:16]
    common_root_template = {
        "spanID": parent_id,
        "operationName": "RootOp",
        "startTime": 1000,
        "duration": 1000,
        "references": [],
        "processID": "service-default",
    }

    # First run: A before B
    root1 = dict(common_root_template)
    base1 = 1000 + random.randint(0, 50)
    d1_1 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    gap1 = random.randint(MIN_OVERLAP, MIN_OVERLAP * 3)
    d2_1 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    s1_1 = base1
    s2_1 = s1_1 + d1_1 + gap1
    childA1 = make_span(f"ChildA_{trace_id}", s1_1, d1_1, parent_id)
    childB1 = make_span(f"ChildB_{trace_id}", s2_1, d2_1, parent_id)
    trace1 = {"spans": [root1, childA1, childB1]}

    # Second run: B before A
    root2 = dict(common_root_template)
    base2 = 1000 + random.randint(0, 50)
    d1_2 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    gap2 = random.randint(MIN_OVERLAP, MIN_OVERLAP * 3)
    d2_2 = random.randint(MIN_OVERLAP * 2, MIN_OVERLAP * 4)
    s2_2 = base2
    s1_2 = s2_2 + d2_2 + gap2
    childA2 = make_span(f"ChildA_{trace_id}", s1_2, d1_2, parent_id)
    childB2 = make_span(f"ChildB_{trace_id}", s2_2, d2_2, parent_id)
    trace2 = {"spans": [root2, childA2, childB2]}

    return [trace1, trace2]


def write_synthetic_dataset(output_dir, count_per_type=5):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    types = ["parallel", "sequential", "inconsistent"]
    for t in types:
        for i in range(count_per_type):
            trace_id = f"{t}_{i:02}"
            if t == "parallel":
                data = [generate_parallel(trace_id)]
            elif t == "sequential":
                data = [generate_sequential(trace_id)]
            else:
                data = generate_inconsistent_pair(trace_id)

            fname = f"{trace_id}.json"
            with open(os.path.join(output_dir, fname), "w") as f:
                json.dump({"data": data}, f, indent=2)

    total = len(types) * count_per_type
    print(f"Generated {total} synthetic traces in '{output_dir}'")


if __name__ == "__main__":
    write_synthetic_dataset("synthetic_traces", count_per_type=5)
