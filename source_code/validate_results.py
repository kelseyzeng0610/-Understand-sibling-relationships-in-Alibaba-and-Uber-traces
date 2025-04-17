from classify import classify_siblings, spans_overlap
import os
import json
from collections import defaultdict
from itertools import combinations

def get_parent_id(span):
    if 'parentSpanId' in span and span['parentSpanId']:
        return span['parentSpanId']
    if 'references' in span:
        for ref in span['references']:
            if ref.get('refType', '').lower() == 'child_of':
                return ref.get('spanID')
    return None

def rebuild_sibling_evidence(trace_dir):
    evidence = defaultdict(list)

    for fname in os.listdir(trace_dir):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(trace_dir, fname)) as f:
            content = json.load(f)
            if isinstance(content, dict) and "data" in content:
                traces = content["data"]
            elif isinstance(content, list):
                traces = content
            else:
                traces = [content]

        for trace in traces:
            spans = trace.get('spans', trace if isinstance(trace, list) else [])
            parent_to_children = defaultdict(list)
            for span in spans:
                try:
                    span['startTime'] = int(span['startTime'])
                    span['endTime'] = span['startTime'] + int(span['duration'])
                    opKey = f"{span.get('operationName')}<{span['spanID']}>"
                    span['opKey'] = opKey
                    parent_id = get_parent_id(span)
                    if parent_id:
                        parent_to_children[parent_id].append(span)
                except:
                    continue

            for siblings in parent_to_children.values():
                for a, b in combinations(siblings, 2):
                    id_pair = tuple(sorted([a['opKey'], b['opKey']]))
                    if spans_overlap(a, b):
                        evidence[id_pair].append("overlap")
                    else:
                        evidence[id_pair].append("no-overlap")
    return evidence

def validate_classification(trace_dir, classification):
    evidence = rebuild_sibling_evidence(trace_dir)
    errors = []

    for pair, result in classification.items():
        observed = evidence.get(pair, [])
        if result['type'] == 'sequential':
            if 'overlap' in observed:
                errors.append((pair, "Sequential but overlap found"))
        elif result['type'] == 'parallel':
            if 'overlap' not in observed:
                errors.append((pair, "Parallel but no overlap observed"))
        # optional: handle "sequential (inconsistent order)" if needed

    return errors


if __name__ == "__main__":
    trace_dir = "/Users/apple/Desktop/Final_Project_CS151/normal" 
    results = classify_siblings(trace_dir)

    errors = validate_classification(trace_dir, results)
    if errors:
        for pair, msg in errors:
            print(f"❌ {msg}: {pair}")
    else:
        print("✅ Internal consistency checks passed!")