import os
import json
from collections import defaultdict
from itertools import combinations
from tqdm import tqdm # progress bar!

def spans_overlap(a, b):
    return a['startTime'] < b['endTime'] and b['startTime'] < a['endTime']

def get_parent_id(span):
    # Handles Jaegerstyle references and explicit parentSpanId
    if 'parentSpanId' in span and span['parentSpanId']:
        return span['parentSpanId']
    if 'references' in span:
        for ref in span['references']:
            if ref.get('refType', '').lower() == 'child_of':
                return ref.get('spanID')
    return None

def classify_siblings(trace_dir):
    sibling_evidence = defaultdict(list)

    for fname in tqdm(os.listdir(trace_dir), desc="Processing trace files"):
        if not fname.endswith('.json'):
            continue
        path = os.path.join(trace_dir, fname)
        try:
            with open(path) as f:
                content = json.load(f)
                # specific to uber sample dataset that we got
                if isinstance(content, dict) and "data" in content:
                    traces = content["data"]
                elif isinstance(content, list):
                    traces = content
                else:
                    traces = [content]
        except Exception as e:
            print(f" Error reading {fname}: {e}")
            continue

        for trace in traces:
            spans = trace.get('spans', trace if isinstance(trace, list) else [])
            parent_to_children = defaultdict(list)

            for span in spans:
                try:
                    span_id = span['spanID']
                    start = int(span['startTime'])
                    duration = int(span['duration'])
                    end = start + duration
                    op = span.get('operationName', span_id)

                    span['endTime'] = end
                    span['startTime'] = start
                    span['opKey'] = f"{op}<{span_id}>"

                    parent_id = get_parent_id(span)
                    if parent_id:
                        parent_to_children[parent_id].append(span)
                except Exception as e:
                    print(f"⚠️ Malformed span skipped in {fname}: {e}")
                    continue

            # Compare all sibling pairs
            for siblings in parent_to_children.values():
                for a, b in combinations(siblings, 2):
                    a_key, b_key = a['opKey'], b['opKey']
                    id_pair = tuple(sorted([a_key, b_key]))

                    if spans_overlap(a, b):
                        sibling_evidence[id_pair].append('overlap')
                    else:
                        if a['endTime'] <= b['startTime']:
                            sibling_evidence[id_pair].append(f'{a_key}_before_{b_key}')
                        elif b['endTime'] <= a['startTime']:
                            sibling_evidence[id_pair].append(f'{b_key}_before_{a_key}')
                        else:
                            sibling_evidence[id_pair].append('ambiguous')

    # Final classification
    results = {}
    for pair, events in sibling_evidence.items():
        total = len(events)
        overlap_count = sum(1 for e in events if e == 'overlap')

        if overlap_count > 0:
            results[pair] = {
                'type': 'parallel',
                'confidence': round(overlap_count / total, 3),
                'samples': total
            }
        else:
            orderings = set(events)
            if len(orderings) == 1:
                results[pair] = {
                    'type': 'sequential',
                    'order': list(orderings)[0],
                    'confidence': 1.0,
                    'samples': total
                }
            else:
                results[pair] = {
                    'type': 'sequential (inconsistent order)',
                    'orderings': list(orderings),
                    'confidence': 1.0,
                    'samples': total
                }

    return results
