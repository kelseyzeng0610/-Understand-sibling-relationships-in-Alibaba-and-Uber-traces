# classify.py
import os
import json
from collections import defaultdict, Counter
from itertools import combinations
from tqdm import tqdm  # optional, for progress bar

# -- threshold for “real” overlap (in the same units as your timestamps) --
MIN_OVERLAP = 10.0  

def spans_overlap(a, b):
    return a['startTime'] < b['endTime'] and b['startTime'] < a['endTime']

def get_parent_id(span):
    if 'parentSpanId' in span and span['parentSpanId']:
        return span['parentSpanId']
    if 'references' in span:
        for ref in span['references']:
            if ref.get('refType', '').lower() == 'child_of':
                return ref.get('spanID')
    return None

def overlap_duration(a, b):
    start = max(a['startTime'], b['startTime'])
    end   = min(a['endTime'],   b['endTime'])
    return max(0.0, end - start)


def parse_traces(path):
    all_spans = []
    files = [path] if os.path.isfile(path) else [
        os.path.join(path, f) for f in os.listdir(path) if f.endswith('.json')
    ]
    for fp in files:
        try:
            with open(fp) as f:
                content = json.load(f)
            traces = content.get("data", content) if isinstance(content, dict) else content
            for trace in traces:
                spans = trace.get("spans", trace)
                for s in spans:
                    s["startTime"] = float(s["startTime"])
                    s["endTime"]   = s["startTime"] + float(s["duration"])
                    all_spans.append(s)
        except Exception:
            continue
    return all_spans
def group_by_parent(spans):
    """Group spans by their parent span ID."""
    parent_to_children = defaultdict(list)
    for span in spans:
        parent_id = get_parent_id(span)
        if parent_id:
            parent_to_children[parent_id].append(span)
    return parent_to_children





def collapse_by_op(siblings):
    """
    Aggregate multiple same-named spans under one parent into a single synthetic span.
    """
    agg = {}
    for s in siblings:
        op = s['opKey']
        if op not in agg:
            agg[op] = {'startTime': s['startTime'], 'endTime': s['endTime']}
        else:
            agg[op]['startTime'] = min(agg[op]['startTime'], s['startTime'])
            agg[op]['endTime']   = max(agg[op]['endTime'],   s['endTime'])
    return [{'opKey': op, **times} for op, times in agg.items()]

def classify_siblings(trace_dir, global_mode=False):
    sibling_evidence = defaultdict(list)

    for fname in tqdm(os.listdir(trace_dir), desc="Processing trace files"):
        if not fname.endswith('.json'):
            continue
        path = os.path.join(trace_dir, fname)
        try:
            content = json.load(open(path))
        except Exception:
            continue

        if isinstance(content, dict) and "data" in content:
            traces = content["data"]
        elif isinstance(content, list):
            traces = content
        else:
            traces = [content]

        for trace in traces:
            spans = trace.get("spans", trace if isinstance(trace, list) else [])
            parent_map = defaultdict(list)
            for s in spans:
                try:
                    # -- use floats for full precision --
                    s["startTime"] = float(s["startTime"])
                    dur = float(s.get("duration", 0.0))
                    s["endTime"]   = s["startTime"] + dur
                    s["opKey"]     = s.get("operationName", s.get("spanID"))
                    pid = get_parent_id(s)
                    if pid:
                        parent_map[pid].append(s)
                except Exception:
                    continue

            if global_mode:
                # aggregate purely by operation-pair, ignoring parent ID
                for siblings in parent_map.values():
                    for a, b in combinations(siblings, 2):
                        ops = tuple(sorted([a['opKey'], b['opKey']]))
                        if spans_overlap(a, b):
                            sibling_evidence[ops].append('overlap')
                        else:
                            if a['endTime'] <= b['startTime']:
                                sibling_evidence[ops].append(f"{a['opKey']}_before_{b['opKey']}")
                            elif b['endTime'] <= a['startTime']:
                                sibling_evidence[ops].append(f"{b['opKey']}_before_{a['opKey']}")
                            else:
                                sibling_evidence[ops].append('weak_overlap')
            else:
                # per-parent context, collapse same-op spans first
                for pid, raw_siblings in parent_map.items():
                    siblings = collapse_by_op(raw_siblings)
                    for a, b in combinations(siblings, 2):
                        key = (pid, a['opKey'], b['opKey'])
                        dur = overlap_duration(a, b)
                        if dur >= MIN_OVERLAP:
                            sibling_evidence[key].append('overlap')
                        else:
                            if a['endTime'] <= b['startTime']:
                                sibling_evidence[key].append(f"{a['opKey']}_before_{b['opKey']}")
                            elif b['endTime'] <= a['startTime']:
                                sibling_evidence[key].append(f"{b['opKey']}_before_{a['opKey']}")
                            else:
                                sibling_evidence[key].append('weak_overlap')

    # Summarize across all traces
    results = {}
    for key, events in sibling_evidence.items():
        total        = len(events)
        counts       = Counter(events)
        overlap_cnt  = counts.pop('overlap', 0)
        weak_cnt     = counts.pop('weak_overlap', 0)

        if overlap_cnt > 0:
            # any real overlap → strong parallel
            results[key] = {
                'type':       'parallel',
                'confidence': overlap_cnt / total,
                'samples':    total,
                'distribution': dict(counts, overlap=overlap_cnt, weak_overlap=weak_cnt)
            }
        elif weak_cnt > 0:
            # no true overlap but some weak_overlap 
            results[key] = {
                'type':       'uncertain',
                'confidence': weak_cnt / total,
                'samples':    total,
                'distribution': dict(counts, weak_overlap=weak_cnt)
            }
        else:
            
            if len(counts) == 1:
                order, freq = next(iter(counts.items()))
                results[key] = {
                    'type':       'sequential',
                    'order':      order,
                    'confidence': freq / total,
                    'samples':    total,
                    'distribution': dict(counts)
                }
            else:
                # saw both A_before_B and B_before_A → inconsistent
                most_common, freq = counts.most_common(1)[0]
                results[key] = {
                    'type':       'inconsistent',
                    'orderings': list(counts.keys()),
                    'confidence': freq / total,
                    'samples':    total,
                    'distribution': dict(counts)
                }

    return results
