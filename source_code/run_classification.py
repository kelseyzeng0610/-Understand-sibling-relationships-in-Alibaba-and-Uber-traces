
import os
import argparse
import numpy as np

from classify import classify_siblings  # Import your per-parent classifier

# Default fallback if there are no parallel pairs
DEFAULT_ANOMALY_THRESHOLD = 0.01

def save_anomalies_txt(results, output_file):
    """
    Write out:
    - pairs that are classified 'parallel' but with confidence < 1.0
      (i.e. mostly sequential but with some overlaps)
    - pairs with inconsistent ordering across runs
    """
    with open(output_file, "w") as f:
        for pair, info in sorted(results.items(), key=lambda x: -x[1].get('confidence', 0)):
            if info['type'] == 'parallel' and info['confidence'] < 0.01 :
                overlaps = int(info['confidence'] * info['samples'])
                f.write(
                    f"Anomaly: {pair[0]} vs {pair[1]} – "
                    f"mostly sequential ({info['samples'] - overlaps}/{info['samples']} runs) "
                    f"but {overlaps} overlap(s)\n"
                )
            elif info['type'] == 'inconsistent':
                f.write(
                    f"Anomaly: {pair[0]} vs {pair[1]} – inconsistent ordering "
                    f"{info.get('orderings')} over {info['samples']} runs\n"
                )


def save_results_txt(results, output_file):
    with open(output_file, "w") as f:
        for pair, info in sorted(results.items(), key=lambda x: -x[1].get('confidence', 0)):
            f.write(f"{pair[0]} -> {pair[1]}\n")
            for key, val in info.items():
                f.write(f"    {key}: {val}\n")
            f.write("\n")

def save_per_parent_results(results, output_file):
    """Write full per-parent classification results to a file."""
    with open(output_file, 'w') as f:
        for (parent_id, opA, opB), info in sorted(
                results.items(), key=lambda x: (x[0][0], -x[1]['confidence'])):
            f.write(f"Parent: {parent_id} | {opA} -> {opB}\n")
            for k, v in info.items():
                f.write(f"    {k}: {v}\n")
            f.write("\n")


def save_per_parent_anomalies(results, output_file, threshold):
    """
    Write anomalies for per-parent results:
      - parallel pairs with confidence < threshold
      - inconsistent ordering pairs
    """
    with open(output_file, 'w') as f:
        for (parent_id, opA, opB), info in sorted(
                results.items(), key=lambda x: (x[0][0], -x[1]['confidence'])):

            if info['type'] == 'parallel' and info['confidence'] < threshold:
                overlaps = int(info['confidence'] * info['samples'])
                f.write(
                    f"Anomaly (Parent {parent_id}): {opA} vs {opB} – "
                    f"mostly sequential ({info['samples'] - overlaps}/{info['samples']}) "
                    f"but {overlaps} overlap(s) (<{threshold*100:.2f}% of runs)\n"
                )

            elif info['type'] == 'inconsistent':
                f.write(
                    f"Anomaly (Parent {parent_id}): {opA} vs {opB} – inconsistent ordering "
                    f"{info['orderings']} over {info['samples']} runs\n"
                )



if __name__ == "__main__":
    trace_dir = "/Users/apple/Documents/-Understand-sibling-relationships-in-Alibaba-and-Uber-traces/normal"
    global_results = classify_siblings(trace_dir,global_mode=True)

    # Full classification
    output_txt = "sibling_results.txt"
    save_results_txt(global_results, output_txt)
    save_anomalies_txt(global_results, "sibling_anomalies_global.txt")
    overlap_rates = [info['confidence'] for info in global_results.values() if info['type'] == 'parallel']
    if overlap_rates:
        dynamic_thresh = np.percentile(overlap_rates, 1)
    else:
        dynamic_thresh = DEFAULT_ANOMALY_THRESHOLD
    print(f"Dynamic anomaly threshold (1st percentile): {dynamic_thresh:.4f}")



    print(f"Wrote classification results to {output_txt}")
    # save per parent
    per_parent_results = classify_siblings(trace_dir, global_mode=False)
    per_parent_txt = "sibling_per_parent_results.txt"
    save_per_parent_results(per_parent_results, per_parent_txt)
    print(f"Wrote per-parent classification results to {per_parent_txt}")

    # One-off & inconsistent-order anomalies
    anomalies_txt = "sibling_anomalies.txt"
    save_per_parent_anomalies(per_parent_results, anomalies_txt, 0.01)
    print(f"Wrote anomalies to {anomalies_txt}")