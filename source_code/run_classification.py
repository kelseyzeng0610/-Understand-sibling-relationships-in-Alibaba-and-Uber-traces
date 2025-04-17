import json
from classify import classify_siblings


def save_results_txt(results,output_file):
    with open(output_file,"w") as f:
        for pair, info in sorted(results.items(),key= lambda x:-x[1].get('confidence',0)):
            f.write(f"{pair[0]} -> {pair[1]}\n")
            for key,val in info.items():
                f.write(f"    {key}:{val} \n")
            f.write("\n")
    
if __name__ == "__main__":
    trace_dir = "/Users/apple/Documents/-Understand-sibling-relationships-in-Alibaba-and-Uber-traces/normal"     
    output_txt = "sibling_results.txt"  

    print("Running sibling execution classification...")
    results = classify_siblings(trace_dir)

    print(f"Writing results to {output_txt}...")
    save_results_txt(results, output_txt)
    print(" Done!")
