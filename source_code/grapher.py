import os
import json
import matplotlib.pyplot as plt
import networkx as nx
from collections import Counter

def plot_relationship_type_pie(results, save_path='relationship_type_pie.png'):
    type_counts = Counter(r['type'] for r in results.values())
    labels = type_counts.keys()
    sizes = type_counts.values()

    plt.figure(figsize=(8,6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title('Distribution of Sibling Relationship Types')
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Saved pie chart to {save_path}")

def plot_confidence_histogram(results, save_path='confidence_histogram.png'):
    confidences = [r['confidence'] for r in results.values()]

    plt.figure(figsize=(8,6))
    plt.hist(confidences, bins=10, range=(0,1), edgecolor='black')
    plt.xlabel('Confidence Score')
    plt.ylabel('Number of Sibling Pairs')
    plt.title('Distribution of Confidence Scores')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Saved confidence histogram to {save_path}")

def plot_sibling_network(results, save_path='sibling_network_graph.png', top_n=30, min_confidence=0.8):
    import matplotlib.patches as mpatches

    filtered = [
        (key, info) for key, info in results.items()
        if info.get('confidence', 0) >= min_confidence
    ]

    filtered = sorted(filtered, key=lambda x: -x[1]['samples'])

    filtered = filtered[:top_n]

    G = nx.Graph()

    for key, info in filtered:
        op1, op2 = key.split('|||')

        relationship = info['type']

        if relationship == 'parallel':
            color = 'green'
            label = 'Parallel'
        elif relationship == 'sequential':
            color = 'blue'
            label = 'Sequential'
        elif relationship == 'sequential (inconsistent order)':
            color = 'red'
            label = 'Inconsistent'
        else:
            color = 'gray'
            label = 'Ambiguous'

        G.add_edge(op1, op2, color=color, label=label)

    pos = nx.shell_layout(G)

    plt.figure(figsize=(16,14))
    nx.draw_networkx_nodes(G, pos, node_size=2200, node_color='lightgray')
    nx.draw_networkx_labels(G, pos, font_size=10)

    edges = G.edges()
    edge_colors = [G[u][v]['color'] for u,v in edges]
    nx.draw_networkx_edges(
        G, pos, edgelist=edges, edge_color=edge_colors, width=2, connectionstyle='arc3,rad=0.2'
    )

    edge_labels = {(u, v): G[u][v]['label'] for u, v in edges}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)

    legend_elements = [
        mpatches.Patch(color='green', label='Parallel'),
        mpatches.Patch(color='blue', label='Sequential'),
        mpatches.Patch(color='red', label='Inconsistent'),
        mpatches.Patch(color='gray', label='Ambiguous')
    ]
    plt.legend(handles=legend_elements, loc='upper left', fontsize=10, title="Relationship Type")

    plt.title(f"Sibling Relationships Network Graph (Top {top_n} Pairs, Conf ≥ {min_confidence})", fontsize=16)
    plt.suptitle(f"{len(G.nodes())} operations, {len(G.edges())} sibling relationships", y=0.92, fontsize=12, color='gray')

    plt.axis('off')
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    print(f"Saved sibling network graph to {save_path}")




def main(results_file_path):

    with open(results_file_path, 'r') as f:
        results = json.load(f)

    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)

    plot_relationship_type_pie(results, save_path=os.path.join(output_dir, 'relationship_type_pie.png'))
    plot_confidence_histogram(results, save_path=os.path.join(output_dir, 'confidence_histogram.png'))
    plot_sibling_network(results, save_path=os.path.join(output_dir, 'sibling_network_graph.png'))

if __name__ == "__main__":

    results_json_path = '/home/marvilion/DCC-final-project/sibling_results.json'
    main(results_json_path)
