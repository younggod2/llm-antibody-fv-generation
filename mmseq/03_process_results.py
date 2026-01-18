import pandas as pd
import json
import os

def main():
    # Define paths
    if os.path.exists('data/agab.parquet'):
        data_path = 'data/agab.parquet'
        output_data_path = 'data/agab_mmseq.parquet'
        work_dir = 'mmseq'
    elif os.path.exists('../data/agab.parquet'):
        data_path = '../data/agab.parquet'
        output_data_path = '../data/agab_mmseq.parquet'
        work_dir = '.'
    else:
        raise FileNotFoundError("Could not find data/agab.parquet")

    mapping_path = os.path.join(work_dir, 'antigen_mapping.json')
    cluster_tsv_path = os.path.join(work_dir, 'cluster_results_cluster.tsv')

    if not os.path.exists(cluster_tsv_path):
        print(f"Cluster results not found at {cluster_tsv_path}. Make sure you ran the mmseqs script.")
        return

    print("Loading mapping...")
    with open(mapping_path, 'r') as f:
        hash_to_seq = json.load(f)

    print("Loading cluster results...")
    # mmseqs output format: representative <tab> member
    clusters = pd.read_csv(cluster_tsv_path, sep='\t', names=['representative_hash', 'member_hash'])

    # Create a map: member_seq -> representative_seq
    # This allows us to group sequences by their cluster representative
    
    seq_to_rep_map = {}
    
    print("Processing clusters...")
    for _, row in clusters.iterrows():
        rep_hash = str(row['representative_hash'])
        mem_hash = str(row['member_hash'])
        
        rep_seq = hash_to_seq.get(rep_hash)
        mem_seq = hash_to_seq.get(mem_hash)
        
        if rep_seq and mem_seq:
            seq_to_rep_map[mem_seq] = rep_seq

    print(f"Loading {data_path}...")
    df = pd.read_parquet(data_path)

    print("Mapping representative sequences...")
    # Add 'mmseq_cluster_rep' column.
    # This column will contain the SEQUENCE of the cluster representative.
    # All antigens in the same cluster will have the SAME value in this column.
    
    df['mmseq_cluster_rep'] = df['antigen_sequence'].map(seq_to_rep_map)
    
    # Check coverage
    missing_count = df['mmseq_cluster_rep'].isna().sum()
    if missing_count > 0:
        print(f"Warning: {missing_count} rows have antigen sequences not found in clustering results.")
        # Optional: fill with self if missing? 
        # df['mmseq_cluster_rep'] = df['mmseq_cluster_rep'].fillna(df['antigen_sequence'])
    
    print(f"Saving to {output_data_path}...")
    df.to_parquet(output_data_path)
    
    # Stats
    n_clusters = df['mmseq_cluster_rep'].nunique()
    print(f"Total rows: {len(df)}")
    print(f"Total unique clusters (representatives): {n_clusters}")
    print("Done. Use 'mmseq_cluster_rep' column to group data for Train/Test split.")

if __name__ == "__main__":
    main()
