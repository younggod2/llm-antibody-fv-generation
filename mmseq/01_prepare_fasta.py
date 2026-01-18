import pandas as pd
import hashlib
import os
import json

def get_md5(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def main():
    # Define paths assuming script is run from project root or inside mmseq
    # Adjusting to run from project root typically
    if os.path.exists('data/agab.parquet'):
        input_path = 'data/agab.parquet'
        output_dir = 'mmseq'
    elif os.path.exists('../data/agab.parquet'):
        input_path = '../data/agab.parquet'
        output_dir = '.'
    else:
        raise FileNotFoundError("Could not find data/agab.parquet")

    print(f"Reading {input_path}...")
    df = pd.read_parquet(input_path)
    
    if 'antigen_sequence' not in df.columns:
        raise ValueError("Column 'antigen_sequence' not found in DataFrame")

    # Get unique sequences, drop None/NaN
    sequences = df['antigen_sequence'].dropna().unique()
    print(f"Found {len(sequences)} unique antigen sequences.")

    fasta_path = os.path.join(output_dir, 'antigens.fasta')
    mapping_path = os.path.join(output_dir, 'antigen_mapping.json')

    mapping = {}
    
    print(f"Writing FASTA to {fasta_path}...")
    with open(fasta_path, 'w') as f:
        for seq in sequences:
            # Skip empty strings if any
            if not seq:
                continue
                
            seq_hash = get_md5(seq)
            mapping[seq_hash] = seq
            
            f.write(f">{seq_hash}\n{seq}\n")

    print(f"Writing mapping to {mapping_path}...")
    with open(mapping_path, 'w') as f:
        json.dump(mapping, f, indent=2)

    print("Done.")

if __name__ == "__main__":
    main()
