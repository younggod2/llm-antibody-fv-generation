#!/bin/bash

# Ensure we are in the directory where the script is located or handle paths correctly
# This script assumes it is run in the directory containing antigens.fasta or paths are adjusted

# Check if mmseqs is installed
if ! command -v mmseqs &> /dev/null
then
    echo "mmseqs could not be found. Please ensure it is in your PATH."
    # Try local bin if likely
    if [ -f "./mmseqs/bin/mmseqs" ]; then
        export PATH=$(pwd)/mmseqs/bin/:$PATH
        echo "Added local mmseqs to PATH."
    else
        echo "If you installed it locally, run: export PATH=\$(pwd)/mmseqs/bin/:\$PATH"
        exit 1
    fi
fi

echo "Running mmseqs easy-cluster..."

# Parameters Explanation:
# easy-cluster   : More sensitive than linclust, suitable for dataset size (~9k).
# --min-seq-id 0.4 : Strict threshold (40%) to ensure train/test separation (generalization).
# -c 0.8         : 80% alignment coverage required (avoid matching small domains only).
# --cov-mode 0   : Coverage must be satisfied for BOTH sequences (bidirectional).

mmseqs easy-cluster antigens.fasta cluster_results tmp \
    --min-seq-id 0.4 \
    -c 0.8 \
    --cov-mode 0

echo "Done. Results should be in cluster_results_cluster.tsv"
