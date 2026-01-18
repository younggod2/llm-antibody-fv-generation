"""
Helper module for antibody processing.
Contains functions that need to be pickled for multiprocessing.
"""
from anarci import anarci

def run_anarci_batch(sequences):
    """Helper function to run ANARCI in a separate process."""
    try:
        return anarci(sequences, scheme="imgt", ncpu=1, assign_germline=True)
    except Exception as e:
        return None, None, str(e)
