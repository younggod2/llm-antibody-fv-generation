import pandas as pd
import glob
import os


# Пороги аффинности: (порог, оператор)
AFFINITY_THRESHOLDS = {
    'fuzzy': ('h', '=='),
    'bool': (1, '=='),
    'alphaseq': (2, '<'),
    '-log KD': (7, '>'),
    'kd': (100, '<'),
    'delta_g': (-9.5, '<'),
    'log_enrichment': (1, '>'),
    'elisa_mut_to_wt_ratio': (1, '>'),
    'ic_50': (100, '<'),
}


def load_parquet_folder(data_path: str) -> pd.DataFrame:
    """Загружает все parquet файлы из папки в один DataFrame"""
    parquet_files = glob.glob(os.path.join(data_path, "part-*.parquet"))
    if not parquet_files:
        raise ValueError(f"Не найдено parquet файлов в папке {data_path}")
    
    print(f"Найдено {len(parquet_files)} parquet файлов")
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    print(f"Общий размер данных: {df.shape}")
    return df


def filter_basic(df: pd.DataFrame) -> pd.DataFrame:
    """Базовая фильтрация: не nanobody, high confidence, обе цепи или scfv"""
    mask = (
        (df['nanobody'] == False)
        # & df['confidence'].isin(['high', 'very_high'])
        & (
            (df['scfv'] == True)
            | (
                df['light_sequence'].notna() 
                & df['heavy_sequence'].notna()
                & (df['light_sequence'] != '')
                & (df['heavy_sequence'] != '')
            )
        )
    )
    return df[mask]


def filter_by_affinity(df: pd.DataFrame) -> pd.DataFrame:
    """Фильтрация по порогам аффинности"""
    numeric_affinity = pd.to_numeric(df['affinity'], errors='coerce')
    
    masks = []
    for aff_type, (threshold, op) in AFFINITY_THRESHOLDS.items():
        type_match = df['affinity_type'] == aff_type
        
        if op == '==':
            masks.append(type_match & (df['affinity'] == threshold))
        elif op == '<':
            masks.append(type_match & (numeric_affinity < threshold))
        elif op == '>':
            masks.append(type_match & (numeric_affinity > threshold))
    
    # Объединяем все маски через OR
    combined = masks[0]
    for m in masks[1:]:
        combined |= m
    
    return df[combined]


def main():
    """Основной пайплайн обработки данных AgAb."""
    df = load_parquet_folder('./data/asd')
    
    df = filter_basic(df)
    print(f"После базовых фильтров: {df.shape}")
    
    df = filter_by_affinity(df)
    print(f"После фильтрации по аффинности: {df.shape}")
    
    df = df.drop_duplicates(
        subset=['heavy_sequence', 'light_sequence', 'antigen_sequence'],
        keep='first'
    )
    print(f"После удаления дубликатов: {df.shape}")
    
    output_path = '/Users/denischekalin/Desktop/Cursor/Antibody/data/agab_filtered.parquet'
    df.to_parquet(output_path, index=False, engine='pyarrow')
    print(f"Сохранено в {output_path}")


if __name__ == '__main__':
    main()
