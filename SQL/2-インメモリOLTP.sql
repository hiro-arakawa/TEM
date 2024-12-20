-- メモリ最適化ファイルグループの追加
ALTER DATABASE TEM
ADD FILEGROUP MemoryOptimizedFG CONTAINS MEMORY_OPTIMIZED_DATA;

-- ファイルを追加
ALTER DATABASE TEM
ADD FILE (
    NAME = N'MemoryOptimizedFile',
    FILENAME = N'E:\SQLServerData\MemoryOptimizedFile'
) TO FILEGROUP MemoryOptimizedFG;