# File format
## Headers

| Field name       | Description                      | Type |
| ---------------- |:---------------------------------|-----:|
| magic            | Magic number                     | u32  |
| version          | Version number                   | u8   |
| timestamp        | Creation timestamp               | i64  |
| si_base_offset   | Where the sparse index begins    | u64  |
| di_base_offset   | Where the dense index begins     | u64  |
| data_base_offset | Where the compressed data begins | u64  |
| num_entries      | Number of entries in file        | u64  |

## Sparse Index
| Key    | DI Offset |
|--------|-----------|
| h_0000 | di_off_1  |
| h_1000 | di_off_2  |
| h_2000 | di_off_3  |
| ...    | ...       |
| h_xxxx | di_off_x  |

## Dense Index
| DI Offset | Key    | Data Offset |
|-----------|--------|-------------|
| di_off_1  | h_0000 | data_off_1  |
|           | h_0001 | data_off_2  |
|           | h_0013 | data_off_3  |
| ...       | ...    | ...         |
|           | h_0988 | data_off_4  |
| di_off_2  | h_1000 | data_off_5  |
|           | h_1003 | data_off_6  |
| ...       | ...    | ...         |
| di_off_x  | h_xxxx | data_off_x  |

## Data
| Data Offset | Data  |
|-------------|-------|
| data_off_1  | LZ4_1 |
| data_off_2  | LZ4_2 |
| data_off_3  | LZ4_3 |
| ...         | ...   |
| data_off_x  | LZ4_x |

# Explanation
A binstore file is split in four sections:

1. The headers.  The headers help us identify a binstore file (via the
   magic number), they allow us to know whether the current binstore
   engine can read a given binstore file (version number), and when the
   binstore file was created.  We also store the offsets for the other
   sections and the number of entries stored in this data file.
2. The sparse index.  An index of no more than 1-2 MB; the sparse is
   used to jump into the dense index at roughly the spot where the key
   we are looking for is located.  The sparse index is essential to
   avoid a full scan.
3. The dense index.  In the dense index, there is a mapping from a key
   (keys are explained below) to the file offset where the set of
   `Value`s associated with that key is stored.  The dense index entries
   are of fixed sized and are ordered by their keys; this enables
   binary searching.
4. The data.  This is where the actual `Value`s are stored.  To save
   space, we use the LZ4 compression algorithm.
