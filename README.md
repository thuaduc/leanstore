# LeanStore

[LeanStore](https://leanstore.io) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling

Install dependencies:

`apt install cmake libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev liblmdb-dev libwiredtiger-dev liburing-dev libboost-context-dev libnvme-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`

## TPC-C Example

`./frontend/tpcc --ssd_path="/blk/k0" --ioengine=io_uring --nopp=true --partition_bits=12 --tpcc_warehouse_count=100 --run_for_seconds=60 --dram_gib=2 --worker_tasks=32 --async_batch_size=32 --optimistic_parent_pointer=1 --xmerge=1 --contention_split=1 --worker_threads=20 --pp_threads=20`

check `./frontend/tpcc --help` for other options

To use multiple SSDs you can use semicolon to separate them, LeanStore will stripe data over all SSDs: `--ssd_path="/blk/k0;/blk/k1"`

## YCSB Example

`./frontend/ycsb --ssd_path="/blk/k0" --ioengine=io_uring --partition_bits=12 --target_gib=20 --ycsb_read_ratio=100 --run_for_seconds=60 --dram_gib=2 --worker_tasks=128 --optimistic_parent_pointer=1 --xmerge=1 --contention_split=1 --nopp --worker_threads=20 --pp_threads=20`

## SPDK

You will have to install SPDK dependencies and setup the SSDs (i.e.: build/vendor/spdk/src/spdk-ext/scripts/setup.sh), see the [SPDK documentation](https://spdk.io/doc/getting_started.html) to get started with it.

Compile with SPDK:

`cmake -DCMAKE_BUILD_TYPE=Release -DLEANSTORE_INCLUDE_SPDK=ON ..`

Set ioengine and use correct PCIe path:

`./frontend/tpcc --ssd_path="traddr=0000.c7.00.0" --ioengine=spdk --nopp=true --partition_bits=12 --tpcc_warehouse_count=100 --run_for_seconds=60 --dram_gib=2 --worker_tasks=32 --async_batch_size=32 --optimistic_parent_pointer=1 --xmerge=1 --contention_split=1 --worker_threads=1 --pp_threads=1`

## Implemented Features in this Branch

- [x] Lightweight buffer manager with pointer swizzling [ICDE18]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE19]
- [x] Variable-length key/values B-Tree with prefix compression and hints  [BTW23]
- [x] What Modern NVMe Storage Can Do, And How to Exploit It: High-Performance {I/O} for High-Performance Storage Engines [VLDB 2023 paper](https://www.vldb.org/pvldb/vol16/p2090-haas.pdf)

## Cite

The code in this branch we used for our [VLDB 2023 paper](https://www.vldb.org/pvldb/vol16/p2090-haas.pdf) that covers the fast I/O implementation:

```BibTeX
@article{haas23,
  author       = {Gabriel Haas and Viktor Leis},
  title        = {What Modern NVMe Storage Can Do, And How to Exploit It: High-Performance {I/O} for High-Performance Storage Engines},
  journal      = {Proc. {VLDB} Endow.},
  year         = {2023}
}
```

