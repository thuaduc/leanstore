#!/bin/bash

# Loop through range_lock_variant from 0 to 3
for RANGE_LOCK_VARIANT in $(seq 0 3)
do
    # Output file to save results for the current range_lock_variant
    OUTPUT_FILE="benchmark_results_variant_${RANGE_LOCK_VARIANT}.txt"

    # Clear the output file if it exists
    > "$OUTPUT_FILE"

    # Loop through worker_count from 1 to 32
    for WORKER_COUNT in $(seq 1 32)
    do
        echo "Running benchmark with range_lock_variant=$RANGE_LOCK_VARIANT and worker_count=$WORKER_COUNT"

        # Run the benchmark program with the current worker_count and range_lock_variant value
        .buid/benchmark/LeanStore_YCSB -ycsb_read_ratio=100 -ycsb_exec_seconds=10 -ycsb_payload_size=1048576 -ycsb_record_count=1000 -bm_virtual_gb=128 -bm_physical_gb=32 -db_path=/blk/s0 -blob_logging_variant=1 -worker_count=$WORKER_COUNT -range_lock_variant=$RANGE_LOCK_VARIANT >> "$OUTPUT_FILE"

        # Add a separator for readability
        echo -e "\n\n--- End of benchmark for range_lock_variant=$RANGE_LOCK_VARIANT and worker_count=$WORKER_COUNT ---\n\n" >> "$OUTPUT_FILE"
    done

    echo "Benchmarking completed for range_lock_variant=$RANGE_LOCK_VARIANT. Results are saved in $OUTPUT_FILE."
done

echo "All benchmarking completed."