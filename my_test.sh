make clean
make
# Run the benchmark with:
#   1 insertion thread
#   1 query thread
./main 1 1


# ----------------------------------------------------------------------
# The following commands are used for memory profiling with Valgrind
# Massif. They are commented out by default and can be enabled when
# analyzing memory consumption and peak usage.
# ----------------------------------------------------------------------

# Rebuild the program before profiling
# make clean
# make

# Run the benchmark under Valgrind Massif to track heap usage over time.
#   --threshold        records even small allocation changes
#   --time-unit=ms     reports memory usage in milliseconds
#   --detailed-freq=1  captures detailed snapshots at every time point
#   --massif-out-file  specifies the output file
#
# The workload uses:
#   1 insertion thread
#   1 query threads
# valgrind --tool=massif --threshold=0.000001 --time-unit=ms \
#          --detailed-freq=1 --massif-out-file=massif.out \
#          ./main 1 1

# Convert Massif output into a human-readable report
# ms_print massif.out > massif_report.txt

# Visualize memory usage over time using Massif Visualizer
# massif-visualizer massif.out


