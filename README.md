# HierDiff: Hierarchical Grouped Differential Bitmap Versioning for HTAP

This repository contains a prototype implementation and experimental evaluation code for **HierDiff**, a hierarchical grouped differential bitmap multi-version management scheme designed for **HTAP (Hybrid Transactional/Analytical Processing)** workloads. Our paper, "Optimizing Bitmap-Based Multi-Version Management in Columnar Storage for HTAP Databases", is submitted to VLDB 2026.

The codebase provides an initial standalone implementation of the bitmap-based MVCC design and its **HierDiff**-enhanced variant, which was later integrated and refined within the closed-source HexaDB system.

## File Descriptions

- **HierDiffController.h**  
  Implements the proposed **HierDiff** framework, including hierarchical grouped bitmap organization, differential bitmap encoding, concurrent version insertion, visibility-oriented garbage collection, and adaptive inter-group merging.

- **OriginalHexaDBController.h**  
  Contains a simplified implementation of the original HexaDB bitmap-based MVCC design, where each version stores a complete bitmap and versions are maintained in a single CSN-ordered chain.

- **main.cpp**  
  Provides a configurable benchmark driver that generates bitmap versions with controlled update distances, executes concurrent insert and query workloads, verifies correctness, and reports throughput statistics.

- **Makefile**  
  Defines build rules for compiling the benchmark and related components.

- **my_test.sh**  
  A helper script for cleaning, building, and running the benchmark. It also includes optional commands for memory profiling using Valgrind Massif.

