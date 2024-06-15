# Data Warehouses Project 2 - TPC-DI Benchmark Using DuckDB

<div align="center">
<a href="https://www.bruface.eu/">
    <img src="https://www.bruface.eu/sites/default/files/BUA-BRUFACE_RGB_Update_DEF_1_0_0.jpg" height=100"/>
</a>
</div>

## Overview
This repo is our project "TPC-DI Benchmark Using DuckDB" in the course "Data Warehouses" at Université Libre de Bruxelles (ULB). In this project, we implement the [TPC-DI Benchmark](https://www.tpc.org/tpcdi/) on [DuckDB](https://duckdb.org/) Database Management System.

## Setup
1. Clone the repo
   ```sh
   git clone https://github.com/hieunm44/dw-tpcdi-duckdb.git
   cd dw-tpcdi-duckdb
   ```
2. Install `duckdb` package
   ```sh
   pip install duckdb
   ```
3. Go to https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp and download `TPC-DI_Tools_v1.1.0.zip`, then unzip it to a folder `TPC-DI-Tool`.
4. Check the document `TPC-DI_v1.1.0.pdf` (also from the link above), the paper [TPC-DI: The First Industry Benchmark for Data Integration](http://www.vldb.org/pvldb/vol7/p1367-poess.pdf), and the slides [TPC-DI_Slides](TPC-DI_Slides.pdf) to get details about the TPC-DI benchmark.
5. Give full access permission to the data folder
   ```sh
   chmod 777 generated_data
   ```

## Usage
We only show examples for scale factor 3. Other scales can be reimplemented similarly.
1. Data generation
   ```sh
   mv Tools/PDGF Tools/pdgf
   cd TPC-DI-Tool/Tool
   java -jar DIGen.jar -sf 3 -o ../../generated_data
   ```
   Data will be generated in the folder `generated_data`.
2. Run the benchmark
   ```sh
   python3 main.py
   ```
   A database file `sf_3.db` will be created in the folder `created_db`. The result will be saved in the file `results/result_sf3.csv`.

## References
We use the following repository as a reference: https://github.com/risg99/tpc-di-benchmark.