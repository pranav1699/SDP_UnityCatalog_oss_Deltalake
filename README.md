# Unity Catalog Delta SDP

A sample medallion data pipeline built with Apache Spark Declarative Pipelines (SDP), Spark Connect, Delta Lake, and Unity Catalog. The pipeline ingests store sales, customer, item, and store data into bronze, cleans and enriches it in silver, and produces sales and customer analytics in gold.

## Architecture

| Layer | Purpose | Example tables |
| --- | --- | --- |
| Bronze | Read source tables from Unity Catalog as streaming tables | `demo.sdp_bronze.stores_sales_bronze`, `customers_bronze`, `item_bronze`, `stores_bronze` |
| Silver | Filter and clean sales, customer, and item records, then enrich sales with dimensions | `store_sales_silver`, `customer_silver`, `item_silver`, `silver_sales_enriched` |
| Gold | Publish aggregated datasets for reporting and analysis | `demo.sdp_gold.gold_sales_summary`, `gold_customer_analytics` |

The pipeline definition is in [`demo_sdp_uc/spark-pipeline.yml`](demo_sdp_uc/spark-pipeline.yml). It connects to Spark Connect at `sc://localhost:15002` and loads Python transformations from `demo_sdp_uc/transformations/`.

## Prerequisites

- Java 17, 21, or 25
- Python 3.12 (the project setup below uses `uv`)
- Bun, for the Unity Catalog UI
- Git

Spark 4.2.1 uses Scala 2.13. Use the Spark 4.2.1 binary distribution that includes Spark Connect. See the [Apache Spark download page](https://spark.apache.org/downloads.html) and [Spark 4.2.1 release notes](https://spark.apache.org/releases/spark-release-4-2-1.html).

## 1. Download and unpack Spark 4.2.1

Download the Spark 4.2.1 binary distribution with Spark Connect enabled from the Apache Spark downloads page. For example, if the archive is in your downloads directory:

```bash
tar -xzf ~/Downloads/spark-4.2.1-bin-hadoop3-connect.tgz
cd spark-4.2.1-bin-hadoop3-connect
```

Keep this terminal in the Spark installation directory for the Spark Connect server step below. If the archive filename differs, use the name of the archive you downloaded.

## 2. Start Unity Catalog

In a separate terminal, clone Unity Catalog and start its local server:

```bash
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog
bin/start-uc-server
```

The Unity Catalog server listens at `http://localhost:8080` by default. Keep it running while using Spark and the pipeline.

## 3. Start the Unity Catalog UI (optional)

In another terminal, from the Unity Catalog checkout:

```bash
cd ui
bun install
bun run start
```

Open the local URL printed by the UI development server in your browser.

## 4. Create the Unity Catalog catalog and schema

In a terminal at the root of the Unity Catalog checkout, create the catalog and schema used by this project:

```bash
./bin/uc catalog create --name demo --storage_root /tmp/demo_storage_root
./bin/uc schema create --catalog demo --name default
```

The pipeline also writes its bronze, silver, and gold tables under the `demo` catalog. The source tables expected by the bronze transformations are `demo.sdp.store_sales`, `demo.sdp.customer`, `demo.sdp.item`, and `demo.sdp.store`; make sure these source tables exist and contain data before running the pipeline.

## 5. Start Spark Connect with Delta and Unity Catalog

In the Spark installation directory from step 1, run:

```bash
./sbin/start-connect-server.sh \
  --packages io.unitycatalog:unitycatalog-spark_4.2_2.13:0.7.0,io.delta:delta-spark_4.2_2.13:4.4.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.demo=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.demo.uri=http://localhost:8080 \
  --conf spark.sql.catalog.demo.auth.type=static \
  --conf spark.sql.catalog.demo.auth.token=not-used
```

This loads the Unity Catalog Spark integration and Delta Lake packages, enables the Delta SQL extension, and registers the `demo` Unity Catalog. Leave this server running. The project configuration expects it at `sc://localhost:15002`.

## 6. Set up the Python environment with uv

Install [`uv`](https://docs.astral.sh/uv/) if it is not already installed. From the repository root, create and activate a Python 3.12 virtual environment, then install the project client:

```bash
uv venv --python 3.12
source .venv/bin/activate
uv pip install "pyspark[pipelines]==4.2.1"
```

If you prefer not to activate the virtual environment, `uvx` can run the CLI in a temporary environment with the same Python and PySpark versions:

```bash
uvx --python 3.12 --from "pyspark[pipelines]==4.2.1" spark-pipelines --help
```

The transformation modules import `pyspark.pipelines` and use the active Spark Connect session. Keep the server and client on the same Spark release: use Spark 4.2.1 on the Connect server as well as the PySpark 4.2.1 client.

## 7. Run the pipeline

The current pipeline spec has a machine-specific `storage` path. Before running it on your machine, edit `storage` in [`demo_sdp_uc/spark-pipeline.yml`](demo_sdp_uc/spark-pipeline.yml) to an absolute local path where the pipeline can keep checkpoints. Then run:

```bash
cd demo_sdp_uc
uvx --python 3.12 --from "pyspark[pipelines]==4.2.1" spark-pipelines run --remote sc://localhost:15002 --spec spark-pipeline.yml
```

This launches the pipeline and monitors it until the update completes. Its configuration reads source data from `demo.sdp`, writes bronze and silver datasets to `demo.sdp_bronze`, and writes analytics datasets to `demo.sdp_gold`. To validate the pipeline definitions without processing or writing data, run:

```bash
uvx --python 3.12 --from "pyspark[pipelines]==4.2.1" spark-pipelines dry-run --remote sc://localhost:15002 --spec spark-pipeline.yml
```

After a successful update, verify the output tables in Unity Catalog or query them from a Spark Connect client:

```sql
SELECT * FROM demo.sdp_gold.gold_sales_summary;
SELECT * FROM demo.sdp_gold.gold_customer_analytics;
```

## Configuration notes

- The local Spark Connect endpoint is configured in `demo_sdp_uc/spark-pipeline.yml` as `sc://localhost:15002`.
- The catalog URI is `http://localhost:8080`, matching the local Unity Catalog server.
- The static token value `not-used` is appropriate only for this local development setup.
- Pipeline checkpoints and generated state are written under `demo_sdp_uc/pipeline-storage/`, which is excluded from Git.
- The storage root `/tmp/demo_storage_root` is local to the machine running Unity Catalog. Use a suitable shared storage location when running outside local development.

## Repository layout

```text
demo_sdp_uc/
├── spark-pipeline.yml
└── transformations/
    ├── bronze/
    ├── silver/
    └── gold/
```
