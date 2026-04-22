# Titanic Medallion Pipeline — Microsoft Fabric

A complete bronze → silver → gold medallion architecture implemented as three PySpark notebooks for Microsoft Fabric. Uses the classic 891-passenger Titanic dataset to demonstrate a full pipeline from raw CSV ingestion through to a Power BI-ready star schema.

Originally built as the core demo for a **Fabric vs Databricks** conference talk.

---

## What's in this repo

```
.
├── 01_Bronze_Titanic.py      # Raw CSV → Delta with audit columns
├── 02_Silver_Titanic.py      # Cleansed, conformed passenger table
├── 03_Gold_Titanic.py        # Star schema: 1 fact + 4 dimensions
└── README.md
```

Three Fabric-flavoured Python notebooks. Each file begins with Fabric's notebook metadata comment block and uses `# CELL ********************` markers to denote cell boundaries — when you paste these into a new Fabric notebook, split on those markers.

---

## What each notebook does

### `01_Bronze_Titanic.py`

Reads the raw Titanic CSV from the Lakehouse `Files/landing/titanic/titanic.csv` path with an explicit schema (strings for messy columns, numerics for clean ones), then writes it to a Delta table `bronze_titanic` with three audit columns:

- `_ingested_at` — timestamp of the Bronze load
- `_source_file` — originating file path
- `_pipeline_run_id` — passed in from the orchestrating pipeline for lineage

**Rules followed:** no business logic, no type fixes beyond what the schema declares, no renames. Bronze is deliberately dumb — its job is lineage and reproducibility, not transformation.

**Mode:** overwrite. For production you'd switch to append + watermark, but for a demo that makes re-runs idempotent.

### `02_Silver_Titanic.py`

Transforms `bronze_titanic` into `silver_passenger` — analyst-friendly, one row per passenger, cleansed and conformed. Key transformations:

- **Title extraction** — parses honorifics out of the `Name` column (`"Cumings, Mrs. John Bradley"` → `Mrs`) and collapses rare titles (Dr, Rev, Capt, Col, Sir, Lady, etc.) into a single `Rare` bucket. Normalises French variants (Mlle → Miss, Mme → Mrs).
- **Age imputation** — fills missing ages using the **median per (Pclass, Sex) group** via a window function, then an overall-median fallback. This is significantly better than a naive global mean.
- **Readable mappings** — `Sex` (male/female → Male/Female), `Embarked` (S/C/Q → Southampton/Cherbourg/Queenstown), `Pclass` (1/2/3 → First/Second/Third).
- **Derivations** — `FamilySize` (SibSp + Parch + 1, *including the passenger*), `IsAlone`, `AgeGroup` (Child/Teen/Adult/MiddleAged/Senior), `CabinDeck` (first letter of Cabin, or Unknown).
- **DQ gates** — assertions fail the notebook if duplicate or null `PassengerId`s appear, or if the row count drops to zero. These run before the write, so a DQ failure aborts without corrupting the table.

### `03_Gold_Titanic.py`

Builds a star schema from `silver_passenger`:

```
                     ┌──────────────────┐
                     │  dim_passenger   │
                     │ PassengerKey PK  │
                     └────────┬─────────┘
                              │
┌──────────────┐     ┌────────▼──────────────────┐     ┌──────────────┐
│  dim_class   ├─────┤ fact_passenger_voyage     ├─────┤   dim_port   │
│ ClassKey PK  │     │ PassengerKey FK           │     │ PortKey PK   │
└──────────────┘     │ ClassKey     FK           │     └──────────────┘
                     │ PortKey      FK           │
                     │ DateKey      FK           │
                     │ Fare, FarePerPerson       │     ┌──────────────┐
                     │ SurvivedFlag, Count       ├─────┤   dim_date   │
                     └───────────────────────────┘     │ DateKey PK   │
                                                       └──────────────┘
```

**Fact grain:** one row per passenger per voyage. Titanic only sailed once, so effectively one row per passenger — but the model is structured correctly so additional voyages would slot in without reshaping.

**Surrogate keys:** generated via `row_number()` over a stable sort. Production systems might use a sequence/identity column, but `row_number()` is deterministic and fine for an idempotent demo.

**Measures chosen deliberately for BI-friendliness:**
- `SurvivedFlag` (0/1) — sums to count of survivors, averages to a survival rate
- `PassengerCount` (always 1) — explicit fact counter; easier for Power BI users than `COUNT(*)`
- `Fare` is additive; `FarePerPerson = Fare / FamilySize` is semi-additive (only averages are meaningful)

At the end the notebook runs a sanity check — survival rate by class and sex — which should produce the canonical Titanic result: first-class women survived at ~97%, third-class men at ~13%.

---

## Deployment — what you need to build around the notebooks

The notebooks are the transformation layer. You also need a Lakehouse, an ingestion path, and a pipeline to orchestrate it all. Total setup time: ~15 minutes.

### 1. Create a Fabric workspace and Lakehouse

1. In a Fabric-capable workspace, **+ New item → Lakehouse**, name it `lh_titanic`.
2. In the Lakehouse explorer, under **Files**, create the folder path `landing/titanic`.

### 2. Import the three notebooks

For each `.py` file:

1. **+ New item → Notebook**, name it to match (`01_Bronze_Titanic`, etc.).
2. Paste the file contents into the new notebook. Split on `# CELL ********************` markers to recreate the cell boundaries.
3. In the left pane of the notebook, click **Add lakehouse → Existing → `lh_titanic`**, and mark it as the default lakehouse. The notebooks won't resolve table names without this.

### 3. Build the data pipeline

A Fabric Data Pipeline to orchestrate ingestion + the three notebooks in sequence.

1. **+ New item → Data pipeline**, name it `pl_titanic_medallion`.
2. **Add activity → Copy data**:
   - **Source:** HTTP connector
     - Base URL: `https://raw.githubusercontent.com/`
     - Relative URL: `datasciencedojo/datasets/master/titanic.csv`
     - Anonymous authentication
     - Format: CSV, header row enabled, quote `"`, escape `"` (critical — some passenger names contain commas inside quoted strings)
   - **Destination:** Lakehouse `lh_titanic`
     - Path: `landing/titanic`
     - Filename: `titanic.csv`
     - Format: CSV with header
3. Add three **Notebook** activities in sequence, each pointing at the corresponding notebook. Chain them with "on success" arrows: `Copy → Bronze → Silver → Gold`.
4. On the Bronze notebook activity, expand **Settings → Base parameters** and pass `pipeline_run_id = @pipeline().RunId` so lineage flows through.
5. Save and run.

### 4. Verify

In the Lakehouse explorer you should see:

- `Files/landing/titanic/titanic.csv`
- Tables: `bronze_titanic`, `silver_passenger`, `gold_dim_passenger`, `gold_dim_class`, `gold_dim_port`, `gold_dim_date`, `gold_fact_passenger_voyage`

Then open the SQL analytics endpoint and run:

```sql
SELECT c.ClassName, p.Sex,
       COUNT(*) AS Passengers,
       SUM(f.SurvivedFlag) AS Survivors,
       ROUND(AVG(f.SurvivedFlag) * 100, 1) AS SurvivalRatePct
FROM gold_fact_passenger_voyage f
JOIN gold_dim_passenger p ON f.PassengerKey = p.PassengerKey
JOIN gold_dim_class     c ON f.ClassKey     = c.ClassKey
GROUP BY c.ClassName, p.Sex
ORDER BY c.ClassName, p.Sex;
```

If the first row (`First, Female`) shows ~97% survival and the last row (`Third, Male`) shows ~13%, the pipeline is correct.

---

## Optional downstream layers (not in this repo)

Once the gold tables exist, there are several useful things to build on top. None of them are in this repo — they're configured in the Fabric portal rather than being source-controllable notebooks — but they're worth knowing about if you're extending the demo.

- **Semantic model** — from the Lakehouse, **New semantic model** over the gold tables. Creates a DirectLake model you can point Power BI at. Needs manual relationship setup (Fabric auto-detection is naive for star schemas) and some DAX measures.
- **Power BI report** — built on the semantic model. The canonical slicers are Class, Sex, AgeGroup, and Port of embarkation.
- **Data Agent** — natural-language Q&A over the semantic model. Useful demo moment: "how did survival rate vary by class?" gets answered conversationally.
- **Ontology** — a graph layer over the Gold tables (Passenger / PassengerClass / Port entities with `travelledIn` / `embarkedFrom` relationships). Preview feature as of writing; check your tenant has it enabled.
- **Teams integration** — the Data Agent can be exposed in Microsoft Teams via Copilot Studio, giving non-technical users conversational access.

---

## Data source notes

The pipeline pulls from `raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv`. This is the standard anonymous-access mirror used across most Titanic tutorials — stable for years, no authentication required.

**Why not Kaggle?** Kaggle requires a `kaggle.json` API credential file loaded into a Fabric connection. Doable, but adds a 10-minute setup tangent for a demo. For production realism, swap the HTTP source for an authenticated Azure Blob Storage connection you own.

---

## Known limitations and design choices

A few honest things to flag if you're adapting this:

- **Overwrite mode everywhere.** Every notebook uses `mode("overwrite")`. This makes the demo idempotent (you can re-run end-to-end without state drift) but isn't a production pattern. For real workloads you'd want append + watermark for Bronze, and merge/upsert for Silver and Gold.
- **No incremental processing.** Everything is a full rebuild each run. Fine for 891 rows. Would not scale to a real dataset of any size.
- **`row_number()` surrogate keys depend on stable sort.** If the Silver rows are reordered between runs, the `PassengerKey` values in `gold_dim_passenger` will shuffle. The fact-dim joins still work because they happen in the same notebook run, but downstream systems caching old keys would break. Production would use an identity column or a persistent key lookup table.
- **DQ failures abort the pipeline, not reroute to a quarantine.** Simpler and louder, but less graceful than a proper DLQ pattern.
- **Sort-by-column in the semantic model requires an extra column.** Fabric's DirectLake doesn't support calculated columns, so if you want `AgeGroup` to sort in order (Child / Teen / Adult / MiddleAged / Senior) rather than alphabetically, you'll need to add an `AgeGroupSort` column in the Gold notebook. This isn't included here because it's a semantic-model concern rather than a data one — decide based on your use case.
- **Age imputation uses `percentile_approx` — approximate, not exact.** For 891 rows the difference is invisible. For large datasets, exact medians would be slower but the approximation gets more reliable, so this is a genuine win for scale.

---

## Testing

The notebooks have been validated end-to-end locally with PySpark 3.5 using synthetic Titanic-shaped data. The classic survival pattern (first-class women near 100%, third-class men near 0%) emerges correctly.

There's no unit test file in this repo — adding one is a useful next step. Recommended approach: build a pytest fixture that generates a small Titanic-shaped DataFrame, then test the Silver transforms (title extraction, age imputation, derivations) as individual functions. This is one of the genuine advantages of the notebook approach over low-code Dataflow Gen2 equivalents — Python code is unit-testable.

---

## Licence and attribution

The Titanic dataset itself is public domain (Encyclopedia Titanica, Kaggle redistribution, datasciencedojo mirror). The notebooks in this repo are free to reuse and adapt for any purpose.
