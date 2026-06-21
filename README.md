# Databricks Portfolio Data Pipeline

> **ETL pipeline rebuild of a probabilistic wealth simulator using Databricks + Delta Lake (Medallion Architecture)**

個人プロジェクト「資産形成シミュレーター」のデータ基盤レイヤーをDatabricksで再構築したポートフォリオ。  
設計から実装まで一気通貫で担当。フロントエンド（Streamlit）はそのまま流用し、データ処理基盤をSASベースの処理からDatabricks + Delta Lakeに移行した想定で設計・実装している。

---

## Why this project

SASでのETL・データ基盤構築を約8年経験してきた。  
Databricks Data Engineer Associate資格取得後、実務レベルの設計・実装力を証明するために本プロジェクトを立ち上げた。

**証明したいこと：**
- メダリオンアーキテクチャを自分で設計し、各レイヤーの責務を言語化できること
- PySpark・Delta Lake・SQLを実務水準で書けること
- 制約条件（環境・コスト）を踏まえた現実的な技術判断ができること
- READMEで「なぜこう設計したか」を説明できること

---

## Architecture Overview

```
Yahoo Finance (yfinance API)
        │
        │ incremental fetch (SPY, QQQ, VT, 1540.T)
        ▼
┌─────────────────────────────────────────────────────┐
│              Databricks (Community Edition)          │
│                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  Bronze  │───▶│  Silver  │───▶│     Gold     │  │
│  │          │    │          │    │              │  │
│  │ Raw data │    │ Cleansed │    │  Aggregated  │  │
│  │ Delta Lk │    │ PySpark  │    │  metrics     │  │
│  │ append   │    │ typed    │    │  UC Table    │  │
│  └──────────┘    └──────────┘    └──────────────┘  │
│                                         │           │
└─────────────────────────────────────────┼───────────┘
                                          │ export (Parquet)
                                          ▼
                               Streamlit (local)
                        Probabilistic Wealth Simulation
```

### Tech Stack

| Category | Tool | Role |
|---|---|---|
| Data ingestion | yfinance (Python) | Yahoo Finance API wrapper |
| Storage | Delta Lake + Unity Catalog | ACID-compliant columnar storage |
| Processing | PySpark (Databricks Serverless) | Distributed transformation |
| Aggregation | Spark SQL | Gold layer aggregation |
| Orchestration | Databricks Notebook Jobs | Pipeline scheduling |
| Frontend | Streamlit | Visualization (existing, reused) |
| Version control | GitHub | Code + design documentation |

---

## Medallion Architecture Design

### 設計思想

各レイヤーの責務を明確に分離することで、以下を実現する：

- **再現性**：Bronze層に生データを保持することで、下流レイヤーの処理を何度でも再実行できる
- **品質保証**：Silver層で型・Null・重複を一元管理し、Gold層にはビジネスロジックのみを置く
- **変更容易性**：下流の集計ロジックが変わってもBronze・Silverを変更せずに対応できる

---

### Bronze Layer — Raw Ingestion

**責務：** 生データをそのまま保持する。変換しない。

**設計判断：**
- `append-only`で運用し、過去の状態をいつでも再現できるようにする
- 増分取得ロジックにより前回取得済みの日付以降のみ取得してappendする
- スキーマを明示的に定義し、型の安定性を担保する
- パーティションは`ticker` / `year`で分割し、特定銘柄・期間のスキャンコストを削減する

**スキーマ：**

```
bronze_stock_prices (Unity Catalog Volume)
├── ticker        STRING      (e.g. "SPY", "1540.T")
├── date          DATE
├── open          DOUBLE
├── high          DOUBLE
├── low           DOUBLE
├── close         DOUBLE
├── adj_close     DOUBLE
├── volume        LONG
├── ingested_at   TIMESTAMP   (処理日時、監査用)
├── source        STRING      (= "yfinance")
└── year          INTEGER     (パーティション用)

Partition: ticker, year(date)
```

---

### Silver Layer — Cleansed & Validated

**責務：** 品質を保証する。ビジネスロジックは持たない。

**設計判断：**
- `adj_close`ベースで日次対数リターンを計算する（分割・配当調整済みのため）
- 異常値（±20%超の日次変動）はフラグを立てて除外せず保持する（除外ルールはGold層の責務）
- 重複除去はticker × dateのユニーク制約で管理する

**スキーマ：**

```
silver_stock_returns (Unity Catalog Managed Table)
├── ticker          STRING
├── date            DATE
├── adj_close       DOUBLE
├── daily_return    DOUBLE    (log return: ln(P_t / P_{t-1}))
├── is_valid        BOOLEAN   (異常値フラグ: False = 要確認)
└── processed_at    TIMESTAMP

Partition: ticker
```

**変換処理（PySpark）：**
```python
from pyspark.sql import functions as F

silver_df = (
    bronze_df
    .filter(F.col("adj_close").isNotNull())
    .dropDuplicates(["ticker", "date"])
    .withColumn(
        "daily_return",
        F.log(F.col("adj_close") / F.lag("adj_close", 1).over(window_spec))
    )
    .withColumn(
        "is_valid",
        F.abs(F.col("daily_return")) < 0.20
    )
)
```

---

### Gold Layer — Business Metrics

**責務：** Streamlitが直接消費するビジネス指標を提供する。

**設計判断：**
- 現行Streamlitアプリが計算していたロジック（年率リターン・標準偏差・相関行列）をここに移管する
- Unity Catalog Managed Tableとして定義し、下流からはSQLで参照できるようにする
- シャープレシオのリスクフリーレートは設定値として外出し管理する

**テーブル構成：**

```
gold_stats (銘柄別統計) / Unity Catalog Managed Table
├── ticker            STRING
├── annual_return     DOUBLE    (年率期待リターン: daily_mean × 252)
├── annual_volatility DOUBLE    (年率標準偏差: daily_std × √252)
├── sharpe_ratio      DOUBLE    (リスクフリーレート: 0.02 想定)
└── calc_date         DATE      (計算基準日)

gold_corr_matrix (相関行列)
├── ticker_a          STRING
├── ticker_b          STRING
├── correlation       DOUBLE
└── calc_date         DATE

gold_portfolio_metrics (ポートフォリオ加重後)
├── expected_return   DOUBLE
├── portfolio_risk    DOUBLE
├── sharpe_ratio      DOUBLE
├── weights           STRING    (JSON: {"SPY": 0.35, "QQQ": 0.40, ...})
└── calc_date         DATE
```

---

## Environment & Constraints

### 実行環境

| Item | Detail |
|---|---|
| Databricks | Community Edition（無料枠） |
| Compute | Serverless |
| Storage | Unity Catalog Volume + Managed Table |
| Frontend | Streamlit（ローカル実行） |

### Community Edition制約と対応方針

本プロジェクトはCommunity Editionで実装しているため、以下の機能は使用していない。  
ただし、本番環境への移行を想定した設計書を別途作成している（`docs/`参照）。

| 機能 | CE制約 | 対応方針 |
|---|---|---|
| Delta Live Tables (DLT) | 使用不可 | Notebookによる手動パイプラインで代替。本番ではDLTに移行する設計を`docs/dlt_design.md`に記載 |
| Unity Catalog（フル機能） | 一部制限 | Managed TableとVolumeは使用。RBAC・監査ログ等の設計を`docs/unity_catalog.md`に記載 |
| Job Scheduler（本格） | 制限あり | Notebook手動実行で対応 |

---

## Repository Structure

```
databricks-portfolio-pipeline/
│
├── README.md                        # 本ファイル
│
├── notebooks/
│   ├── 01_bronze_ingest.ipynb       # Yahoo Finance → Delta Lake (Bronze) ※増分取得対応
│   ├── 02_silver_cleanse.ipynb      # 型変換・Null処理・重複除去・リターン計算
│   ├── 03_gold_aggregate.ipynb      # 統計量・相関行列・ポートフォリオ指標
│   └── 04_export_job.ipynb          # Gold → Parquet export
│
└── docs/
    ├── dlt_design.md                # Delta Live Tables移行設計
    ├── unity_catalog.md             # Unity Catalogガバナンス設計
    └── design_decisions.md          # 設計判断ログ（なぜこう決めたか）
```

---

## Design Decisions Log

設計上の判断とその理由を記録する。詳細は`docs/design_decisions.md`を参照。

| 判断 | 選択 | 理由 |
|---|---|---|
| リターン計算式 | 対数リターン `ln(P_t/P_{t-1})` | 時間加法性があり、長期統計・モンテカルロ法に適している |
| 価格系列の基準 | `adj_close` | 分割・配当調整済みのため銘柄間・期間間の比較に公平 |
| パーティション設計 | `ticker` / `year(date)` | 銘柄単位・年単位のスキャンが最も多い想定アクセスパターンに合わせた |
| 異常値の扱い | 除外せずフラグ管理 | 除外基準はビジネス要件依存のためGold層に判断を委ねる |
| 増分取得 | 前回最大日付の翌日以降を取得 | 重複蓄積を防ぎストレージ効率を高める |
| ストレージ | Unity Catalog Volume + Managed Table | Serverless環境ではDBFS rootが無効のため |

---

## Getting Started

```bash
# 1. リポジトリをクローン
git clone https://github.com/your-username/databricks-portfolio-pipeline.git

# 2. Databricks Community Editionにノートブックをインポート
#    Workspace > New > Notebook > File > Import

# 3. Unity Catalog でスキーマ・Volumeを作成
#    Catalog > workspace > Create schema: portfolio
#    portfolio > Create Volume: raw_data
#    portfolio > Create Volume: exports

# 4. 実行順序
#    01_bronze_ingest → 02_silver_cleanse → 03_gold_aggregate → 04_export_job

# 5. Gold層のParquetをCatalog UIからダウンロードしてstreamlit_app/data/に配置

# 6. Streamlit起動
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

---

## Background

- SAS歴約8年（ETL・データ基盤構築中心）
- Databricks Data Engineer Associate取得（2025年12月）
- 本プロジェクトは「資格あり・実務経験なし」の状態から、設計・実装力を証明するために構築

---

*Built as a portfolio project to demonstrate end-to-end data engineering capability on Databricks.*
