# Delta Live Tables (DLT) 移行設計

本プロジェクトはDatabricks Community Editionで実装しているため、Delta Live Tables（DLT）は使用していない。  
本ドキュメントでは、本番環境へ移行する際のDLT設計を示す。

---

## DLTとは

Delta Live Tablesは、Databricksが提供するデータパイプラインの宣言的フレームワーク。

**現在の実装（Notebook手動パイプライン）との違い：**

| 項目 | 現在（Notebook） | 本番（DLT） |
|---|---|---|
| パイプライン定義 | 手続き的（how to） | 宣言的（what to） |
| 依存関係管理 | 手動（実行順序を明示） | 自動（DLTが解決） |
| エラーハンドリング | 手動実装が必要 | 自動リトライ・アラート |
| データ品質管理 | コード内でアサーション | Expectationsで宣言的に定義 |
| 監視・可観測性 | 手動でログ確認 | DLT UIで一元管理 |
| スケジューリング | Job Schedulerと別管理 | DLTパイプライン内で完結 |

---

## 現在の実装からDLTへの移行設計

### Bronze層の移行

**現在の実装：**
```python
# 01_bronze_ingest.ipynb
sdf_bronze.write.format("delta").mode("append").partitionBy("ticker", "year").save(BRONZE_PATH)
```

**DLT移行後：**
```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_stock_prices",
    comment="Yahoo Financeから取得した生の株価データ。変換・クレンジングは行わない。",
    table_properties={"quality": "bronze"}
)
def bronze_stock_prices():
    # 増分取得ロジックをDLT Auto Loaderに置き換える
    return (
        spark.readStream
        .format("cloudFiles")          # Auto Loader: 新規ファイルを自動検知
        .option("cloudFiles.format", "json")
        .load(RAW_DATA_PATH)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit("yfinance"))
        .withColumn("year", F.year(F.col("date")))
    )
```

**移行のポイント：**
- `write`→`@dlt.table`デコレータによる宣言的定義
- append-onlyの増分制御はDLT Auto Loaderが自動的に処理する
- パーティション設定はtable_propertiesで管理

---

### Silver層の移行

**現在の実装：**
```python
# 02_silver_cleanse.ipynb
sdf_silver.write.format("delta").mode("overwrite").partitionBy("ticker").saveAsTable(SILVER_TABLE)
```

**DLT移行後：**
```python
@dlt.table(
    name="silver_stock_returns",
    comment="クレンジング済み株価データ。対数リターン・異常値フラグを付与。",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_adj_close", "adj_close IS NOT NULL")          # Null除去の保証
@dlt.expect("valid_date", "date IS NOT NULL")                    # 日付の保証
@dlt.expect_or_drop("no_future_date", "date <= current_date()") # 未来日付を除外
def silver_stock_returns():
    w_dedup   = Window.partitionBy("ticker", "date").orderBy(F.desc("ingested_at"))
    w_ffill   = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    w_return  = Window.partitionBy("ticker").orderBy("date")

    return (
        dlt.read_stream("bronze_stock_prices")  # Bronze層を参照
        .withColumn("row_num", F.row_number().over(w_dedup))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("adj_close", F.last(F.col("adj_close"), ignorenulls=True).over(w_ffill))
        .withColumn("daily_return", F.log(F.col("adj_close") / F.lag("adj_close", 1).over(w_return)))
        .withColumn("is_valid", F.when(F.col("daily_return").isNull(), F.lit(None).cast("boolean"))
                                 .otherwise(F.abs(F.col("daily_return")) < 0.20))
        .select("ticker", "date", "adj_close", "daily_return", "is_valid")
    )
```

**移行のポイント：**
- `@dlt.expect`によるデータ品質ルールの宣言的定義（Expectations）
- `dlt.read_stream()`でBronze層への依存関係を宣言
- DLTが自動的にBronze→Silverの実行順序を解決する

---

### Gold層の移行

**現在の実装：**
```python
# 03_gold_aggregate.ipynb
sdf_stats.write.format("delta").mode("overwrite").saveAsTable(GOLD_STATS_TABLE)
```

**DLT移行後：**
```python
@dlt.table(
    name="gold_stats",
    comment="銘柄別年率統計。Streamlitが直接消費するビジネス指標。",
    table_properties={"quality": "gold"}
)
def gold_stats():
    return (
        dlt.read("silver_stock_returns")   # Silver層を参照（バッチ読み込み）
        .filter(F.col("is_valid") == True)
        .filter(F.col("daily_return").isNotNull())
        .groupBy("ticker")
        .agg(
            (F.mean("daily_return") * 252).alias("annual_return"),
            (F.stddev("daily_return") * F.sqrt(F.lit(252))).alias("annual_volatility"),
            F.min("date").alias("start_date"),
            F.max("date").alias("end_date"),
            F.count("*").alias("sample_days"),
        )
        .withColumn(
            "sharpe_ratio",
            (F.col("annual_return") - F.lit(RISK_FREE_RATE)) / F.col("annual_volatility")
        )
        .withColumn("calc_date", F.current_date())
    )
```

---

## DLT Expectations（データ品質ルール）設計

DLTのExpectationsは品質ルールを宣言的に定義し、違反時のアクションを指定できる。

| ルール名 | 対象列 | 条件 | アクションと理由 |
|---|---|---|---|
| `valid_adj_close` | adj_close | IS NOT NULL | warn：ffillで補完するため除外しない |
| `valid_date` | date | IS NOT NULL | fail：日付なしのレコードは処理不能 |
| `no_future_date` | date | <= current_date() | drop：未来日付はデータ品質問題 |
| `valid_ticker` | ticker | IN ('SPY','QQQ','VT','1540.T') | warn：想定外銘柄を記録して保持 |
| `valid_return_range` | daily_return | ABS < 0.20 OR IS NULL | warn：異常値はis_validフラグで管理 |

**アクションの種類：**
- `@dlt.expect`：違反を記録するが処理を継続（warn）
- `@dlt.expect_or_drop`：違反レコードを除外
- `@dlt.expect_or_fail`：違反があればパイプライン全体を停止

---

## パイプライン構成図

```
┌─────────────────────────────────────────────────────────────┐
│                    DLT Pipeline                             │
│                                                             │
│  [Auto Loader]          [Streaming]         [Batch]        │
│  Raw JSON/CSV  ──▶  bronze_stock_prices ──▶ silver_stock   │
│  (yfinance出力)         @dlt.table           _returns       │
│                         quality=bronze       @dlt.table     │
│                         Expectations:        quality=silver  │
│                         - valid_date         Expectations:  │
│                                              - valid_return  │
│                                                    │        │
│                                                    ▼        │
│                                             gold_stats      │
│                                             gold_corr_matrix│
│                                             gold_portfolio  │
│                                             @dlt.table      │
│                                             quality=gold    │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    Unity Catalog (workspace.portfolio)
                    Streamlit / BI Tools
```

---

## 現在の実装との対応関係

| 現在のファイル | DLT移行後 | 変更内容 |
|---|---|---|
| `01_bronze_ingest.ipynb` | `pipeline/bronze.py` | `@dlt.table` + Auto Loader |
| `02_silver_cleanse.ipynb` | `pipeline/silver.py` | `@dlt.table` + Expectations |
| `03_gold_aggregate.ipynb` | `pipeline/gold.py` | `@dlt.table` |
| `04_export_job.ipynb` | 不要 | Gold層をStreamlitが直接参照 |

---

## 移行時の注意点

**1. ストリーミング vs バッチの選択**
- Bronze層：ストリーミング（Auto Loaderで新規データを自動検知）
- Silver層：ストリーミング（Bronze層の更新を自動で処理）
- Gold層：バッチ（集計処理はバッチで十分）

**2. Auto Loaderの前提**
- yfinanceの取得結果をJSONまたはCSVとしてクラウドストレージに保存する前処理が必要
- 現在の実装ではyfinanceから直接Spark DataFrameに変換しているため、Auto Loader用の出力ステップを追加する

**3. Unity Catalogとの統合**
- DLTはUnity Catalogと完全統合されており、テーブルは自動的にカタログに登録される
- データリネージ（どのデータがどこから来たか）がDLT UIで可視化される

---

*本ドキュメントはCommunity Edition環境での制約を踏まえた概念設計であり、有償Databricks環境での実装を想定している。*
