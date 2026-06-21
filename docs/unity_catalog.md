# Unity Catalog ガバナンス設計

本プロジェクトはDatabricks Community Editionで実装しているため、Unity Catalogの全機能は使用していない。  
本ドキュメントでは、本番環境へ移行する際のUnity Catalogを用いたデータガバナンス設計を示す。

---

## Unity Catalogとは

Unity CatalogはDatabricksが提供するデータガバナンスの統合管理基盤。  
テーブル・ファイル・MLモデルへのアクセス制御・監査・データリネージを一元管理できる。

**現在の実装との違い：**

| 項目 | 現在（Community Edition） | 本番（Unity Catalog） |
|---|---|---|
| 名前空間 | `workspace.portfolio.*` | `{env}.{domain}.*` |
| アクセス制御 | なし | ロールベース（RBAC） |
| データリネージ | なし | 自動追跡 |
| 監査ログ | なし | 全操作を記録 |
| 列レベルセキュリティ | なし | 列単位でマスキング可能 |
| タグ管理 | なし | テーブル・列単位でタグ付け |

---

## 名前空間設計（三層構造）

Unity Catalogは`Catalog.Schema.Table`の三層構造で管理する。

```
Unity Catalog
├── dev                          ← 開発環境カタログ
│   └── portfolio
│       ├── bronze_stock_prices
│       ├── silver_stock_returns
│       ├── gold_stats
│       ├── gold_corr_matrix
│       └── gold_portfolio_metrics
│
├── staging                      ← 検証環境カタログ
│   └── portfolio
│       └── （devと同一構成）
│
└── prod                         ← 本番環境カタログ
    └── portfolio
        └── （devと同一構成）
```

**環境ごとにカタログを分離する理由：**
- 開発中のデータ変更が本番に影響しない
- 環境ごとのアクセス制御を明確に分離できる
- データの昇格（dev → staging → prod）をカタログ間のコピーで管理できる

---

## アクセス制御設計（RBAC）

### ロール定義

| ロール名 | 対象者 | 付与する権限 |
|---|---|---|
| `data_engineer` | データエンジニア | Bronze/Silver/Goldの読み書き |
| `data_analyst` | アナリスト | Goldのみ読み取り |
| `pipeline_service` | DLTパイプライン実行ユーザー | Bronze書き込み・Silver/Gold読み書き |
| `streamlit_service` | Streamlitアプリ実行ユーザー | Goldのみ読み取り |

### 権限付与（SQL）

```sql
-- data_engineerロールへの権限付与
GRANT USE CATALOG ON CATALOG prod TO data_engineer;
GRANT USE SCHEMA ON SCHEMA prod.portfolio TO data_engineer;
GRANT SELECT, MODIFY ON TABLE prod.portfolio.bronze_stock_prices TO data_engineer;
GRANT SELECT, MODIFY ON TABLE prod.portfolio.silver_stock_returns TO data_engineer;
GRANT SELECT, MODIFY ON TABLE prod.portfolio.gold_stats TO data_engineer;

-- data_analystロールへの権限付与（Goldのみ読み取り）
GRANT USE CATALOG ON CATALOG prod TO data_analyst;
GRANT USE SCHEMA ON SCHEMA prod.portfolio TO data_analyst;
GRANT SELECT ON TABLE prod.portfolio.gold_stats TO data_analyst;
GRANT SELECT ON TABLE prod.portfolio.gold_corr_matrix TO data_analyst;
GRANT SELECT ON TABLE prod.portfolio.gold_portfolio_metrics TO data_analyst;

-- streamlit_serviceへの権限付与
GRANT SELECT ON TABLE prod.portfolio.gold_stats TO streamlit_service;
GRANT SELECT ON TABLE prod.portfolio.gold_corr_matrix TO streamlit_service;
GRANT SELECT ON TABLE prod.portfolio.gold_portfolio_metrics TO streamlit_service;
```

**設計方針：**
- 最小権限の原則：各ロールに必要最小限の権限のみ付与する
- Bronze層への直接アクセスはdata_engineerとpipeline_serviceのみに限定する
- StreamlitアプリはGold層のみ参照できるため、生データへのアクセスリスクがない

---

## テーブルプロパティ・タグ設計

### テーブルレベルのタグ

```sql
-- Bronze層
ALTER TABLE prod.portfolio.bronze_stock_prices
SET TAGS ('layer' = 'bronze', 'domain' = 'portfolio', 'source' = 'yfinance', 'pii' = 'false');

-- Silver層
ALTER TABLE prod.portfolio.silver_stock_returns
SET TAGS ('layer' = 'silver', 'domain' = 'portfolio', 'sla' = 'daily');

-- Gold層
ALTER TABLE prod.portfolio.gold_stats
SET TAGS ('layer' = 'gold', 'domain' = 'portfolio', 'consumer' = 'streamlit', 'sla' = 'daily');
```

### 列レベルのコメント

```sql
ALTER TABLE prod.portfolio.silver_stock_returns
ALTER COLUMN daily_return COMMENT '対数リターン: ln(P_t / P_{t-1})。時間加法性があるためモンテカルロ法に適している。';

ALTER TABLE prod.portfolio.silver_stock_returns
ALTER COLUMN is_valid COMMENT '異常値フラグ: |daily_return| >= 0.20 の場合False。Gold層でフィルタに使用する。';
```

---

## データリネージ

Unity Catalog + DLTの組み合わせでデータの流れを自動的に追跡する。
本プロジェクトではDLTを使用していないため自動追跡は機能しないが、データの流れは以下の通り。

```
Yahoo Finance API
      │
      ▼
bronze_stock_prices     ← yfinanceから取得した生データ
      │
      ▼
silver_stock_returns    ← Bronze層から派生。対数リターン・異常値フラグを付与
      │
      ├──▶ gold_stats              ← 銘柄別年率統計
      ├──▶ gold_corr_matrix        ← 銘柄間相関行列
      └──▶ gold_portfolio_metrics  ← ポートフォリオ加重後指標
                    │
                    ▼
             Streamlit Application
```

**リネージ追跡の価値：**
- 上流データの変更が下流に与える影響を即座に把握できる
- 問題発生時の原因特定（どのテーブルが影響を受けているか）が容易になる
- データの信頼性を証明する監査証跡として機能する

---

## 監査ログ設計

Unity Catalogは全てのデータアクセスを自動的にシステムテーブルに記録する。

```sql
-- 過去24時間のアクセスログを確認
SELECT
    user_name,
    action_name,
    request_params.full_name_arg AS table_name,
    event_time
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 1 DAY
  AND action_name IN ('SELECT', 'MODIFY', 'CREATE', 'DROP')
ORDER BY event_time DESC;
```

**監査ログの活用：**
- 誰がいつどのテーブルにアクセスしたかを追跡できる
- 異常なアクセスパターン（大量データの取得など）を検知できる
- コンプライアンス要件（金融規制など）への対応に利用できる

---

## 現在の実装との対応関係

| 現在の実装 | 本番（Unity Catalog） | 変更内容 |
|---|---|---|
| `workspace.portfolio.*` | `prod.portfolio.*` | カタログを環境ごとに分離 |
| アクセス制御なし | RBACで役割ごとに制御 | ロール定義・権限付与SQLを追加 |
| タグなし | テーブル・列単位でタグ付け | メタデータ管理の強化 |
| リネージなし | 自動追跡 | DLTとの統合で実現 |

---

## 移行手順（概要）

```
1. 本番カタログの作成
   CREATE CATALOG prod;
   CREATE SCHEMA prod.portfolio;

2. ロールの作成と権限付与
   CREATE ROLE data_engineer;
   GRANT ... TO data_engineer;

3. DLTパイプラインの接続先を本番カタログに変更
   pipeline_config: target = "prod.portfolio"

4. タグ・コメントの付与
   ALTER TABLE ... SET TAGS (...);

5. 監査ログの確認
   SELECT * FROM system.access.audit ...;
```

---

*本ドキュメントはCommunity Edition環境での制約を踏まえた概念設計であり、有償Databricks環境での実装を想定している。*
