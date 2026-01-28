# 勤怠データ自動集計システム (KOT & SmartHR to BigQuery)

KING OF TIME (KOT) の勤怠データと SmartHR の従業員データをAPI経由で取得し、整形・計算を行った上で Google BigQuery へ自動連携するETLスクリプトです。

KOT標準のAPIレスポンスだけでは算出が難しい「フレックス制の正確な残業時間」や「裁量労働制・管理監督者向けの36協定管理用データ」を、独自のロジックで算出して格納します。

## 🚀 機能概要

* **データソース**:
    * **SmartHR**: 従業員基本情報（氏名、部署名、雇用形態コードなど）
    * **KING OF TIME**: 日次勤怠データ、月次勤怠データ（カスタム項目を含む）
* **データ処理**:
    * カスタムデータ項目を用いた「実労働時間」の再計算
    * 雇用区分（フレックス、裁量労働、管理監督者、固定時間制）ごとの残業計算ロジック分岐
    * 36協定管理用の法定超過時間の算出
* **データ格納**:
    * Google BigQuery の指定データセット内のテーブルへ `WRITE_TRUNCATE`（洗い替え）でロード

## 🛠 計算ロジックの仕様（重要）

本システムでは、KOTの設定仕様に合わせて以下のロジックで数値を算出しています。

### 1. 実労働合計時間 (Total Working Minutes)
KOTのカスタムデータ項目を使用し、以下の計算式で「真の総労働時間」を算出しています。

> **計算式**: `カスタム項目001 (所定)` + `カスタム項目003 (平日残業＋所定休日)`

※ `001` は基準時間でキャップされている場合があり、`002` は `003` と重複するため、`001 + 003` を採用しています。

### 2. 残業時間 (Overtime Minutes) の算出
雇用区分（`type_code` または `typeName`）に基づいて計算ロジックを分岐させています。

| 雇用区分 | 残業時間の定義 (`overtime_minutes`) | 所定時間の定義 (`assigned_minutes`) |
| :--- | :--- | :--- |
| **フレックス** | `実労働合計` - `基準時間(当月の所定労働日数×8h)` | `実労働合計` - `残業時間` |
| **裁量労働・管理監督者** | `深夜残業時間 (NightOvertime)` のみ | `実労働合計` - `深夜残業時間` |
| **固定時間制** | カスタム項目 `003` の値 (またはKOT標準残業時間) | `実労働合計` - `残業時間` |

### 3. 36協定集計用項目
管理監督者・裁量労働制の36協定チェック用に、以下の3項目を算出します。

* **① 法定超過時間 (`thirty_six_total_excess`)**:
    * 計算式: `②平日法定超過` + `③休日実労働`
* **② 平日法定超過時間 (`thirty_six_weekday_excess`)**:
    * 計算式: `max(0, (実労働合計 - 休日実労働) - 基準時間)`
* **③ 法定・法定外休日実労働時間 (`thirty_six_holiday_work`)**:
    * 計算式: `法定休日(所定+所定外)` + `法定外休日(所定+所定外)`

## 📦 必要要件 (Requirements)

* Python 3.x
* 以下のPythonライブラリ:
    * `requests`
    * `pandas`
    * `google-cloud-bigquery`
    * `jpholiday`

インストールコマンド:
```bash
pip install requests pandas google-cloud-bigquery jpholiday


環境変数名,説明
KOT_TOKEN,KING OF TIME API アクセストークン
SMARTHR_TOKEN,SmartHR API アクセストークン
SMARTHR_SUBDOMAIN,SmartHRのサブドメインID
GCP_PROJECT_ID,Google Cloud プロジェクトID
BQ_DATASET,BigQuery データセット名 (例: roumu_automation)
GCP_KEY_PATH,サービスアカウントキー（JSON）へのパス (例: credentials.json)

python patch.py

📊 BigQuery テーブル定義
1. kot_monthly_summary (月次集計テーブル)
主要なカラム構成は以下の通りです。

emp_code: 従業員コード

total_working_minutes: 実労働合計（分）

assigned_minutes: 所定労働時間（分）

overtime_minutes: 残業時間（分）

thirty_six_total_excess: 【36協定】法定超過合計

thirty_six_weekday_excess: 【36協定】平日超過

thirty_six_holiday_work: 【36協定】休日実労働

その他、各種休暇取得日数（有休、慶弔、看護、介護など）


Last Updated: 2026-1-28