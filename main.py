import os
from google.cloud import bigquery
import requests
import calendar
from datetime import datetime

# --- 設定項目 ---
GCP_KEY_PATH = "backoffice-259107-a225b5187ea3.json" # ステップ1でDLしたJSONキーのパス
SMARTHR_TOKEN = "shr_b253_gtEZtazBdUnhEW8zmxyDD9dA2z6isRm3"
SMARTHR_SUBDOMAIN = "0571dd0f70ced9a04616605f" # 貴社のサブドメイン
GCP_PROJECT_ID = "backoffice-259107"
BQ_DATASET = "roumu_automation_1029"
BQ_TABLE = "smarthr_employees"

KOT_TOKEN = "9ed34cd9f47b45ceb27879edd8028b72" # KOTのBearerトークン
KOT_BQ_TABLE = "kot_attendance"  # ステップ2で作成したテーブル名

# GCP認証キーのパスを環境変数に設定
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH

def fetch_smarthr_data():
    """1. SmartHR APIから従業員データを"全ページ"取得"""
    print("SmartHR APIからデータを取得中...")
    
    all_employees = [] # 全従業員を格納するリスト
    
    # 1ページあたりの件数を最大値(100件)に指定して、初回リクエストURLを構築
    # 1ページあたりの件数を最大値(100件)に指定して、初回リクエストURLを構築
    url = f"https://{SMARTHR_SUBDOMAIN}.smarthr.jp/api/v1/crews?per_page=100"
    headers = {"Authorization": f"Bearer {SMARTHR_TOKEN}"}
    
    page_count = 1
    
    while url:
        print(f"  ...{page_count} ページ目のデータを取得中")
        
        response = requests.get(url, headers=headers)
        response.raise_for_status() # エラーがあればここで停止
        
        # 取得したページの従業員データを全リストに追加
        page_data = response.json()
        all_employees.extend(page_data)
        
        # --- ページネーション処理 ---
        # 'Link' ヘッダーから 'next' (次のページ) のURLを探す
        link_header = response.headers.get('Link')
        url = None # 次のページのURLを一旦リセット
        
        if link_header:
            links = link_header.split(',')
            for link in links:
                # 'rel="next"' (次のページ) を含むリンク部分を探す
                if 'rel="next"' in link:
                    # <URL> の形式からURL部分だけを抽出
                    parts = link.split(';')
                    url = parts[0].strip().replace('<', '').replace('>', '')
                    break # 'next'リンクが見つかったらループを抜ける
        
        page_count += 1

    print(f"全 {len(all_employees)} 件のデータを取得完了。")
    return all_employees


def process_smarthr_data(json_data):
    """2. JSONデータをBigQueryロード用の形式に整形 (安全装置付き)"""
    print("SmartHRデータを整形中...")
    processed_rows = []
    
    for emp in json_data:
        if not emp: continue

        # --- 安全な部署・グループ抽出 ---
        dept_name = None
        group_name = None
        
        # departmentsキーがない、または中身がNoneの場合に備えて "or []" で空リストにする
        depts = emp.get("departments") or []
        
        # 部署リストがあり、かつ1つ目の要素がNoneでないことを確認
        if len(depts) > 0 and depts[0]:
            primary_dept = depts[0]
            group_name = primary_dept.get("name")
            
            # 親部署を遡る (parentがNoneでないか確認しながら進む)
            parent = primary_dept.get("parent")
            if parent:
                grand_parent = parent.get("parent")
                if grand_parent:
                    dept_name = grand_parent.get("name")
                else:
                    dept_name = parent.get("name")
            else:
                # 親がいなければ、グループ名＝部署名とする（または空のまま）
                dept_name = group_name

        # --- 安全な雇用区分抽出 ---
        employment_type_dict = emp.get("employment_type")
        employment_type_name = None
        if employment_type_dict:
            employment_type_name = employment_type_dict.get("name")

        processed_rows.append({
            "emp_code": emp.get("emp_code"),
            # 名前がない場合に備えて空文字 "" をデフォルトにする
            "full_name": f"{emp.get('business_last_name') or ''} {emp.get('business_first_name') or ''}".strip(),
            "joined_date": emp.get("entered_at"),
            "resigned_date": emp.get("resigned_at"),
            "email": emp.get("email"),
            "employment_type": employment_type_name,
            "dept_name": dept_name,
            "group_name": group_name
        })
    
    print(f"全 {len(processed_rows)} 件のデータを整形完了。")
    return processed_rows

def load_to_bigquery(rows_to_insert):
    """3. 整形済みデータをBigQueryにロード (洗い替え)"""
    print("--- 修正版コード（location='US'指定）を実行中 ---")
    print(f"BigQueryテーブル {BQ_DATASET}.{BQ_TABLE} にロード中...")
    client = bigquery.Client(project=GCP_PROJECT_ID, location="US")
    
    
    # ロード設定 (テーブルを毎回クリアして上書き)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("emp_code", "STRING"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("joined_date", "DATE"),
            bigquery.SchemaField("resigned_date", "DATE"), # ★追加
            bigquery.SchemaField("email", "STRING"),       # ★追加
            bigquery.SchemaField("employment_type", "STRING"),
            bigquery.SchemaField("dept_name", "STRING"),   # ★名称変更
            bigquery.SchemaField("group_name", "STRING"),  # ★追加
        ]
    )
    
    job = client.load_table_from_json(
        rows_to_insert,
        f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        job_config=job_config
    )
    job.result() # ジョブの完了を待つ
    print(f"ロード完了: {job.output_rows} 行が書き込まれました。")


# --- メインの実行処理 ---
def main_smarthr():
    try:
        data = fetch_smarthr_data()
        processed_data = process_smarthr_data(data)
        load_to_bigquery(processed_data)
        print("SmartHRデータの処理が正常に完了しました。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


def fetch_kot_data(target_month: str):
    """1. KOT APIから月別勤怠データを取得 (VPN必須)"""
    print(f"KOT APIから {target_month} のデータを取得中...")
    
    # 例: "月別勤怠データ" エンドポイント
    # https://.../monthly-workings/{date}?additionalFields=...
    # {date} の部分に "2025-10" などを入れます
    url = f"https://api.kingtime.jp/v1.0/monthly-workings/{target_month}"
    
    headers = {"Authorization": f"Bearer {KOT_TOKEN}"}
    params = {
        "additionalFields": "currentDateEmployee"
    }
    response = requests.get(url, headers=headers, params=params)
    
    # 403 (IP制限) や 401 (トークン) エラーなどをチェック
    response.raise_for_status() 
    return response.json()


def process_kot_data(json_data, target_month: str):
    """2. KOTデータを整形 (36協定用・詳細データ対応版)"""
    print("KOTデータを整形中...")
    processed_rows = []
    
    for emp_record in json_data:
        current_employee = emp_record.get('currentEmployee', {})
        emp_code = current_employee.get('code')
        
        if not emp_code: continue

        # --- ネストされた(深い場所にある)データを取得 ---
        # ※すべて「分単位」です。SQL側で ÷60 します。
        
        # 1. 基本の残業・深夜
        overtime = emp_record.get('overtime', 0)               # 残業時間
        night_overtime = emp_record.get('nightOvertime', 0)    # 深夜残業時間
        
        # 2. 所定外 (unassigned)
        unassigned = emp_record.get('unassigned', 0)           # 所定外労働
        night_unassigned = emp_record.get('nightUnassigned', 0) # 深夜所定外

        # 3. 休日労働 (オブジェクトの中から詳細を取り出す)
        # 法定休日
        legal_hol = emp_record.get('legalHolidayWork', {})
        legal_holiday_time = (
            legal_hol.get('normal', 0) + 
            legal_hol.get('night', 0) + 
            legal_hol.get('overtime', 0) + 
            legal_hol.get('nightOvertime', 0)
        )
        
        # 法定外休日
        general_hol = emp_record.get('generalHolidayWork', {})
        general_holiday_time = (
            general_hol.get('normal', 0) + 
            general_hol.get('night', 0) + 
            general_hol.get('overtime', 0) + 
            general_hol.get('nightOvertime', 0)
        )

        # 4. 遅刻・早退
        late = emp_record.get('late', 0)
        early_leave = emp_record.get('earlyLeave', 0)

        # 5. 有給日数 (既存ロジック)
        paid_leave_days = 0.0
        holidays_list = emp_record.get('holidaysObtained', [])
        for h in holidays_list:
            if h.get('name') == '有休': 
                paid_leave_days += float(h.get('dayCount', 0.0))

        processed_rows.append({
            "emp_code": emp_code,
            "target_month": target_month,
            "start_date": emp_record.get('startDate'),
            "end_date": emp_record.get('endDate'),
            "paid_leave_days": paid_leave_days,
            # ▼追加項目
            "overtime": overtime,
            "night_overtime": night_overtime,
            "unassigned": unassigned,
            "night_unassigned": night_unassigned,
            "legal_holiday_time": legal_holiday_time,
            "general_holiday_time": general_holiday_time,
            "late": late,
            "early_leave": early_leave
        })
            
    return processed_rows

def load_kot_to_bigquery(rows_to_insert):
    """3. 整形済みKOTデータをBigQueryにロード (スキーマ拡張版)"""
    if not rows_to_insert:
        print("KOTデータが空のため、BigQueryロードをスキップしました。")
        return

    print(f"BigQueryテーブル {BQ_DATASET}.{KOT_BQ_TABLE} にロード中...")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # 項目が増えたのでスキーマも拡張
    schema = [
        bigquery.SchemaField("emp_code", "STRING"),
        bigquery.SchemaField("target_month", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("paid_leave_days", "FLOAT"),
        # ▼追加項目 (分単位なのでINTEGER)
        bigquery.SchemaField("overtime", "INTEGER"),
        bigquery.SchemaField("night_overtime", "INTEGER"),
        bigquery.SchemaField("unassigned", "INTEGER"),
        bigquery.SchemaField("night_unassigned", "INTEGER"),
        bigquery.SchemaField("legal_holiday_time", "INTEGER"),
        bigquery.SchemaField("general_holiday_time", "INTEGER"),
        bigquery.SchemaField("late", "INTEGER"),
        bigquery.SchemaField("early_leave", "INTEGER"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = client.load_table_from_json(
        rows_to_insert,
        f"{GCP_PROJECT_ID}.{BQ_DATASET}.{KOT_BQ_TABLE}",
        job_config=job_config
    )
    job.result()
    print(f"KOTデータ ロード完了: {job.output_rows} 行が書き込まれました。")


def load_kot_to_bigquery(rows_to_insert):
    """3. 整形済みKOTデータをBigQueryにロード (スキーマ拡張版)"""
    if not rows_to_insert:
        print("KOTデータが空のため、BigQueryロードをスキップしました。")
        return

    print(f"BigQueryテーブル {BQ_DATASET}.{KOT_BQ_TABLE} にロード中...")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # ▼ここに新しい項目(overtimeなど)が全て定義されている必要があります
    schema = [
        bigquery.SchemaField("emp_code", "STRING"),
        bigquery.SchemaField("target_month", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("paid_leave_days", "FLOAT"),
        # ▼追加項目 (分単位なのでINTEGER)
        bigquery.SchemaField("overtime", "INTEGER"),
        bigquery.SchemaField("night_overtime", "INTEGER"),
        bigquery.SchemaField("unassigned", "INTEGER"),
        bigquery.SchemaField("night_unassigned", "INTEGER"),
        bigquery.SchemaField("legal_holiday_time", "INTEGER"),
        bigquery.SchemaField("general_holiday_time", "INTEGER"),
        bigquery.SchemaField("late", "INTEGER"),
        bigquery.SchemaField("early_leave", "INTEGER"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = client.load_table_from_json(
        rows_to_insert,
        f"{GCP_PROJECT_ID}.{BQ_DATASET}.{KOT_BQ_TABLE}",
        job_config=job_config
    )
    job.result()
    print(f"KOTデータ ロード完了: {job.output_rows} 行が書き込まれました。")


def main_kot(target_month: str):
    """KOT連携のメイン処理"""
    try:
        data = fetch_kot_data(target_month)
        processed_data = process_kot_data(data, target_month)
        load_kot_to_bigquery(processed_data)
        print(f"KOTデータ ({target_month}) の処理が正常に完了しました。")
    except Exception as e:
        print(f"KOTデータの処理中にエラーが発生しました: {e}")
# ==========================================
# ▼▼▼ 修正版: 日別データ取得・処理用コード ▼▼▼
# ==========================================

# 日別データ専用のテーブル名設定
KOT_BQ_TABLE_DAILY = "kot_daily_attendance"

def fetch_kot_daily(target_month: str):
    """1. KOT APIから『日別』勤怠データを取得 (範囲指定版)"""
    print(f"--- 日別データ取得モード: {target_month} ---")
    print(f"KOT APIから {target_month} の『日別』データを取得中...")
    
    # 1. 月末日を計算する (例: 2025-10 -> 2025-10-01 ～ 2025-10-31)
    year, month = map(int, target_month.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    
    start_date = f"{target_month}-01"
    end_date = f"{target_month}-{last_day}"

    # 2. URLは「月」を含まないベースのURLにする
    # × https://.../daily-workings/2025-10
    # ○ https://.../daily-workings
    url = "https://api.kingtime.jp/v1.0/daily-workings"
    
    headers = {"Authorization": f"Bearer {KOT_TOKEN}"}
    
    # 3. パラメータで期間を指定する
    params = {
        "start": start_date,
        "end": end_date,
        "additionalFields": "currentDateEmployee"
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    # エラー時の詳細を表示する (デバッグ用)
    if response.status_code != 200:
        print(f"API Error: {response.status_code} - {response.text}")
    
    response.raise_for_status() 
    return response.json()
def process_kot_daily(json_data, target_month: str):
    """2. KOT日別データをBigQuery用に整形 (キー名完全修正版)"""
    print("KOT日別データを整形中...")
    processed_rows = []
    
    for day_data in json_data:
        date = day_data.get('date')
        daily_workings = day_data.get('dailyWorkings', [])
        
        for emp_record in daily_workings:
            # 従業員コード取得
            current_employee = emp_record.get('currentDateEmployee') or emp_record.get('currentEmployee') or {}
            emp_code = current_employee.get('code')
            if not emp_code: continue

            # --- 時間取得 (キー名を修正) ---
            overtime = emp_record.get('overtime', 0)
            
            # ★ここが修正ポイント: nightOvertime -> lateNightOvertime
            night_overtime = emp_record.get('lateNightOvertime', 0)
            
            unassigned = emp_record.get('unassigned', 0)
            
            # ★ここが修正ポイント: nightUnassigned -> lateNightUnassigned
            night_unassigned = emp_record.get('lateNightUnassigned', 0)
            
            late = emp_record.get('late', 0)
            early_leave = emp_record.get('earlyLeave', 0)
            
            # --- 休日判定 ---
            # holidayType(数字)がないため、workdayTypeName(文字)で判定します
            workday_type_name = emp_record.get('workdayTypeName', '')
            
            is_legal_holiday = 1 if '法定休日' in workday_type_name else 0
            is_general_holiday = 1 if '法定外' in workday_type_name else 0
            
            # 法定外休日かつ「法定休日」の文字を含まない場合 (念のため)
            if is_legal_holiday: is_general_holiday = 0

            # 深夜勤務フラグ
            is_night_work = 1 if (night_overtime > 0 or night_unassigned > 0) else 0

            processed_rows.append({
                "emp_code": emp_code,
                "date": date,
                "target_month": target_month,
                
                # BigQueryスキーマの holiday_type は INTEGER なので、簡易的に変換して入れます
                "holiday_type": 2 if is_legal_holiday else (1 if is_general_holiday else 0),
                
                "overtime": overtime,
                "night_overtime": night_overtime,
                "unassigned": unassigned,
                "night_unassigned": night_unassigned,
                "late": late,
                "early_leave": early_leave,
                "is_night_work": is_night_work,
                "is_legal_holiday": is_legal_holiday,
                "is_general_holiday": is_general_holiday
            })
            
    print(f"整形後のデータ件数: {len(processed_rows)}件")
    return processed_rows


def load_kot_daily_to_bigquery(rows_to_insert):
    """3. 日別データをロード (日別用のテーブルを作成)"""
    if not rows_to_insert:
        print("データが空のためロードをスキップします")
        return

    print(f"BigQueryテーブル {BQ_DATASET}.{KOT_BQ_TABLE_DAILY} にロード中...")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # 日別用のスキーマ定義
    schema = [
        bigquery.SchemaField("emp_code", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("target_month", "STRING"),
        bigquery.SchemaField("holiday_type", "INTEGER"),
        bigquery.SchemaField("overtime", "INTEGER"),
        bigquery.SchemaField("night_overtime", "INTEGER"),
        bigquery.SchemaField("unassigned", "INTEGER"),
        bigquery.SchemaField("night_unassigned", "INTEGER"),
        bigquery.SchemaField("late", "INTEGER"),
        bigquery.SchemaField("early_leave", "INTEGER"),
        bigquery.SchemaField("is_night_work", "INTEGER"),
        bigquery.SchemaField("is_legal_holiday", "INTEGER"),
        bigquery.SchemaField("is_general_holiday", "INTEGER"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = client.load_table_from_json(
        rows_to_insert,
        f"{GCP_PROJECT_ID}.{BQ_DATASET}.{KOT_BQ_TABLE_DAILY}",
        job_config=job_config
    )
    job.result()
    print(f"ロード完了: {job.output_rows} 行 (日別データ)")

def main_kot_daily(target_month: str):
    """日別データ取得の実行メイン関数"""
    try:
        data = fetch_kot_daily(target_month)
        processed_data = process_kot_daily(data, target_month)
        load_kot_daily_to_bigquery(processed_data)
        print(f"【成功】KOT日別データ ({target_month}) の処理が完了しました。")
    except Exception as e:
        print(f"【エラー】KOT日別データの処理中にエラーが発生しました: {e}")


import calendar # 日付計算に必要です

# ==========================================
# ▼▼▼ 新規追加: 欠勤・代休対応・日別ETL処理 ▼▼▼
# ==========================================

# BigQueryのテーブル名 (必要に応じて変更してください)
KOT_BQ_TABLE_DAILY_DETAILED = "kot_daily_attendance_detail"

def fetch_kot_daily_range(target_month: str):
    """1. [詳細版] KOT APIから指定月の日別データを取得"""
    print(f"--- [詳細版] 日別データ取得開始: {target_month} ---")
    
    # 月初と月末を計算
    year, month = map(int, target_month.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{target_month}-01"
    end_date = f"{target_month}-{last_day}"

    # 日別データ取得URL
    url = "https://api.kingtime.jp/v1.0/daily-workings"
    headers = {"Authorization": f"Bearer {KOT_TOKEN}"}
    params = {
        "start": start_date,
        "end": end_date,
        "additionalFields": "currentDateEmployee"
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def process_kot_daily_detailed(json_data, target_month: str):
    """2. [詳細版] 欠勤・代休・有休の日数を正確に集計して整形"""
    print("データを整形中 (欠勤・代休対応)...")
    processed_rows = []
    
    for day_data in json_data:
        date = day_data.get('date')
        daily_workings = day_data.get('dailyWorkings', [])
        
        for emp_record in daily_workings:
            # 従業員コード取得
            current_employee = emp_record.get('currentDateEmployee') or emp_record.get('currentEmployee') or {}
            emp_code = current_employee.get('code')
            if not emp_code: continue

            # --- 時間データの取得 ---
            overtime = emp_record.get('overtime', 0)
            night_overtime = emp_record.get('lateNightOvertime', 0)
            unassigned = emp_record.get('unassigned', 0)
            night_unassigned = emp_record.get('lateNightUnassigned', 0)
            late = emp_record.get('late', 0)
            early_leave = emp_record.get('earlyLeave', 0)
            
            # --- 休暇・欠勤・代休カウントロジック ---
            paid_leave_days = 0.0
            absence_days = 0.0
            sub_leave_days = 0.0
            
            holidays_obtained = emp_record.get('holidaysObtained', {})

            # (A) 全日休 (fulltimeHoliday)
            full_hol = holidays_obtained.get('fulltimeHoliday')
            if full_hol:
                h_name = full_hol.get('name')
                if h_name == '有休': paid_leave_days += 1.0
                elif h_name == '欠勤': absence_days += 1.0
                elif h_name == '代休': sub_leave_days += 1.0

            # (B) 半日休 (halfdayHolidays)
            half_hols = holidays_obtained.get('halfdayHolidays', [])
            for h in half_hols:
                h_name = h.get('name')
                if h_name == '有休': paid_leave_days += 0.5
                elif h_name == '欠勤': absence_days += 0.5
                elif h_name == '代休': sub_leave_days += 0.5

            # --- 休日タイプ・深夜フラグ ---
            workday_type_name = emp_record.get('workdayTypeName', '')
            is_legal_holiday = 1 if '法定休日' in workday_type_name else 0
            is_general_holiday = 1 if '法定外' in workday_type_name else 0
            if is_legal_holiday: is_general_holiday = 0 # 重複回避

            is_night_work = 1 if (night_overtime > 0 or night_unassigned > 0 or emp_record.get('lateNight', 0) > 0) else 0

            processed_rows.append({
                "emp_code": emp_code,
                "date": date,
                "target_month": target_month,
                "overtime": overtime,
                "night_overtime": night_overtime,
                "unassigned": unassigned,
                "night_unassigned": night_unassigned,
                "late": late,
                "early_leave": early_leave,
                "paid_leave_days": paid_leave_days,
                "absence_days": absence_days,       # ★追加
                "sub_leave_days": sub_leave_days,   # ★追加
                "holiday_type": 2 if is_legal_holiday else (1 if is_general_holiday else 0),
                "is_night_work": is_night_work,
                "is_legal_holiday": is_legal_holiday,
                "is_general_holiday": is_general_holiday
            })
            
    return processed_rows

def load_kot_daily_detailed_to_bq(rows_to_insert):
    """3. [詳細版] BigQueryへロード (スキーマ拡張版)"""
    if not rows_to_insert:
        print("データが空のためロードをスキップします")
        return

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{KOT_BQ_TABLE_DAILY_DETAILED}"
    print(f"BigQueryテーブル {table_id} にロード中...")
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    schema = [
        bigquery.SchemaField("emp_code", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("target_month", "STRING"),
        bigquery.SchemaField("overtime", "INTEGER"),
        bigquery.SchemaField("night_overtime", "INTEGER"),
        bigquery.SchemaField("unassigned", "INTEGER"),
        bigquery.SchemaField("night_unassigned", "INTEGER"),
        bigquery.SchemaField("late", "INTEGER"),
        bigquery.SchemaField("early_leave", "INTEGER"),
        # ▼ここが重要な追加項目
        bigquery.SchemaField("paid_leave_days", "FLOAT"),
        bigquery.SchemaField("absence_days", "FLOAT"),
        bigquery.SchemaField("sub_leave_days", "FLOAT"),
        # ▲ここまで
        bigquery.SchemaField("holiday_type", "INTEGER"),
        bigquery.SchemaField("is_night_work", "INTEGER"),
        bigquery.SchemaField("is_legal_holiday", "INTEGER"),
        bigquery.SchemaField("is_general_holiday", "INTEGER"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
    job.result()
    print(f"【完了】詳細データロード完了: {job.output_rows} 行")

def run_kot_daily_detailed_etl(target_month: str):
    """★これを実行: 詳細日別データのETLメイン関数"""
    try:
        data = fetch_kot_daily_range(target_month)
        processed = process_kot_daily_detailed(data, target_month)
        load_kot_daily_detailed_to_bq(processed)
        print(f"処理成功: {target_month} の詳細データ更新が完了しました。")
    except Exception as e:
        print(f"エラー発生: {e}")


# このファイルを実行したときに main_smarthr 関数を呼び出す

if __name__ == "__main__":
    #main_smarthr()
    
   
    # 2. KOTの勤怠データを更新 (例: 2025年10月分)
    #target_month_to_fetch = "2025-10" # 取得したい対象月を指定
    #main_kot(target_month_to_fetch)

    #print(">>> 日別データ更新プロセスを開始します <<<")
    #target_month_daily = "2025-10"  # 取得したい月
    ##main_kot_daily(target_month_daily)
    print(">>> KOT日別詳細ETL (欠勤・代休対応版) を開始します <<<")
    
    # 対象月を指定して実行
    target_month_input = "2025-10" 
    run_kot_daily_detailed_etl(target_month_input)
    
    print("全プロセスの実行が終了しました。")