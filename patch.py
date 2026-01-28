import requests
import json
import pandas as pd
import calendar
from google.cloud import bigquery
import os
import calendar
import jpholiday
from datetime import datetime, date


KOT_TOKEN = os.getenv("KOT_TOKEN", "").strip()
SMARTHR_TOKEN = os.getenv("SMARTHR_TOKEN", "").strip()
SMARTHR_SUBDOMAIN = os.getenv("SMARTHR_SUBDOMAIN", "")
GCP_KEY_PATH = os.getenv("GCP_KEY_PATH", "credentials.json")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET = "roumu_automation_1029"

# テーブルID
TABLE_SMARTHR = f"{GCP_PROJECT_ID}.{BQ_DATASET}.smarthr_employees"
TABLE_KOT_DAILY = f"{GCP_PROJECT_ID}.{BQ_DATASET}.kot_daily_attendance_detail"
TABLE_KOT_MONTHLY = f"{GCP_PROJECT_ID}.{BQ_DATASET}.kot_monthly_summary"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH

TARGET_MONTH = "2025-11"
def fetch_smarthr_data():
    all_employees = []
    url = f"https://{SMARTHR_SUBDOMAIN}.smarthr.jp/api/v1/crews?per_page=100"
    headers = {"Authorization": f"Bearer {SMARTHR_TOKEN}"}
    
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page_data = response.json()
        all_employees.extend(page_data)
        
        link_header = response.headers.get('Link')
        url = None
        if link_header:
            for link in link_header.split(','):
                if 'rel="next"' in link:
                    url = link.split(';')[0].strip().replace('<', '').replace('>', '')
                    break
    return all_employees

def process_smarthr_data(json_data):
    processed_rows = []
    for emp in json_data:
        if not emp: continue
        depts = emp.get("departments") or []

        dept_name = None
        if depts and depts[0] is not None:
            dept_name = depts[0].get("name")
        
        
        processed_rows.append({
            "emp_code": emp.get("emp_code"),
            "full_name": f"{emp.get('business_last_name') or ''} {emp.get('business_first_name') or ''}".strip(),
            "employment_type": (emp.get("employment_type") or {}).get("name"),
            "dept_name": dept_name
        })
    return processed_rows

# ==========================================
# 3. KING OF TIME データ取得 & 整形
# ==========================================

def fetch_kot_daily_detailed(target_month):
    """日次データを一括取得する (修正版)"""
    year, month = map(int, target_month.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    
    # URLはベースのみを記述（末尾に日付やスラッシュを入れない）
    url = "https://api.kingtime.jp/v1.0/daily-workings"
    headers = {"Authorization": f"Bearer {KOT_TOKEN}"}
    
    
    # パラメータは辞書形式で渡す
    params = {
        "start": f"{target_month}-01",
        "end": f"{target_month}-{last_day}",
        "additionalFields": "currentDateEmployee" # 日次専用フィールド
    }
    
    print(f"KOT日次APIリクエスト中: {url} ({params['start']}～{params['end']})")
    
    response = requests.get(url, headers=headers, params=params)
    
    # 403エラー時に詳細を表示するデバッグ処理
    if response.status_code != 200:
        print(f"--- KOT API Error ---")
        print(f"Status: {response.status_code}")
        print(f"Reason: {response.text}") # ここに具体的なエラー理由が出ます
        
    response.raise_for_status()
    return response.json()

def process_kot_daily_detailed(json_data, target_month):
    """
    日次データを整形：
    裁量労働・管理職の集計用に実労働時間と雇用形態コードを保持する。
    """
    processed_rows = []
    for day_data in json_data:
        date = day_data.get('date')
        for rec in day_data.get('dailyWorkings', []):
            # 従業員情報を取得
            emp = rec.get('currentDateEmployee') or rec.get('currentEmployee') or {}
            emp_code = emp.get('code')
            if not emp_code:
                continue

            # 実労働時間 (totalWork) と 雇用形態コード (typeCode)
            total_work = rec.get('totalWork', 0)
            type_code = str(emp.get('typeCode', ''))

            processed_rows.append({
                "emp_code": str(emp_code),
                "work_date": date,
                "type_code": type_code,
                "overtime_minutes": int(rec.get('overtime', 0)),
                "total_work_minutes": int(total_work)  # 裁量・管理用
            })
    return processed_rows

def calculate_true_standard_minutes(target_month_str, daily_working_hours=8):
    """
    対象月の本来あるべき基準時間（分）を計算する。
    (全日数 - 土日 - 祝日 - 年末年始休暇) * 480分
    """
    target_date = datetime.strptime(target_month_str, '%Y-%m')
    year, month = target_date.year, target_date.month
    
    # 会社独自の休日リスト (年末年始 12/29〜1/3 をデフォルト設定)
    company_holidays = [
        date(2025, 12, 29), date(2025, 12, 30), date(2025, 12, 31),
        date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
    ]
    
    num_days = calendar.monthrange(year, month)[1]
    work_days = 0
    
    for day in range(1, num_days + 1):
        curr_date = date(year, month, day)
        # 土日・祝日・会社休日を除外
        if curr_date.weekday() >= 5 or jpholiday.is_holiday(curr_date) or curr_date in company_holidays:
            continue
        work_days += 1
        
    return work_days * daily_working_hours * 60

def fetch_kot_monthly_summary(target_month, daily_raw_data):
    # 基準時間を計算 (144:00など)
    true_standard_min = calculate_true_standard_minutes(target_month)
    
    # 従業員マスタ作成
    emp_master = {} 
    for day_data in daily_raw_data:
        for rec in day_data.get('dailyWorkings', []):
            emp = rec.get('currentDateEmployee') or rec.get('currentEmployee') or {}
            key = rec.get('employeeKey')
            if key and emp.get('code'):
                emp_master[key] = {
                    "code": str(emp.get('code')),
                    "name": f"{emp.get('lastName', '')} {emp.get('firstName', '')}".strip(),
                    "type_name": str(emp.get('typeName', '')),
                    "type_code": str(emp.get('typeCode', ''))
                }

    url = f"https://api.kingtime.jp/v1.0/monthly-workings/{target_month}"
    headers = {"Authorization": f"Bearer {KOT_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    processed_rows = []
    for reg in data:
        emp_key = reg.get('employeeKey')
        m = emp_master.get(emp_key)
        if not m: continue

        # --- 全区分共通：実労働合計の算出 ---
        custom_items = reg.get('customMonthlyWorkings', [])
        # 001(所定/基準内) と 003(残業/超過分) だけを足す（002は重複のため無視）
        c001 = next((float(item['calculationResult']) for item in custom_items if item['code'] == '001'), 0)
        c003 = next((float(item['calculationResult']) for item in custom_items if item['code'] == '003'), 0)
        
        # 真の実労働合計 (徳山様なら 144:00 + 8:13 = 152:13)
        total_actual_work_min = int(c001 + c003)

        # 判定用
        t_code = m.get('type_code', '')
        t_name = m.get('type_name', '')
        is_flex = t_code in ['1000', '3100', '1060', '1070', '6001', '6002'] or "フレックス" in t_name
        is_discretionary = "裁量労働" in t_name
        is_manager = "管理監督者" in t_name

        # 休日労働
        l_h = reg.get('legalHolidayWork', {})
        g_h = reg.get('generalHolidayWork', {})
        holiday_work_min = (int(l_h.get('normal', 0)) + int(l_h.get('extra', 0)) + 
                            int(g_h.get('normal', 0)) + int(g_h.get('extra', 0)))

        # 36協定用 (AV, AW, AX)
        weekday_work_min = total_actual_work_min - holiday_work_min
        weekday_excess_min = max(0, weekday_work_min - true_standard_min)
        total_excess_min = weekday_excess_min + holiday_work_min

        # 所定・残業の振分け
        if is_flex:
            # フレックス: 合算した合計 - 基準時間 = 残業
            overtime_val = max(0, total_actual_work_min - true_standard_min)
            assigned_val = total_actual_work_min - overtime_val
        elif is_discretionary or is_manager:
            # 裁量・管理職: 深夜残業のみを残業代対象とする
            overtime_val = int(reg.get('nightOvertime', 0))
            assigned_val = total_actual_work_min - overtime_val
        else:
            # 固定時間制: 003(残業) の値をそのまま採用（あるいはKOT標準値）
            overtime_val = int(c003) if c003 > 0 else int(reg.get('overtime', 0))
            assigned_val = total_actual_work_min - overtime_val

        # 休暇取得
        holidays = reg.get('holidaysObtained', [])
        def get_h(name, field='dayCount'):
            return sum(h.get(field, 0) for h in holidays if h.get('name') == name)

        processed_rows.append({
            "emp_code": m['code'],
            "user_name": m['name'],
            "employment_type": t_name if t_name else f"コード:{t_code}",
            "target_month": target_month,
            "standard_labor_minutes": true_standard_min,
            "assigned_minutes": assigned_val,
            "unassigned_minutes": int(reg.get('unassigned', 0)),
            "overtime_minutes": overtime_val,
            "premium_overtime": 0,
            "night_assigned": int(reg.get('night', 0)),
            "night_unassigned": int(reg.get('nightUnassigned', 0)),
            "night_overtime": int(reg.get('nightOvertime', 0)),
            "premium_night_overtime": 0,
            "legal_h_assigned": l_h.get('normal', 0),
            "legal_h_unassigned": l_h.get('extra', 0),
            "legal_h_overtime": l_h.get('overtime', 0),
            "legal_h_night_assigned": l_h.get('night', 0),
            "legal_h_night_unassigned": l_h.get('nightExtra', 0),
            "legal_h_night_overtime": l_h.get('nightOvertime', 0),
            "gen_h_assigned": g_h.get('normal', 0),
            "gen_h_unassigned": g_h.get('extra', 0),
            "gen_h_overtime": g_h.get('overtime', 0),
            "gen_h_night_assigned": g_h.get('night', 0),
            "gen_h_night_unassigned": g_h.get('nightExtra', 0),
            "gen_h_night_overtime": g_h.get('nightOvertime', 0),
            "late_minutes": int(reg.get('late', 0)),
            "early_leave_minutes": int(reg.get('earlyLeave', 0)),
            "break_minutes": int(reg.get('breakSum', 0)),
            "interval_shortage_count": int(reg.get('intervalShortageCount', 0)),
            "total_working_minutes": total_actual_work_min,
            "late_count": int(reg.get('lateCount', 0)),
            "early_leave_count": int(reg.get('earlyLeaveCount', 0)),
            "workingday_count": float(reg.get('workingdayCount', 0)),
            "absentday_count": float(reg.get('absentdayCount', 0)),
            "daikyu_days": get_h('代休'),
            "yuq_days": get_h('有休'),
            "keicho_days": get_h('慶弔休暇'),
            "summer_days": get_h('夏季休暇'),
            "special_paid_days": get_h('特別休暇（有給）'),
            "seiri_paid_days": get_h('生理休暇（有給）'),
            "kango_days": get_h('子の看護休暇（未就学）'),
            "kaigo_days": get_h('介護休暇'),
            "shukko_special_days": get_h('出向者特別休暇（有給）'),
            "regarding_minutes": int(reg.get('regarding', 0)),
            "kango_minutes": int(get_h('子の看護休暇（未就学）', 'minutes')),
            "kaigo_minutes": int(get_h('介護休暇', 'minutes')),
            "shukko_special_minutes": int(get_h('出向者特別休暇（有給）', 'minutes')),
            "thirty_six_total_excess": total_excess_min,
            "thirty_six_weekday_excess": weekday_excess_min,
            "thirty_six_holiday_work": holiday_work_min,
        })
    return processed_rows
# ==========================================
# 4. BigQuery ロード処理
# ==========================================
def load_to_bq(rows, table_id, schema):  # 第2引数を 'table_id' にします
    if not rows:
        print(f"データが空のためロードをスキップします。")
        return
    
    # clientを初期化（project指定は任意ですが、認証が通っていればこれだけでOK）
    client = bigquery.Client()
    
    # すでに TABLE_SMARTHR 等の変数にプロジェクト名が含まれているので、
    # ここでは加工せず、そのまま table_id を使用します
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )
    
    # ここで引数の table_id を使います
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    print(f"ロード完了: {table_id} ({job.output_rows} 行)")

# ==========================================
# 5. メイン実行ブロック
# ==========================================
if __name__ == "__main__":
    # A. SmartHR 処理
    hr_raw = fetch_smarthr_data()
    hr_processed = process_smarthr_data(hr_raw)
    hr_schema = [
        bigquery.SchemaField("emp_code", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("employment_type", "STRING"),
        bigquery.SchemaField("dept_name", "STRING"),
    ]
    load_to_bq(hr_processed, TABLE_SMARTHR, hr_schema)

    # B. KOT 日次処理
    daily_raw = fetch_kot_daily_detailed(TARGET_MONTH)
    daily_processed = process_kot_daily_detailed(daily_raw, TARGET_MONTH)
    # B. KOT 日次処理用のスキーマ
    daily_schema = [
        bigquery.SchemaField("emp_code", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("work_date", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("type_code", "STRING", mode="NULLABLE"),     # typeCodeでの判定用
        bigquery.SchemaField("overtime_minutes", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_work_minutes", "INTEGER", mode="NULLABLE"), # 実労働
    ]
    load_to_bq(daily_processed, TABLE_KOT_DAILY, daily_schema)

    # C. KOT 月次処理
    monthly_processed = fetch_kot_monthly_summary(TARGET_MONTH, daily_raw)
   
    monthly_schema = [
    bigquery.SchemaField("emp_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("employment_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("target_month", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("standard_labor_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("assigned_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("unassigned_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("overtime_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("premium_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("night_assigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("night_unassigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("night_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("premium_night_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_assigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_unassigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_night_assigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_night_unassigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("legal_h_night_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_assigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_unassigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_night_assigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_night_unassigned", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gen_h_night_overtime", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("late_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("early_leave_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("break_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("interval_shortage_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_working_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("late_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("early_leave_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("workingday_count", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("absentday_count", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("daikyu_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("yuq_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("keicho_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("summer_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("special_paid_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("seiri_paid_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("kango_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("kaigo_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("shukko_special_days", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("regarding_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("kango_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("kaigo_minutes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("shukko_special_minutes", "INTEGER", mode="NULLABLE"),
    # 【36協定・裁量管理用項目】
    bigquery.SchemaField("thirty_six_total_excess", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("thirty_six_weekday_excess", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("thirty_six_holiday_work", "INTEGER", mode="NULLABLE"),
]
    load_to_bq(monthly_processed, TABLE_KOT_MONTHLY, monthly_schema)

    print(f"--- 全工程が完了しました ({TARGET_MONTH}分) ---")