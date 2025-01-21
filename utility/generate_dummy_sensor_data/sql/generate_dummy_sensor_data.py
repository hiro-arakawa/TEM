import random
import datetime
from collections import defaultdict
import os  # 追加: ディレクトリ操作のため

# 工場コードのマスタ
factories = {
    "H": "元町工場"
}

# factories = {
#     "A": "本社管理",
#     "F2": "飛島",
#     "G": "本社工場",
#     "H": "元町工場",
#     "J": "上郷工場",
#     "K": "高岡工場",
#     "L": "三好工場",
#     "M": "東富士",
#     "N": "田原工場",
#     "P": "衣浦工場",
#     "Q": "貞宝工場",
#     "R": "堤工場",
#     "S": "下山工場",
#     "T": "明知工場",
#     "Y": "本社PT3号館",
# }

# ダミーデータ生成関数
def generate_sensor_data(factory_code, start_date, end_date, table_name, tag_rule):
    """
    指定されたテーブルとタグ生成ルールに基づいてセンサーデータを生成します。

    Parameters:
        factory_code (str): 工場コード
        start_date (datetime.date): データ生成の開始日
        end_date (datetime.date): データ生成の終了日
        table_name (str): 出力するテーブル名
        tag_rule (callable): タグ生成ルールを定義する関数

    Returns:
        defaultdict: 日付とセンサーごとのデータを格納した辞書
    """
    data = defaultdict(dict)
    date_range = (end_date - start_date).days + 1

    for day in range(date_range):
        current_date = start_date + datetime.timedelta(days=day)
        weekday = current_date.weekday()

        for device in range(1, 4):  # 各工場に3つの機器
            for sensor in range(1, 1001):  # 各機器に1000のセンサー
                tag = tag_rule(factory_code, device, sensor)  # タグ生成ルールを適用
                local_tag = tag
                local_id = device
                unit = "kwh"
                data_division = 3

                # センサーデータ生成
                base_value = random.uniform(100, 1000)
                row = {
                    "factory": factory_code,
                    "tag": tag,
                    "date": current_date.strftime('%Y-%m-%d'),
                    "local_tag": local_tag,
                    "local_id": local_id,
                    "name": f"Sensor-{sensor}",
                    "unit": unit,
                    "data_division": data_division,
                }

                for hour in range(30):  # 0時～翌5時までの30時間分
                    is_non_operating = (
                        (weekday == 5 and hour >= 6) or
                        (weekday == 6) or
                        (weekday == 0 and hour < 5)
                    )
                    variation = random.uniform(-0.02, 0.02) * base_value
                    assurance = random.choices([1] + list(range(2, 10)), weights=[95] + [5] * 8, k=1)[0]
                    d1 = round(base_value + variation, 1)
                    d2 = round(d1 * random.uniform(0.9, 1.1), 1)
                    d3 = round(d2 * random.uniform(0.9, 1.1), 1)

                    if is_non_operating:
                        d1 = round(d1 * 0.1, 1)
                        d2 = round(d2 * 0.1, 1)
                        d3 = round(d3 * 0.1, 1)

                    row[f"d0_{hour}"] = assurance
                    row[f"d1_{hour}"] = d1
                    row[f"d2_{hour}"] = d2
                    row[f"d3_{hour}"] = d3

                data[current_date][tag] = row
    return data

def generate_insert_statements(data, factory_code, table_name):
    """
    センサーデータをINSERT文として日付単位でファイルに出力します。

    Parameters:
        data (dict): 日付とセンサーごとのデータを格納した辞書
        factory_code (str): 工場コード
        table_name (str): テーブル名 (DataLoadTemp または CalculationTemp)
    """
    # 出力フォルダ名をテーブル名ごとに設定
    output_dir = os.path.join("SQL", table_name)  # SQL/DataLoadTemp または SQL/CalculationTemp
    os.makedirs(output_dir, exist_ok=True)  # フォルダが存在しない場合は作成

    # 日付ごとにSQLファイルを出力
    for date, sensors in data.items():
        file_name = os.path.join(output_dir, f"{date}_{factory_code}.sql")  # ファイル名を生成
        with open(file_name, "w", encoding="utf-8") as f:
            for tag, row in sensors.items():
                columns = ", ".join(row.keys())  # カラム名を生成
                values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()])  # 値を生成
                # スキーマ batch を含むテーブル名に修正
                f.write(f"INSERT INTO [batch].[{table_name}] ({columns}) VALUES ({values});\n")
        print(f"{file_name} にデータを保存しました。")


# 両日付にまたがるデータを調整
def adjust_data_across_days(data, start_date, end_date):
    """
    翌日のデータを前日の24時～29時にコピーして反映します。

    Parameters:
        data (dict): 日付とセンサーごとのデータを格納した辞書
        start_date (datetime.date): データ生成の開始日
        end_date (datetime.date): データ生成の終了日
    """
    date_range = (end_date - start_date).days
    for day in range(date_range):
        current_date = start_date + datetime.timedelta(days=day)
        previous_date = current_date - datetime.timedelta(days=1)
        if previous_date in data:
            for tag, row in data[current_date].items():
                # 前日の24～29時に同じ値を設定
                for hour in range(0, 6):
                    previous_hour = hour + 24
                    data[previous_date][tag][f"d0_{previous_hour}"] = row[f"d0_{hour}"]
                    data[previous_date][tag][f"d1_{previous_hour}"] = row[f"d1_{hour}"]
                    data[previous_date][tag][f"d2_{previous_hour}"] = row[f"d2_{hour}"]
                    data[previous_date][tag][f"d3_{previous_hour}"] = row[f"d3_{hour}"]

def generate_tag_for_data_load_temp(factory_code, device, sensor):
    """DataLoadTemp用のタグ生成ルール"""
    return f"{factory_code}D{device}3{sensor:03}"


def generate_tag_for_calculation_temp(factory_code, device, sensor):
    """CalculationTemp用のタグ生成ルール"""
    return f"C{factory_code}3{sensor:03}"

def main():
    """
    全工場を対象にセンサーデータを生成し、日付単位のINSERT文を出力します。
    """
    # 日付範囲
    start_date = datetime.date(2024, 12, 1)
    end_date = datetime.date(2024, 12, 31)

    # DataLoadTemp の処理
    data_load_data = generate_sensor_data(
        factory_code="H",
        start_date=start_date,
        end_date=end_date,
        table_name="data_loader_data_load_temp",
        tag_rule=generate_tag_for_data_load_temp
    )
    generate_insert_statements(data_load_data, "H", "sensor_data")  # 出力先フォルダを指定

    # CalculationTemp の処理
    # calculation_data = generate_sensor_data(
    #     factory_code="H",
    #     start_date=start_date,
    #     end_date=end_date,
    #     table_name="data_processing_calculation_temp",
    #     tag_rule=generate_tag_for_calculation_temp
    # )
    # generate_insert_statements(calculation_data, "H", "data_processing_calculation_temp")  # 出力先フォルダを指定

if __name__ == "__main__":
    main()
