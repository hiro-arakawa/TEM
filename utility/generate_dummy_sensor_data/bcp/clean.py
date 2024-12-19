import pandas as pd

# CSVファイルを読み込む
input_file = "utility/generate_dummy_sensor_data/bcp/bcp_all_factories_data.csv"
output_file = "utility/generate_dummy_sensor_data/bcp/cleaned_bcp_data.csv"

# データを読み込み、問題のある行をチェック
data = pd.read_csv(input_file)

# 空の値や不要な空白を削除
data = data.dropna().applymap(lambda x: str(x).strip() if isinstance(x, str) else x)

# クリーンなデータを保存
data.to_csv(output_file, index=False)
