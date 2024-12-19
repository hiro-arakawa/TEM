import os
import pyodbc
import datetime

# SQL Serverの接続情報
SERVER = '192.168.80.133'  # サーバー名
DATABASE = 'TEM'  # データベース名
USERNAME = 'tem_prog'  # ユーザー名
PASSWORD = 'Ladm01#'  # パスワード

def execute_sql_files(sql_folder):
    """SQLフォルダ配下のすべてのSQLファイルを実行"""
    # SQL Serverに接続
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
    )
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            print(f"SQL実行開始時刻: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # SQLフォルダ内のすべてのファイルを処理
            for file_name in sorted(os.listdir(sql_folder)):
                if file_name.endswith('.sql'):  # .sqlファイルのみ対象
                    file_path = os.path.join(sql_folder, file_name)
                    print(f"実行中: {file_path}")
                    start_time = datetime.datetime.now()  # 実行開始時刻を記録
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                        try:
                            cursor.execute(sql_script)  # SQL実行
                            conn.commit()  # コミット
                            end_time = datetime.datetime.now()  # 実行終了時刻
                            elapsed_time = (end_time - start_time).total_seconds()
                            print(f"成功: {file_name} ({elapsed_time:.2f} 秒)")
                        except pyodbc.Error as e:
                            end_time = datetime.datetime.now()  # 実行終了時刻
                            elapsed_time = (end_time - start_time).total_seconds()
                            print(f"エラー: {file_name} - {e} ({elapsed_time:.2f} 秒)")
    except pyodbc.Error as e:
        print(f"SQL Serverへの接続に失敗しました: {e}")

def main():
    # SQLフォルダのパス
    sql_folder = "SQL"  # SQLフォルダの相対パスまたは絶対パス
    if not os.path.exists(sql_folder):
        print(f"指定されたフォルダが見つかりません: {sql_folder}")
        return

    # SQLファイルの実行
    execute_sql_files(sql_folder)

if __name__ == "__main__":
    main()