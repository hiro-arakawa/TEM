def generate_bcp_format_file(output_file="data_loader_format.fmt"):
    lines = []
    lines.append("14.0")  # BCPフォーマットバージョン
    lines.append("95")    # カラム数（合計）

    # 固定カラム定義
    fixed_columns = [
        ("factory", "SQLCHAR", 50),
        ("tag", "SQLCHAR", 50),
        ("date", "SQLCHAR", 10),  # 日付は文字列として扱います
        ("local_tag", "SQLCHAR", 50),
        ("local_id", "SQLCHAR", 50),
        ("name", "SQLCHAR", 100),
        ("unit", "SQLCHAR", 10),
        ("data_division", "SQLINT", 4),  # INT型
    ]
    column_id = 1
    for name, sql_type, size in fixed_columns:
        lines.append(f"{column_id}       {sql_type}       0       {size}      \",\"      {column_id}     {name} SQL_Latin1_General_CP1_CI_AS")
        column_id += 1

    # 動的カラム定義 (d0_0 ~ d3_29)
    for i in range(30):
        for prefix, sql_type, size in [
            ("d0", "SQLINT", 4),  # INT型
            ("d1", "SQLFLT8", 8),  # FLOAT型
            ("d2", "SQLFLT8", 8),  # FLOAT型
            ("d3", "SQLFLT8", 8),  # FLOAT型
        ]:
            lines.append(f"{column_id}       {sql_type}       0       {size}      \",\"      {column_id}     {prefix}_{i} SQL_Latin1_General_CP1_CI_AS")
            column_id += 1

    # 最終カラム (last_update)
    lines.append(f"{column_id}       SQLCHAR       0       23      \"\\r\\n\"   {column_id}     last_update SQL_Latin1_General_CP1_CI_AS")

    # フォーマットファイルを出力
    with open(output_file, "w") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    # フォーマットファイルを生成
    generate_bcp_format_file()
