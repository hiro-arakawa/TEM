
# BatchService

## 概要

`BatchService` は、TEMシステムの一部として設計されたサービスであり、複数のバッチ処理を管理し、依存関係やスケジュールに基づいてバッチの実行を制御します。

- **依存関係管理**: 特定のバッチが他のバッチに依存している場合、その依存関係を考慮して実行します。
- **スケジュール管理**: バッチごとに設定された曜日にのみ実行されるよう制御します。
- **再試行機能**: バッチが失敗した場合、指定された回数まで再試行を行います。
- **通知機能**: バッチが最大再試行回数を超えて失敗した場合、エラー通知を送信します。
- **循環依存検出**: 循環依存が存在する場合、エラーをスローします。

## 機能

### 1. バッチスケジュール
- バッチは曜日や依存関係を考慮してスケジュールされます。
- 必要に応じて動的にスケジュールを変更可能です。

### 2. 依存関係管理
- バッチは設定された依存関係が満たされている場合にのみ実行されます。

### 3. 再試行管理
- 再試行回数を管理し、最大回数を超えた場合にはスキップまたはエラー通知を行います。

### 4. 循環依存検出
- 循環依存が存在する場合は実行前に検出し、エラーをスローします。

## 使用方法

### 初期化

`BatchService` をインスタンス化する際、必要に応じて日時モジュールを差し替えることができます。

```python
from batch_service import BatchService

service = BatchService()
```

### バッチのスケジュール登録

`BatchService` によってバッチをスケジュールします。

```python
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.start()

service.schedule_batches(scheduler)
```

### 再試行の設定

失敗したバッチの再試行は自動で管理されます。最大再試行回数はデフォルトで `3` に設定されていますが、必要に応じて変更できます。

```python
service.max_retry_count = 5
```

### バッチ状態の更新

バッチの成功や失敗を記録します。

```python
service.update_last_processed("バッチ名", "SUCCESS")  # 成功
service.update_last_processed("バッチ名", "FAILED")  # 失敗
```

### バッチスケジュールの動的変更

スケジュールを動的に変更できます。

```python
service.update_batch_schedule("データロード1", ["sat", "sun"])
```

### 循環依存の検出

循環依存があるかを検出します。

```python
try:
    service.detect_circular_dependency()
except Exception as e:
    print(e)  # "Circular dependency detected" のエラーをキャッチ
```

## バッチ構成例

以下は、バッチマスタの構成例です。

```python
batch_master = [
    {"batch_name": "データロード1", "factory_code": "工場A", "depends_on": None, "schedule_days": ["mon", "tue", "wed", "thu", "fri"]},
    {"batch_name": "データロード2", "factory_code": "工場B", "depends_on": None, "schedule_days": ["mon", "wed", "fri"]},
    {"batch_name": "bcp実行", "factory_code": "工場A", "depends_on": "データロード1", "schedule_days": ["mon", "wed", "fri"]},
    {"batch_name": "計算値生成処理", "factory_code": "工場A", "depends_on": "bcp実行", "schedule_days": ["tue", "thu"]},
]
```

## テスト

テストは `pytest` を使用して実行できます。

```bash
pytest common/service/batch_service_test.py
```

## 開発者向け情報

- **循環依存検出**: 開発時に新しいバッチを追加する際、依存関係を明確にすることで循環エラーを防げます。
- **スケジュール更新**: 動的スケジュール変更が必要な場合、`update_batch_schedule` メソッドを利用してください。

## ライセンス

このプロジェクトは [MITライセンス](LICENSE) の下で提供されています。
