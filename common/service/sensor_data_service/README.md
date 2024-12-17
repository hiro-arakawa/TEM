
# SensorDataService

## 概要
`SensorDataService` はセンサーデータを操作・管理するためのビジネスロジック層を提供します。このクラスはリポジトリ (`SensorDataRepository`) を通じて、データベースからセンサーデータを取得・保存・削除する操作を行います。

## 主な機能
- センサーデータの取得 (`get_sensor_data`, `get_sensor_data_as_dto`)
- センサーデータの保存 (`save_sensor_data`, `save_calculation_result`)
- センサーデータの削除 (`delete_sensor_data`, `delete_calculation_result`)

## クラス詳細

### `SensorDataService`
センサーデータを操作するための主要クラス。

#### コンストラクタ
```python
def __init__(self, repository: SensorDataRepository):
```
- **引数**
  - `repository` : `SensorDataRepository` のインスタンス

#### メソッド一覧

##### `get_sensor_data(tags: List[str], date: str) -> pd.DataFrame`
指定されたタグと日付に基づいてセンサーデータを取得します。

- **引数**
  - `tags` : 取得するタグのリスト
  - `date` : データを取得する対象の日付（YYYY-MM-DD形式）
- **戻り値**
  - センサーデータを格納した `pandas.DataFrame`
- **例外**
  - `ValueError` : `tags` が `None` の場合
  - `KeyError` : テーブル名の設定が存在しない場合

##### `save_sensor_data(df: pd.DataFrame) -> bool`
センサーデータを保存します。

- **引数**
  - `df` : 保存するセンサーデータを格納した `pandas.DataFrame`
- **戻り値**
  - 保存が成功した場合は `True`
- **例外**
  - `ValueError` : 引数のデータフレームが空の場合

##### `save_calculation_result(df: pd.DataFrame) -> bool`
計算結果を保存します。

- **引数**
  - `df` : 計算結果を格納した `pandas.DataFrame`
- **戻り値**
  - 保存が成功した場合は `True`

##### `delete_sensor_data(tags: List[str], date: str) -> bool`
センサーデータを削除します。

- **引数**
  - `tags` : 削除対象のタグリスト
  - `date` : 削除対象の日付（YYYY-MM-DD形式）
- **戻り値**
  - 削除が成功した場合は `True`

##### `delete_calculation_result(tags: List[str], date: str) -> bool`
計算結果を削除します。

- **引数**
  - `tags` : 削除対象のタグリスト
  - `date` : 削除対象の日付（YYYY-MM-DD形式）
- **戻り値**
  - 削除が成功した場合は `True`

##### `get_sensor_data_as_dto(tags: List[str], date: str) -> List[SensorDataDTO]`
指定されたタグと日付に基づいて正規化された DTO (Data Transfer Object) 形式でデータを取得します。

- **引数**
  - `tags` : 取得するタグのリスト
  - `date` : データを取得する対象の日付（YYYY-MM-DD形式）
- **戻り値**
  - センサーデータ DTO のリスト
- **例外**
  - `ValueError` : DTO の形式が不正な場合

## 使用例
```python
from common.repository.sensor_data_repository import SensorDataRepository
from common.service.sensor_data_service import SensorDataService

# リポジトリのインスタンスを作成
repository = SensorDataRepository()

# サービスを初期化
service = SensorDataService(repository)

# データを取得
tags = ["temperature", "humidity"]
date = "2024-12-17"
sensor_data = service.get_sensor_data(tags, date)

# データを保存
service.save_sensor_data(sensor_data)

# データを削除
service.delete_sensor_data(tags, date)
```

## 注意点
- このサービスは `SensorDataRepository` に依存しています。適切なリポジトリの実装を提供する必要があります。
- センサーデータの正規化や DTO の検証はサービス層で行います。
