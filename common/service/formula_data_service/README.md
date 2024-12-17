
# FormulaDataService

`FormulaDataService` は TEM プロジェクト内のビジネスロジック層を担当するクラスであり、以下のような機能を提供します:

- 演算式の取得およびリスト化
- 演算式の構文検証
- 演算結果の保存と関連する操作

## 機能一覧

### 1. `get_formula_by_id(formula_id: str) -> str`
指定された ID に基づいて演算式を取得します。

#### パラメータ
- `formula_id` (str): 取得する演算式の一意の識別子。

#### 戻り値
- `str`: 対応する演算式。

---

### 2. `list_all_formulas() -> dict`
全ての演算式をリスト形式で取得します。

#### 戻り値
- `dict`: 全演算式のデータ。

---

### 3. `validate_formula(formula: str) -> bool`
指定された演算式が正しい構文であるかを検証します。

#### パラメータ
- `formula` (str): 検証対象の演算式。

#### 戻り値
- `bool`: 構文が正しい場合は `True` を返します。

---

### 4. `save_calculation_results(df: pd.DataFrame, destination: str) -> bool`
計算結果を指定の宛先に保存します。

#### パラメータ
- `df` (pd.DataFrame): 保存する計算結果のデータ。
- `destination` (str): 保存先の宛先名。

#### 戻り値
- `bool`: 保存が成功した場合は `True` を返します。

---

## 利用方法

1. FormulaDataService クラスをインスタンス化します。
2. 必要に応じてリポジトリやロガーを依存関係として渡してください。

```python
from formula_data_service import FormulaDataService

# リポジトリ、サービス、ロガーを準備
formula_repository = FormulaRepository()
sensor_data_service = SensorDataService()
logger = Logger()

# FormulaDataService のインスタンス作成
service = FormulaDataService(
    formula_repository=formula_repository,
    sensor_data_service=sensor_data_service,
    logger=logger
)

# 使用例
formula = service.get_formula_by_id("formula_001")
print("取得した演算式:", formula)
```

---

## 注意事項
- `validate_formula` メソッドは Python 構文に基づいて検証を行います。
- `save_calculation_results` は DataFrame が空の場合に警告を記録し、保存をスキップします。

---

## 作者
- TEM プロジェクトチーム
