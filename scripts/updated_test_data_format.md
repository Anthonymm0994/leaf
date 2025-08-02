## 🧾 **Full Column List and Definitions**

### ⏱ Time Columns (2)

| Column      | Description                                                                                                                                                                               |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `good_time` | Time in `HH:MM:SS.sss` format, **always increasing**. Each value is duplicated 1–5 times in a row.                                                                                        |
| `dumb_time` | Time offset **1–5 minutes after** the `good_time` (can include seconds). In each 200–500 row group, the **first row has an empty value** in this column. All other rows have valid times. |

---

### 🔢 Core Numeric & Categorical Columns (7)

| Column                        | Type        | Notes                                                      |
| ----------------------------- | ----------- | ---------------------------------------------------------- |
| `width`                       | Real        | Values from **1.00 to 200.00**, two decimal places         |
| `height`                      | Real        | Values from **0.2 to 4.8**, one decimal place              |
| `angle`                       | Real        | Values from **0.00 to 360.00**, two decimal places         |
| `category_3` to `category_10` | Categorical | 8 columns, with 3–10 unique values per column respectively |

---

### 🔘 Boolean Columns (5)

| Column      | Description        |
| ----------- | ------------------ |
| `isGood`    | true/false, random |
| `isOld`     | true/false, random |
| `isWhat`    | true/false, random |
| `isEnabled` | true/false, random |
| `isFlagged` | true/false, random |

---

### 🧪 Inference Stress Test Columns (20 total)

Each type below has two versions:

* `_infer_blank`: contains **empty cells** (real missing values)
* `_infer_dash`: contains **"-"** string values

| Type             | Columns                                       |
| ---------------- | --------------------------------------------- |
| Integer          | `integer_infer_blank`, `integer_infer_dash`   |
| Real             | `real_infer_blank`, `real_infer_dash`         |
| Text             | `text_infer_blank`, `text_infer_dash`         |
| Boolean          | `bool_infer_blank`, `bool_infer_dash`         |
| Date             | `date_infer_blank`, `date_infer_dash`         |
| DateTime         | `datetime_infer_blank`, `datetime_infer_dash` |
| TimeSeconds      | `time_sec_infer_blank`, `time_sec_infer_dash` |
| TimeMilliseconds | `time_ms_infer_blank`, `time_ms_infer_dash`   |
| TimeMicroseconds | `time_us_infer_blank`, `time_us_infer_dash`   |
| TimeNanoseconds  | `time_ns_infer_blank`, `time_ns_infer_dash`   |
| Blob             | `blob_infer_blank`, `blob_infer_dash`         |

> ⚠️ Note: These do **not** include empty strings like `""`, only actual empty cells or literal dash `"-"`.

---

### 🏷️ Multi-Value Column (1)

| Column | Description                                                                                 |
| ------ | ------------------------------------------------------------------------------------------- |
| `tags` | Possible values: `"a"`, `"a,b"`, `"a,b,c"`, or `""` (empty string is allowed **only here**) |

---

### 📊 Distribution Test Columns (6)

Each column simulates a unique statistical distribution across rows:

| Column         | Description                                    |
| -------------- | ---------------------------------------------- |
| `normal`       | Basic normal distribution, **mean = 50**       |
| `bimodal`      | Two clusters (e.g., centered around 30 and 70) |
| `skewed_left`  | More values on the right, tail on the left     |
| `skewed_right` | More values on the left, tail on the right     |
| `uniform`      | Evenly distributed values                      |
| `exponential`  | Exponentially decaying distribution            |

---

## 🔁 Grouping & Duplication Behavior

* Data is generated in **row groups of 200–500 rows**.
* In **each group**:

  * `good_time` starts at a new point (progressing forward in time)
  * `dumb_time` for the **first row** is **empty**, the rest have time offsets from `good_time`
* After generating a group:

  * **80%** of the time: a new group is generated.
  * **15%** of the time: the group is **duplicated once**, but both `good_time` and `dumb_time` are updated to occur later in time.
  * **5%** of the time: the group is **duplicated twice** (total of 3 versions), each with updated `good_time` and `dumb_time`.

> All time values across the dataset **strictly increase**, even for duplicated groups.

---

## 📦 Summary

### ✅ Column Count: **52 columns total**

* Time Columns: 2
* Base Numerical/Categorical: 7
* Booleans: 5
* Inference Stress Columns: 20
* Multi-value: 1
* Distribution columns: 6
* **Total: 52**

---

### 📚 Type Breakdown

| Type                   | Count |
| ---------------------- | ----- |
| Time                   | 2     |
| Numerical              | 3     |
| Categorical            | 8     |
| Boolean                | 5     |
| Inference Stress       | 20    |
| Multi-value            | 1     |
| Simulated Distribution | 6     |

