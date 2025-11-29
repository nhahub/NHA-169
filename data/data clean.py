# Project: Student Data Cleaning & Analysis 
import pandas as pd
import numpy as np
import os
print("Starting Data Cleaning & Analysis Project...\n")

def safe_read_excel(path, name):
    try:
        df = pd.read_excel(path)
        print(f"{name} (Excel) loaded successfully ({df.shape[0]} rows).")
        return df
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return pd.DataFrame()

# Load Data
school_data3 = safe_read_excel("school_data3.xlsx", "School Data 3")
school_data = safe_read_excel("School_Data_2023_2024_Final.xlsx", "School Data")  

def clean_dataframe(df, name="DataFrame"):
    if df.empty:
        print(f"{name} is empty — skipping cleaning.")
        return df

    print(f"\nCleaning: {name}")
    initial_shape = df.shape

    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass

    for col in df.columns:
        if df[col].dtype in [np.float64, np.int64]:
            mean_value = df[col].mean()
            df[col] = df[col].fillna(mean_value)
        else:
            df[col] = df[col].fillna("Unknown")

    before_dup = df.shape[0]
    df = df.drop_duplicates()
    print(f"  - Removed duplicates: {before_dup - df.shape[0]} rows")

    # إزالة القيم الشاذة
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        Q1 = df[numeric_cols].quantile(0.25)
        Q3 = df[numeric_cols].quantile(0.75)
        IQR = Q3 - Q1
        condition = ~((df[numeric_cols] < (Q1 - 1.5 * IQR)) |
                      (df[numeric_cols] > (Q3 + 1.5 * IQR))).any(axis=1)
        df = df[condition]
        print(f"  - Outliers removed from {len(numeric_cols)} numeric columns.")

    print(f"Final shape: {df.shape[0]} rows × {df.shape[1]} columns")
    return df

school_data3 = clean_dataframe(school_data3, "School Data 3")
school_data = clean_dataframe(school_data, "School Data")

print("\nMerging all datasets...")

merge_key = "student_id"  

print("Columns in school_data3:", list(school_data3.columns))
print("Columns in school_data:", list(school_data.columns))

if merge_key in school_data3.columns and merge_key in school_data.columns:
    try:
        merged = pd.merge(school_data3, school_data, on=merge_key, how="outer")
        print(f"Merged successfully using '{merge_key}'")
    except Exception as e:
        print(f"Merge failed: {e}")
        merged = pd.concat([school_data3, school_data], ignore_index=True)
else:
    print(f"Column '{merge_key}' not found in one or both files — using concat instead.")
    merged = pd.concat([school_data3, school_data], ignore_index=True)

print(f"Final merged shape: {merged.shape}")

if 'evaluation_date' in merged.columns:
    merged['evaluation_date'] = pd.to_numeric(merged['evaluation_date'], errors='coerce')
    try:
        merged['evaluation_date'] = pd.to_datetime(merged['evaluation_date'], unit='ns', errors='coerce')
    except:
        merged['evaluation_date'] = pd.to_datetime(merged['evaluation_date'], errors='coerce')
    merged['evaluation_date'] = merged['evaluation_date'].dt.date  # نخليها شكل YYYY-MM-DD

for col in ['health_status', 'remarks']:
    if col in merged.columns:
        merged = merged.drop(columns=[col])
        print(f"Column '{col}' removed successfully.")

# Replace Unknown
for col in merged.columns:
    if merged[col].dtype == object:
        merged[col] = merged[col].replace("Unknown", np.nan)
        merged[col] = merged[col].fillna("Not Provided")

numeric_cols = merged.select_dtypes(include=[np.number]).columns
merged[numeric_cols] = merged[numeric_cols].round(1)

try:
    merged.to_csv("cleaned_data.csv", index=False, encoding="utf-8-sig")
    merged.to_excel("cleaned_data.xlsx", index=False, engine="openpyxl")
    print("\nSaved cleaned data as:")
    print("   - cleaned_data.csv")
    print("   - cleaned_data.xlsx")
except Exception as e:
    print(f"Error saving final files: {e}")

print(f"Final merged shape: {merged.shape}")

output_csv = "new_data_cleaning.csv"
output_xlsx = "new_data_cleaning.xlsx"

print(f"\nSaving files to: {os.getcwd()}")
merged.to_csv(output_csv, index=False)
merged.to_excel(output_xlsx, index=False)

print("\nData Cleaning, Merging & Formatting Complete!")
print(f"Saved cleaned data as:\n - {output_csv}\n - {output_xlsx}")

