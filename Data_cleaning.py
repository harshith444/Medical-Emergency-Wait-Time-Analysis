import pandas as pd

excel_path = "/Users/harshithkatakam/Desktop/Harshith/SEM4/DATA.xlsx"
df_hospitals = pd.read_excel(excel_path, sheet_name="Hospitals")
df_metadata = pd.read_excel(excel_path, sheet_name="Measure_metadata")
df_performance = pd.read_excel(excel_path, sheet_name="Performance measures")
df_hospitals['facility_id'] = df_hospitals['facility_id'].astype(str).str.lstrip('0')
df_performance['facility_id'] = df_performance['facility_id'].astype(str).str.lstrip('0')
wait_measure_ids = ['EDV', 'ED_2_Strata_1', 'ED_2_Strata_2', 'OP_18b', 'OP_18c', 'OP_22', 'OP_23', 'OP_29', 'OP_31', 'SEP_1', 'STK_05']
df_performance = df_performance[df_performance['measure_id'].isin(wait_measure_ids)]
# Clean score column
df_performance = df_performance[df_performance['score'].notna()]
df_performance = df_performance[df_performance['score'] != 'Not Available']

df_performance['score_numeric'] = pd.to_numeric(df_performance['score'], errors='coerce')
df_performance.loc[df_performance['measure_id'] == 'EDV', 'score_numeric'] = (
    df_performance[df_performance['measure_id'] == 'EDV']['score']
    .map({'very high': 4, 'high': 3, 'medium': 2, 'low': 1})
)
df_performance['start_date'] = pd.to_datetime(df_performance['start_date'])
df_performance['end_date'] = pd.to_datetime(df_performance['end_date'])
df_performance['sample_numeric'] = pd.to_numeric(df_performance['sample'], errors='coerce')
df_performance['low_sample_flag'] = df_performance['sample_numeric'] < 25
df_performance['has_footnote'] = df_performance['footnote'].notna()
merged = df_performance.merge(df_hospitals, on='facility_id', how='left')
merged['quarter'] = merged['start_date'].dt.to_period('Q').astype(str)
merged['year'] = merged['start_date'].dt.year
state_to_region = {
    'CT': 'Northeast', 'ME': 'Northeast', 'MA': 'Northeast', 'NH': 'Northeast', 'RI': 'Northeast', 'VT': 'Northeast',
    'NJ': 'Northeast', 'NY': 'Northeast', 'PA': 'Northeast',
    'IL': 'Midwest', 'IN': 'Midwest', 'MI': 'Midwest', 'OH': 'Midwest', 'WI': 'Midwest',
    'IA': 'Midwest', 'KS': 'Midwest', 'MN': 'Midwest', 'MO': 'Midwest', 'NE': 'Midwest', 'ND': 'Midwest', 'SD': 'Midwest',
    'DE': 'South', 'FL': 'South', 'GA': 'South', 'MD': 'South', 'NC': 'South', 'SC': 'South', 'VA': 'South', 'DC': 'South',
    'WV': 'South', 'AL': 'South', 'KY': 'South', 'MS': 'South', 'TN': 'South', 'AR': 'South', 'LA': 'South', 'OK': 'South', 'TX': 'South',
    'AZ': 'West', 'CO': 'West', 'ID': 'West', 'MT': 'West', 'NV': 'West', 'NM': 'West', 'UT': 'West', 'WY': 'West',
    'AK': 'West', 'CA': 'West', 'HI': 'West', 'OR': 'West', 'WA': 'West'
}
merged['region'] = merged['state'].map(state_to_region)
# create hospital label for visualizations
merged['hospital_display'] = merged['facility_name'] + ' (' + merged['state'] + ')'
# Outlier (IQR method)
def detect_outliers_iqr(df, group_col='measure_id', value_col='score_numeric'):
    outlier_flags = pd.Series([False] * len(df), index=df.index)
    for measure in df[group_col].unique():
        subset = df[df[group_col] == measure]
        q1 = subset[value_col].quantile(0.25)
        q3 = subset[value_col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        condition = (df[group_col] == measure) & (
            (df[value_col] < lower) | (df[value_col] > upper)
        )
        outlier_flags[condition] = True
    return outlier_flags
merged['is_outlier'] = detect_outliers_iqr(merged)
merged['score_scaled'] = merged.groupby('measure_id')['score_numeric'].transform(
    lambda x: (x - x.min()) / (x.max() - x.min())
)
# z-score
merged['score_standardized'] = merged.groupby('measure_id')['score_numeric'].transform(
    lambda x: (x - x.mean()) / x.std()
)
def performance_level(z):
    if pd.isnull(z):
        return 'Unknown'
    elif z <= -1:
        return 'Above Average'
    elif z >= 1:
        return 'Below Average'
    else:
        return 'Average'

merged['performance_level'] = merged['score_standardized'].apply(performance_level)
merged.to_csv("clean_wait_time_data.csv", index=False)