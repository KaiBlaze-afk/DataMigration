import pandas as pd

def flatten_dataframe(df):

    df = df.copy()
    changed = True

    while changed:
        changed = False

        dict_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict)).any()]

        for col in dict_cols:
            col_data = df[col].apply(lambda x: x if isinstance(x, dict) else {})
            normalized = pd.json_normalize(col_data)
            normalized.columns = [f"{col}_{c}" for c in normalized.columns]
            normalized.index = df.index

            df = df.drop(columns=[col])
            df = pd.concat([df, normalized], axis=1)
            changed = True

        list_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list) and len(x) > 0).any()]

        if list_cols:
            for col in list_cols:
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [x])

            for col in list_cols:
                df = df.explode(col, ignore_index=True)

            changed = True

    return df
