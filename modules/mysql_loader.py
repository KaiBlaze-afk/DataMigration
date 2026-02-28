import pymysql
import pandas as pd
from datetime import timezone
from modules.type_converter import get_column_types, prepare_dataframe_for_mysql

def sanitize_column_name(col):
    return col.replace(".", "_").replace(" ", "_").replace("-", "_").replace("$", "")


def create_database(mysql_config):
    db_name = mysql_config["database"]
    base_config = {k: v for k, v in mysql_config.items() if k != "database"}

    conn = pymysql.connect(**base_config)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    cur.close()
    conn.close()


def get_max_updated_at(table_name, mysql_config):
    conn = pymysql.connect(**mysql_config)
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cur.fetchone():
            return None

        cur.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE 'updated_at'")
        if not cur.fetchone():
            return None

        cur.execute(f"SELECT MAX(updated_at) FROM `{table_name}`")
        row = cur.fetchone()
        if row and row[0] is not None:
            dt = pd.to_datetime(row[0])
            dt = dt.tz_localize(timezone.utc) if dt.tz is None else dt.tz_convert(timezone.utc)
            return dt
        return None
    finally:
        cur.close()
        conn.close()


def _prepare_df(df):
    """Convert types, sanitize column names, infer MySQL column types."""
    df = prepare_dataframe_for_mysql(df)
    sanitized_cols = [sanitize_column_name(c) for c in df.columns]
    df.columns = sanitized_cols
    column_types = get_column_types(df)
    return df, sanitized_cols, column_types


def _ensure_table(cur, table_name, sanitized_cols, column_types):
    """CREATE TABLE on first run. Includes id PK, all data columns, is_deleted, and _id index."""
    col_definitions = ", ".join([f"`{col}` {column_types[col]}" for col in sanitized_cols])

    if "_id" in sanitized_cols:
        create_sql = (
            f"CREATE TABLE `{table_name}` "
            f"(id INT PRIMARY KEY, {col_definitions}, is_deleted TINYINT(1) DEFAULT 0, "
            f"INDEX `idx_id` (`_id`))"
        )
    else:
        create_sql = (
            f"CREATE TABLE `{table_name}` "
            f"(id INT PRIMARY KEY, {col_definitions}, is_deleted TINYINT(1) DEFAULT 0)"
        )

    cur.execute(create_sql)
    print(f"  Table created.")


def _evolve_schema(cur, table_name, sanitized_cols, column_types):
    """
    Schema evolution for incremental loads:
    - ADD COLUMN for any new fields that appeared in MongoDB.
    - Ensure is_deleted column exists.
    - Drop stale unique_id index if present.
    """
    cur.execute(f"DESCRIBE `{table_name}`")
    existing_columns = {row[0] for row in cur.fetchall()}

    for col in sanitized_cols:
        if col not in existing_columns:
            cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {column_types[col]}")
            print(f"  + Column added: {col}")

    if "is_deleted" not in existing_columns:
        cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `is_deleted` TINYINT(1) DEFAULT 0")
        print(f"  + Column added: is_deleted")

    try:
        cur.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'unique_id'")
        if cur.fetchone():
            cur.execute(f"ALTER TABLE `{table_name}` DROP INDEX `unique_id`")
            print(f"  - Dropped stale index: unique_id")
    except Exception:
        pass


def _row_fingerprint(row_dict, fp_cols):
    """Deterministic fingerprint of a row, excluding id / is_deleted / updated_at."""
    return "||".join(str(row_dict.get(c)) for c in fp_cols)


def _upsert_by_id(cur, table_name, df, sanitized_cols):
    """
    Per-_id upsert strategy:

    For each unique _id in the incoming DataFrame:
      1. Fetch all currently active MySQL rows for that _id.
      2. Build a fingerprint map (content hash → list of row ids).
      3. For each incoming row:
           - If fingerprint matches an existing active row → keep it (undelete if needed).
           - Otherwise → reuse a spare slot (UPDATE) or INSERT a brand-new row.
      4. Any leftover MySQL rows that had no matching incoming row → mark is_deleted = 1.

    This ensures:
      - Identical rows are never duplicated.
      - Previously deleted rows with the same content are revived (is_deleted = 0).
      - Rows removed from MongoDB are soft-deleted, never hard-deleted from MySQL.
    """
    fp_exclude = {"id", "is_deleted", "updated_at"}
    fp_cols = [c for c in sanitized_cols if c not in fp_exclude]
    col_select = ", ".join([f"`{c}`" for c in sanitized_cols])

    for uid in df["_id"].unique():
        # Fetch all active rows for this _id from MySQL
        cur.execute(
            f"SELECT id, {col_select} FROM `{table_name}` "
            f"WHERE `_id` = %s AND `is_deleted` = 0 ORDER BY id",
            (uid,),
        )
        existing_rows = cur.fetchall()
        col_names_with_id = ["id"] + sanitized_cols

        # Map fingerprint → list of MySQL row ids (FIFO)
        existing_fp_map = {}
        for erow in existing_rows:
            erow_dict = dict(zip(col_names_with_id, erow))
            fp = _row_fingerprint(erow_dict, fp_cols)
            existing_fp_map.setdefault(fp, []).append(erow_dict["id"])

        matched_mysql_ids = set()
        rows_needing_slot = []

        for _, row in df[df["_id"] == uid].iterrows():
            row_data = row.values.tolist()
            row_dict = dict(zip(sanitized_cols, row_data))
            fp = _row_fingerprint(row_dict, fp_cols)

            if fp in existing_fp_map and existing_fp_map[fp]:
                # Exact match — row already exists; ensure it is active
                mysql_id = existing_fp_map[fp].pop(0)
                matched_mysql_ids.add(mysql_id)
                if "updated_at" in sanitized_cols:
                    cur.execute(
                        f"UPDATE `{table_name}` SET `is_deleted` = 0, `updated_at` = %s WHERE id = %s",
                        (row_dict.get("updated_at"), mysql_id),
                    )
                else:
                    cur.execute(
                        f"UPDATE `{table_name}` SET `is_deleted` = 0 WHERE id = %s",
                        (mysql_id,),
                    )
            else:
                # No match — needs a slot (spare or new)
                rows_needing_slot.append((row_data, row_dict))

        # IDs that were active but not matched by any incoming row
        spare_ids = [erow[0] for erow in existing_rows if erow[0] not in matched_mysql_ids]

        for row_data, row_dict in rows_needing_slot:
            if spare_ids:
                # Reuse an existing row slot
                mysql_id = spare_ids.pop(0)
                matched_mysql_ids.add(mysql_id)
                set_clause = ", ".join([f"`{c}` = %s" for c in sanitized_cols])
                cur.execute(
                    f"UPDATE `{table_name}` SET {set_clause}, `is_deleted` = 0 WHERE id = %s",
                    row_data + [mysql_id],
                )
            else:
                # No spare slot — insert a brand-new row
                cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM `{table_name}`")
                next_id = cur.fetchone()[0] + 1
                columns = "id, " + ", ".join([f"`{c}`" for c in sanitized_cols])
                placeholders = ", ".join(["%s"] * (len(sanitized_cols) + 1))
                cur.execute(
                    f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})",
                    [next_id] + row_data,
                )
                matched_mysql_ids.add(next_id)

        # Soft-delete rows that exist in MySQL but are gone from MongoDB
        if spare_ids:
            cur.execute(
                f"UPDATE `{table_name}` SET `is_deleted` = 1 "
                f"WHERE id IN ({','.join(['%s'] * len(spare_ids))})",
                spare_ids,
            )


def _insert_bulk(cur, table_name, df, sanitized_cols):
    """Simple bulk INSERT for collections that have no _id field."""
    cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM `{table_name}`")
    next_id = cur.fetchone()[0] + 1
    columns = "id, " + ", ".join([f"`{c}`" for c in sanitized_cols])
    placeholders = ", ".join(["%s"] * (len(sanitized_cols) + 1))
    data = [[next_id + i] + row for i, row in enumerate(df.values.tolist())]
    cur.executemany(
        f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})",
        data,
    )


def load_dataframe(table_name, df, mysql_config):
    if df.empty:
        print(f"[{table_name}] No data — skipping.")
        return

    print(f"  Loading {len(df)} records...")

    df, sanitized_cols, column_types = _prepare_df(df)

    conn = pymysql.connect(**mysql_config)
    cur = conn.cursor()

    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cur.fetchone()

    if not table_exists:
        _ensure_table(cur, table_name, sanitized_cols, column_types)
    else:
        _evolve_schema(cur, table_name, sanitized_cols, column_types)

    if "_id" in sanitized_cols:
        _upsert_by_id(cur, table_name, df, sanitized_cols)
    else:
        _insert_bulk(cur, table_name, df, sanitized_cols)

    conn.commit()
    cur.close()
    conn.close()

    print(f"  Done. ({len(df)} records)")


def mark_deleted_documents(table_name, mongo_ids, mysql_config):
    mongo_id_set = {str(mid) for mid in mongo_ids}

    conn = pymysql.connect(**mysql_config)
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT `_id` FROM `{table_name}` WHERE `is_deleted` = 0")
        mysql_id_set = {str(row[0]) for row in cur.fetchall()}

        mongo_count = len(mongo_id_set)
        mysql_count = len(mysql_id_set)
        print(f"  [{table_name}] Mongo: {mongo_count} | MySQL: {mysql_count}")

        if mongo_count != mysql_count:
            deleted_ids = mysql_id_set - mongo_id_set
            if deleted_ids:
                placeholders = ", ".join(["%s"] * len(deleted_ids))
                cur.execute(
                    f"UPDATE `{table_name}` SET `is_deleted` = 1 "
                    f"WHERE `_id` IN ({placeholders}) AND `is_deleted` = 0",
                    list(deleted_ids),
                )
                conn.commit()
                print(f"  {len(deleted_ids)} marked deleted: {deleted_ids}")
            else:
                print(f"  Count mismatch — no specific IDs found.")
    finally:
        cur.close()
        conn.close()
