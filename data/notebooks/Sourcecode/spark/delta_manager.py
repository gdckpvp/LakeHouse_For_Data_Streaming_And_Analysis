from delta.tables import DeltaTable


def optimize_delta(spark,table_path):
    try:
        deltaTable = DeltaTable.forPath(spark, table_path)
        deltaTable.optimize().executeCompaction()
    except Exception as e:
        print(f'{e}')

def vacuum_delta(spark,table_path):
    try:
        deltaTable = DeltaTable.forPath(spark, table_path)
        print(f"Vacuuming data after optimization @ {table_path}")
        deltaTable.vacuum(24)
    except Exception as e:
        print(f"{e}")

def zorder_delta(spark,table_path):
    try:
        deltaTable = DeltaTable.forPath(spark, table_path)
        deltaTable.optimize().executeZOrderBy("coin_id")
    except Exception as e:
        raise(e)