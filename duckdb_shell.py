import IPython
import duckdb
import pandas as pd

from pathlib import Path
import glob

from types import SimpleNamespace

def init_db():
    db = duckdb.connect(":memory:")
    all_files=glob.glob("data/*.csv")

    for file in all_files:
        tblname=Path(file).stem
        db.execute(f"create view {tblname} as select * from read_csv_auto('{file}', ALL_VARCHAR=1)")

    col=db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()
    
    # expose views as namespace; this way we can tab-autocomplete them
    tables = SimpleNamespace(**{tb: db.view(tb) for tb in col.table_name.drop_duplicates().values})
    return db, tables

def main():
    db, tables = init_db()
    print("Tables available:")
    print(", ".join(list([x for x in tables.__dict__.keys()])))

    IPython.embed()
    

if __name__ == '__main__':
    main()