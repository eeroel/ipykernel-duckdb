import IPython
import ipykernel
from ipykernel.ipkernel import IPythonKernel


import glob
from pathlib import Path
import sys

from types import SimpleNamespace
import duckdb

def is_sql_block(code):
    return code.startswith(r"#%%[sql]")

def detect_sql(code):
    # TODO: detect being within a string and sql autocomplete therein,
    # so we can deal with any inline sql in python
    if is_sql_block(code):
        return True

class Kernel(IPythonKernel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_sql_matches(self, code, cursor_pos):
        import re
        # 1. just return the tables
        tables = list(self.col_table.table_name.unique())
        matches=tables

        # 2. if in a token, get match for token instead
        token_length=0

        # find previous whitespace character
        until_cursor = code[:cursor_pos]
        match = re.search('[\s\.]', until_cursor[::-1])
        if match:
            token_start = len(until_cursor)-match.end()+1
            token_length = cursor_pos-token_start
            token = code[token_start:cursor_pos]
            r = f"^{token}"

            # TODO: handle aliases
            # so the table is determined when we have `.`
            referred_tables = [x for x in tables if x in code]
            filtered_columns = list(self.col_table.loc[lambda x: x.table_name.isin(referred_tables)].column_name.unique())

            if code[token_start-1]=='.' or len(referred_tables)>0:
                # only columns (TODO: note this assumes no schema.foo.bar syntax)
                # TODO: we need to quote always, or when necessary(spaces in names etc.)
                matches = [x for x in filtered_columns + tables if re.match(r, x)]
            else:
                matches = [x for x in tables + filtered_columns if re.match(r, x)]
        
        return matches, token_length
    
    def do_complete(self, code, cursor_pos):
        """
        ipython completion but switching to sql when detected
        """
        if detect_sql(code):
            # TODO: is there a way to pass this at startup instead?
            self.db = self.user_ns["db"]
            # TODO: shouldn't be in user_ns in the first place
            self.col_table = self.user_ns["col_table"]

            matches, cursor_offset = self.get_sql_matches(code, cursor_pos)

            out = {
            # status should be 'ok' unless an exception was raised during the request,
            # in which case it should be 'error', along with the usual error message content
            # in other messages.
            'status': 'ok',

            # The list of all matches to the completion request, such as
            # ['a.isalnum', 'a.isalpha'] for the above example.
            'matches': matches,

            # The range of text that should be replaced by the above matches when a completion is accepted.
            # typically cursor_end is the same as cursor_pos in the request.
            'cursor_start' : cursor_pos-cursor_offset,
            'cursor_end' : cursor_pos,

            # Information that frontend plugins might use for extra display information about completions.
            'metadata' : {},
            }
        else:
            out = super().do_complete(code, cursor_pos)

        return out

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if is_sql_block(code):
            self.db = self.user_ns["db"]
            # TODO: valid sql only, handle errors
            # TODO: this should be sent as display_message?
            output_frame = self.db.query(code.replace(r'#%%[sql]', '')).df().to_html()
            self.execution_count += 1

            return {
                'status': 'ok', #'ok' OR 'error' OR 'aborted'
                'payload': list(),
                'user_expressions': {output_frame},
                'execution_count': self.execution_count
            }
        else:
            return super().do_execute(code, silent, store_history, user_expressions, allow_stdin)

def init_db():
    db = duckdb.connect("foo2.duckdb")
    all_files=glob.glob("data/*.csv")

    for file in all_files:
        tblname=Path(file).stem
        db.execute(f"create view {tblname} as select * from read_csv_auto('{file}', ALL_VARCHAR=1)")

    col=db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()
    
    # expose views as namespace; this way we can tab-autocomplete them
    tables = SimpleNamespace(**{tb: db.view(tb) for tb in col.table_name.drop_duplicates().values})
    return db, tables
     

def main():
    """
    Launch a Data Science IPython kernel with
    - duckdb database with views to CSV files, and the db queryable in the kernel
    - TODO: Python and sql autocompletion where appropriate
    - TODO?: SQL magic to simplify `db.query(...` calls?
    - (A database explorer terminal UI)
    """
    ## db and tables will be available in the kernel
    db = duckdb.connect("foo2.duckdb", read_only=True)

    # dataframe with tables and column names
    col_table=db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()

    # create kernel with asyncio ui support
    from ipykernel.kernelapp import IPKernelApp
    app = IPKernelApp.instance(kernel_class=Kernel)
    app.initialize(["-f","foo.json"])
    
    ipykernel.kernelbase.Kernel.start(app.kernel)

    # set up the relevant variables to pass to the embedded kernel; imitating ipykernel.embed here
    f = sys._getframe(0)
    global_ns = f.f_globals
    module = sys.modules[global_ns['__name__']]
    app.kernel.user_module = module
    app.kernel.user_ns = f.f_locals
    app.shell.set_completer_frame()

    app.start()

    return



if __name__ == '__main__':
    main()