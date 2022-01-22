import ipykernel
from ipykernel.ipkernel import IPythonKernel

import sys
import re

import duckdb


def has_open_quotes(s):
    if s.count('"""') % 2:
        return '"""'
    # if triple-quotes were matched, then we can detect single-quote mismatch like this
    if s.count('"') % 2:
        return '"'
    elif s.count("'") % 2:
        return "'"
    else:
        return False

def is_sql_block(code, code_block_marker):
    return code.startswith(code_block_marker)

def is_string_block(code):
    import ast
    # TODO: is there a better way to do this?
    try:
        literal = ast.literal_eval(code)
        if isinstance(literal, str):
            return True
    
    except Exception:
        return False

    return 

def looks_like_sql(code):
    """
    Detect SELECT statement or WITH statement
    """
    lowered = code.lower().strip()
    return lowered.startswith("select") or lowered.startswith("with")


class IPythonDuckdbKernel(IPythonKernel):
    db = None
    sql_block_marker=r'#%%[sql]'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def detect_sql(self, code, cursor_pos):
        open_quote_char = has_open_quotes(code[:cursor_pos])
        if open_quote_char:
            after_last_quote = re.split(re.escape(open_quote_char), code[:cursor_pos])[-1]
        if is_sql_block(code, self.sql_block_marker) or (open_quote_char and looks_like_sql(after_last_quote)):
            return True

    def update_db(self):
        """
        Find duckdb database from user namespace
        """
        # TODO: for performance etc, we could first check if our current db
        # still exists and is open, instead of going through all
        self.db = None
        
        for v in self.user_ns.keys():
            if isinstance(self.user_ns[v], duckdb.DuckDBPyConnection):
                # check it's open as well
                # NOTE: if the user has two db variables, one closed and one open, we
                # may not pick up the open one
                # TODO: any better way to check openness?
                try:
                    self.user_ns[v].query("select 1")  # this should fail only if closed
                    self.db = self.user_ns[v]
                except RuntimeError:
                    self.db = None
                    return False

        if self.db:
            # set up helper table
            # TODO: don't use pandas here, so we only depend on IPython and duckdb
            self.col_table = self.db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()
            return True
        else:
            self.db = None
            # No db, should fall back to ipython
            return False

    def get_sql_matches(self, code, cursor_pos):
        # TODO: spec this so that the matching function is more modular?
        # - it's really just a hierarchy of schema.table.column <- work with that!
        col_table = self.col_table

        # 1. just return the tables
        tables = list(col_table.table_name.unique())
        matches=tables

        # 2. if in a token, get match for token instead
        token_length=0
        
        # find previous whitespace, period, comma, quote or open parenthesis
        until_cursor = code[:cursor_pos]
        match = re.search(r'[\s\.\,\(,\"]', until_cursor[::-1])
        if match:
            token_start = len(until_cursor)-match.end()+1
            token_length = cursor_pos-token_start
            token = code[token_start:cursor_pos]
            r = f"^{re.escape(token)}"

            # TODO: handle aliases
            # so the table is determined when we have `.`
            referred_tables = [x for x in tables if x in code]
            filtered_columns = list(col_table.loc[lambda x: x.table_name.isin(referred_tables)].column_name.unique())
            
            # first recommend column, then table
            if len(r)>0 and (code[token_start-1] in '.,' or len(referred_tables)>0):
                # only columns (TODO: note this assumes no schema.foo.bar syntax)
                # TODO: we need to quote always, or when necessary(spaces in names etc.)
                matches = [x for x in filtered_columns + tables if re.match(r, x)]
            # otherwise recommend all tables first
            else:
                matches = [x for x in tables + filtered_columns if re.match(r, x)]
            
            # quoting:
            # - if we're in a quote, always add end quote
            # - otherwise if column name has non-alphanumeric characters, wrap in quotes
            if code[token_start-1] == '"':
                matches = [x+'"' for x in matches]
            else:
                quotable_chars_re = r'[\s\.\(\)\]\]]'
                matches = [f'"{x}"' if re.search(quotable_chars_re, x) else x for x in matches]
        return matches, token_length
    

    def do_complete(self, code, cursor_pos):
        """
        ipython completion but switching to sql when detected
        """
        if self.update_db() and self.detect_sql(code, cursor_pos):
            matches, cursor_offset = self.get_sql_matches(code, cursor_pos)

            out = {
            'status': 'ok',
            'matches': matches,
            'cursor_start' : cursor_pos-cursor_offset,
            'cursor_end' : cursor_pos,
            'metadata' : {},
            }
        else:
            out = super().do_complete(code, cursor_pos)

        return out


    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        """
        ipython exeuction but sql in special cases
        """
        
        # note: this is a bit different from "detect_sql" for autocomplete,
        # since we also detect user intention to execute
        # remove block marker, surrounding quotes if any, finally surrounding whitespace

        # this preprocessing extracts the sql part we can then pass to duckdb
        sql_code = code.strip().replace(self.sql_block_marker, '').strip().strip("'").strip('"').strip()
        
        # NOTE: as a design choice, we require the SQL code to be a string literal
        # => this way any code executed is also valid python code
        if self.update_db() and \
            (is_string_block(code) and (is_sql_block(code, self.sql_block_marker) or looks_like_sql(sql_code))):

            """
            Pre-execution code for the shell so history and display work correctly
            """
            from IPython.core.interactiveshell import ExecutionInfo
            from IPython.core.interactiveshell import ExecutionResult

            info = ExecutionInfo(code, store_history, silent, shell_futures=True)
            result = ExecutionResult(info)
            if silent:
                store_history = False

            if store_history:
                result.execution_count = self.shell.execution_count
            
            self.shell.events.trigger('pre_execute')
            if not silent:
                self.shell.events.trigger('pre_run_cell', info)
            
            if store_history:
                self.shell.history_manager.store_inputs(self.shell.execution_count, sql_code, code)
                
            try:
                """
                Actually execute the sql
                """
                output_table = self.db.query(sql_code).df()

                """
                Display
                """
                self.shell.displayhook.exec_result = result               
                self.shell.displayhook(output_table)
                
                """
                Post-execution code
                """
                # Reset this so later displayed values do not modify the
                # ExecutionResult
                self.shell.displayhook.exec_result = None
                
                self.shell.last_execution_succeeded = True
                self.shell.last_execution_result = result

                if store_history:
                    self.shell.history_manager.store_output(self.shell.execution_count)
                    self.shell.execution_count += 1
                
                self.shell.events.trigger('post_execute')
                if not silent:
                    self.shell.events.trigger('post_run_cell', result)
                                
                return {
                    'status': 'ok', #'ok' OR 'error' OR 'aborted'
                    'payload': list(),
                    # TODO: what are these again..?
                    'user_expressions': {},
                    'execution_count': self.shell.execution_count-1
                }

            except Exception as err:
                import traceback
                traceback.print_tb(err.__traceback__)

                if store_history:
                    self.shell.execution_count += 1
                result.error_before_exec = err
                self.shell.last_execution_succeeded = False
                self.shell.last_execution_result = result

                return {
                    'status': 'error', #'ok' OR 'error' OR 'aborted'
                    'payload': list(),
                    'traceback': str(err.__traceback__) or [],
                    'ename': str(type(err).__name__),
                    'evalue': str(err),
                    'execution_count': self.shell.execution_count-1
                }
        else:
            return super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
     

def main():
    """
    Launch a Data Science IPython kernel that picks up a duckdb connection and provides
    - Autocompletion of table and column names
    - Helper syntax for querying the database with SQL only
    - Python and sql autocompletion where appropriate
    - TODO: Autocompletion improvements: table + table alias, schema detection, keywords (SELECT * FROM duckdb_keywords() in a future version)
    - TODO: Use something else than #%%[sql] as that will break vscode code cells (comment line not passed to interactive)
    - TODO: How to install simply?
    """
    from ipykernel.kernelapp import IPKernelApp
    app = IPKernelApp.instance(kernel_class=IPythonDuckdbKernel)
    app.initialize(sys.argv[1:])
    
    ipykernel.kernelbase.Kernel.start(app.kernel)

    # TODO: what's the best way to do this?
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
