from ipykernel.ipkernel import IPythonKernel
import re
import duckdb
import sys

from typing import Optional, Any


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def detect_sql(self, code, cursor_pos):
        open_quote_char = has_open_quotes(code[:cursor_pos])
        if open_quote_char:
            after_last_quote = re.split(re.escape(open_quote_char), code[:cursor_pos])[-1]
        if open_quote_char and looks_like_sql(after_last_quote):
            return True

    def update_db(self):
        """
        Find duckdb database from user namespace
        """
        # TODO: for performance etc, we could first check if our current db
        # still exists and is open, instead of going through all
        self.db = None
        
        for v in self.shell.user_ns.keys():
            if isinstance(self.shell.user_ns[v], duckdb.DuckDBPyConnection):
                # check it's open as well
                # NOTE: if the user has two db variables, one closed and one open, we
                # may not pick up the open one
                # TODO: any better way to check openness?
                try:
                    self.shell.user_ns[v].query("select 1")  # this should fail only if closed
                    self.db = self.shell.user_ns[v]
                except RuntimeError:
                    self.db = None
                    return False

        if self.db:
            # set up helper table
            # TODO: don't use pandas here, as python containers will do
            self.col_table = self.db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()
            return True
        else:
            self.db = None
            # No db, should fall back to ipython
            return False

    def get_sql_matches(self, code, cursor_pos):
        # TODO: take this as dict
        col_table = self.col_table

        def generate_tables(df):
            """
            generate {table: {columns: ...}} hierarchy, with all variations of
            column names included (i.e. with and without table prefix)
            """
            # pre-quote names if necessary
            quotable_chars_re = r'[\s\.\(\)\]\]]'
            out = {}
            
            quote_name = lambda x: f'"{x}"' if re.search(quotable_chars_re, x) else x
            for row in df.itertuples(index=False):
                table_name = quote_name(row.table_name)
                if not out.get(table_name):
                    out[table_name] = {"columns": []}
                
                out[table_name]["columns"].append(quote_name(row.column_name))
                out[table_name]["columns"].append(table_name + '.' + quote_name(row.column_name))
            
            return out
        
        # 1. just return the tables
        tables = generate_tables(col_table)
        table_names = list(tables.keys())
        matches=table_names

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
            
            # find all tables used so far in the query
            # only match if we're not a subset of another table
            table_re = lambda x: r'(^|[^a-zA-Z_]){}([^a-zA-Z_]|$)'.format(re.escape(x))
            referred_tables = [x for x in table_names if re.search(table_re(x), code)]

            filtered_columns = [x for y in tables for x in tables[y]["columns"] if y in referred_tables]
            
            # first recommend column, then table
            if len(r)>0 and (code[token_start-1] in '.,' or len(referred_tables)>0):
                # only columns (TODO: note this assumes no schema.foo.bar syntax)
                # TODO: we need to quote always, or when necessary(spaces in names etc.)
                matches = [x for x in filtered_columns + table_names if re.match(r, x)]
            # otherwise recommend all tables first
            else:
                matches = [x for x in table_names + filtered_columns if re.match(r, x)]
            
            # quoting:
            # - if we're in a quote, always add end quote
            if code[token_start-1] == '"':
                matches = [x+'"' for x in matches]

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


    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        """
        ipython execution but sql in special cases
        """
        # this preprocessing extracts the sql part we can then pass to duckdb
        # TODO: remove Python comment lines as part of this
        sql_code = code.strip().strip("'").strip('"').strip()
        
        # NOTE: as a design choice, we require the SQL code to be a string literal
        # => this way any code executed is also valid python code
        if self.update_db() and \
            (is_string_block(code) and looks_like_sql(sql_code)):
            return await self.do_execute_sql(sql_code, code, silent, store_history, user_expressions, allow_stdin)
        else:
            return await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
    
    
    async def do_execute_sql(self, sql_code, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        # Temporarily monkey patch the functionality of the ipython shell
        # IPythonKernel's do_execute will handle async and also prepare the output dictionary
        transform_cell_original = self.shell.transform_cell
        run_cell_async_original = self.shell.run_cell_async

        self.shell.run_cell_async = self.run_sql_cell
        self.shell.transform_cell = lambda x: sql_code # we already extracted this, so should be ok

        reply_content = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)

        self.shell.run_cell_async = run_cell_async_original
        self.shell.transform_cell = transform_cell_original
        
        return reply_content


    async def run_sql_cell(
        self,
        raw_cell,
        silent,
        store_history=True,
        shell_futures=True,
        *,
        transformed_cell: Optional[str] = None,
        preprocessing_exc_tuple: Optional[Any] = None):
        # Pre-execution code for the shell so history and display work correctly
        from IPython.core.interactiveshell import ExecutionInfo
        from IPython.core.interactiveshell import ExecutionResult

        try:
            info = ExecutionInfo(raw_cell, store_history, silent, shell_futures)
            result = ExecutionResult(info)
            if silent:
                store_history = False
            if store_history:
                result.execution_count = self.shell.execution_count

            self.shell.events.trigger('pre_execute')
            if not silent:
                self.shell.events.trigger('pre_run_cell', info)
            
            if store_history:
                self.shell.history_manager.store_inputs(self.shell.execution_count, transformed_cell, raw_cell)
            
            self.shell.displayhook.exec_result = result
            has_raised = False
            try:
                # Actually execute the sql
                output_table = self.db.query(transformed_cell).df()

                # Display         
                self.shell.displayhook(output_table)
            except Exception as e:
                has_raised = True
                self.shell.showtraceback()
                result.error_before_exec = sys.exc_info()[1]
            
            # Post-execution code
            self.shell.last_execution_succeeded = not has_raised
            self.shell.last_execution_result = result
            self.shell.displayhook.exec_result = None

            if store_history:
                self.shell.history_manager.store_output(self.shell.execution_count)
                self.shell.execution_count += 1
            
            self.shell.events.trigger('post_execute')
            if not silent:
                self.shell.events.trigger('post_run_cell', result)
        
        except BaseException as e:
            # TODO: why do we have to do this here in case of KeyboardInterrupt,
            # but InteractiveShell doesn't ?
            self.shell.execution_count += 1

            info = ExecutionInfo(raw_cell, store_history, silent, shell_futures)
            result = ExecutionResult(info)
            result.error_in_exec = e
            self.shell.showtraceback(running_compiled_code=True)

        return result


def main():
    """
    Launch a Data Science IPython kernel that detects and open duckdb connection and provides
    - Autocompletion of table and column names
    - Helper syntax for querying the database with SQL only
    - Python and sql autocompletion where appropriate
    - TODO: Autocompletion improvements: table + table alias, schema detection, keywords (SELECT * FROM duckdb_keywords() in a future version)
    - TODO: remove any comment lines as the first thing (for code cell support etc.)
    """
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=IPythonDuckdbKernel)
    return


if __name__ == '__main__':
    main()
