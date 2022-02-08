from ipykernel.ipkernel import IPythonKernel

from IPython.core.magic import needs_local_scope

import re
import duckdb

from types import SimpleNamespace


@needs_local_scope
def sql(line, cell=None, local_ns={}):
    sql_code = cell if cell is not None else line

    db = get_duckdb_from_local_namespace(local_ns)
    if db is not None:
        output_table = db.query(sql_code).df()
        return output_table

def get_duckdb_from_local_namespace(local_ns):
    for v in local_ns.keys():
        if not v.startswith('_') and isinstance(local_ns[v], duckdb.DuckDBPyConnection):
            # check it's open as well
            # NOTE: if the user has two db variables, one closed and one open, we
            # may not pick up the open one
            try:
                local_ns[v].query("select 1")  # this should fail only if closed
                return local_ns[v]
            except RuntimeError:
                return None
    return None


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


def looks_like_sql(code):
    """
    Detect SELECT statement or WITH statement
    """
    lowered = code.lower().strip()
    return lowered.startswith("select") or lowered.startswith("with")


def generate_tables(tables_and_columns):
    """
    generate {table: {columns: ...}} hierarchy, with all variations of
    column names included (i.e. with and without table prefix)
    """
    # Pre-quote names if necessary; the protocol doesn't have a concept of
    # displaying a different string than the actual completion, so the quotes
    # need to be included. For matching we should ignore them though.
    quotable_chars_re = r'[\s\.\(\)\]\]]'
    out = {}
    
    quote_name = lambda x: f'"{x}"' if re.search(quotable_chars_re, x) else x
    for tbl, col in tables_and_columns:
        table_name = quote_name(tbl)
        if not out.get(table_name):
            out[table_name] = {"columns": []}
        name_quoted = quote_name(col)

        # TODO: deduplication key should consider aliases as well
        # + for aliases we never want the non-prefixed column name
        # note that matchers don't include quotes
        out[table_name]["columns"].append(SimpleNamespace(text=name_quoted, matcher=col, key=tbl+col))
        out[table_name]["columns"].append(SimpleNamespace(text=table_name + '.' + name_quoted, matcher=table_name + '.' + col, key=tbl+col))
    
    return out


def get_sql_matches(tables_and_columns, code, cursor_pos):
    tblnames = [x[0] for x in tables_and_columns]

    # find all tables used so far in the query
    # only match if we're not a subset of another table
    table_re = lambda x: r'(^|[^a-zA-Z_]){}(\s+(as|AS))?(?P<alias>\s+\w+)?([^a-zA-Z_]|$)'.format(re.escape(x))
    # table references + aliases
    referred_tables = []

    tables_and_columns_with_aliases = [x for x in tables_and_columns]
    for x in tblnames:
        match = re.search(table_re(x), code)
        if match:
            referred_tables.append(x)
            # if we find an alias, add it to the tables as well
            alias = match.group("alias")
            if alias:
                alias=alias.strip()
                if alias.lower() in ['join', 'inner', 'left', 'right', 'full', 'self', 'union']:
                    continue
                for tbl, col in tables_and_columns:
                    if tbl==x:
                        tables_and_columns_with_aliases.append((alias, col))
                referred_tables.append(alias)

    # 1. just return the tables
    tables = generate_tables(tables_and_columns_with_aliases)
    table_names = [SimpleNamespace(text=x, matcher=x, key=x) for x in list(tables.keys())]
    matches=table_names

    # 2. if in a token, get match for token instead
    token_length=0
    
    # find previous whitespace, comma, or open parenthesis
    # TODO: binary operators; * probably needs special care
    until_cursor = code[:cursor_pos]
    match = re.search(r'[\s\,\(]', until_cursor[::-1])

    if match:
        token_start = len(until_cursor)-match.end()+1
        token_length = cursor_pos-token_start
        token = code[token_start:cursor_pos]

        # remove quote symbols from token
        token = token.replace('"', '')
        r = f"^{re.escape(token)}"

        # recommend only columns from the found tables
        filtered_columns = [x for y in tables for x in tables[y]["columns"] if y in referred_tables]
        
        # first recommend column, then table
        if len(r)>0:
            # only columns (TODO: note this assumes no schema.foo.bar syntax)
            matches = [x for x in filtered_columns + table_names if re.match(r, x.matcher)]
        # otherwise recommend all tables first
        else:
            matches = [x for x in table_names + filtered_columns if re.match(r, x.matcher)]
    
    deduped = []
    keys = set()
    for item in matches:
        if item.key not in keys:
            keys.add(item.key)
            deduped.append(item.text)
    
    return deduped, token_length


class IPythonDuckdbKernel(IPythonKernel):
    db = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def detect_sql(self, code, cursor_pos):
        open_quote_char = has_open_quotes(code[:cursor_pos])
        if open_quote_char:
            after_last_quote = re.split(re.escape(open_quote_char), code[:cursor_pos])[-1]
            if looks_like_sql(after_last_quote):
                return True

        # sql magic
        elif '%sql' in code:
            after_magic = re.split(r'\%sql', code[:cursor_pos])[-1]
            if looks_like_sql(after_magic):
                return True

    def update_db(self):
        """
        Find duckdb database from user namespace
        """
        self.db = None
        self.db = get_duckdb_from_local_namespace(self.shell.user_ns)

        if self.db:
            # set up helper table
            self.tables_and_columns = self.db.query("""
                select t.table_name, c.column_name
                from INFORMATION_SCHEMA.tables t
                join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name
            """).fetchall()
            return True
        else:
            self.db = None
            # No db, should fall back to ipython
            return False
    

    def do_complete(self, code, cursor_pos):
        """
        ipython completion but switching to sql when detected
        """
        if self.update_db() and self.detect_sql(code, cursor_pos):
            matches, cursor_offset = get_sql_matches(self.tables_and_columns, code, cursor_pos)

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


def main():
    """
    Launch a Data Science IPython kernel that detects and open duckdb connection and provides
    - Autocompletion of table and column names when SQL is detected
    - %sql, %%sql magics for quick queries
    """
    from ipykernel.kernelapp import IPKernelApp
    app = IPKernelApp.instance(kernel_class=IPythonDuckdbKernel)
    app.initialize()
    app.kernel.shell.register_magic_function(sql, "line_cell", "sql")
    app.start()
    return


if __name__ == '__main__':
    main()
