import IPython
from IPython import get_ipython
import ipykernel

import duckdb
import pandas as pd

from pathlib import Path
import glob
import sys
from functools import partial
from types import SimpleNamespace


from ipykernel.ipkernel import IPythonKernel

# 
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import Application
from prompt_toolkit.layout.layout import Layout

from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.layout.containers import VSplit, HSplit, Window, FloatContainer, Float
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.layout.layout import Layout
from prompt_toolkit.widgets import MenuContainer, MenuItem


from table_control import TableControl


class Kernel(IPythonKernel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def do_complete(self, code, cursor_pos):
        """
        TODO: jedi completion + additional sql when inside strings
        """

        return {
            # status should be 'ok' unless an exception was raised during the request,
            # in which case it should be 'error', along with the usual error message content
            # in other messages.
            'status': 'ok',

            # The list of all matches to the completion request, such as
            # ['a.isalnum', 'a.isalpha'] for the above example.
            'matches': [],

            # The range of text that should be replaced by the above matches when a completion is accepted.
            # typically cursor_end is the same as cursor_pos in the request.
            'cursor_start' : cursor_pos,
            'cursor_end' : cursor_pos,

            # Information that frontend plugins might use for extra display information about completions.
            'metadata' : {},
            }


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


kb = KeyBindings()

@kb.add('c-q')
def exit_(event):
    """
    Pressing Ctrl-Q will exit the user interface.

    Setting a return value means: quit the event loop that drives the user
    interface and return this value from the `Application.run()` call.
    """
    event.app.exit()


def show_table(table_name, db, component):
    pd.set_option("max_rows", None)
    df=db.view(table_name).df()

    component.header = list(df.columns)
    component.table = (list(x[1]) for x in df.astype(str).iterrows())
    return
        

def main():
    """
    Launch a Data Science IPython kernel with
    - duckdb database with views to CSV files, and the db queryable in the kernel
    - A database explorer terminal UI
    - TODO: Python and sql autocompletion where appropriate
    - TODO?: SQL magic to simplify `db.query(...` calls?

    The database explorer could also be used by itself (but kernel handles most interaction for now)
    """
    ## db and tables will be available in the kernel
    db, tables = init_db()

    # dataframe with tables and column names
    col_table=db.query("select t.table_name, c.column_name from INFORMATION_SCHEMA.tables t join INFORMATION_SCHEMA.columns c on t.table_name=c.table_name").df()

    main_window = Window()
    table_view = TableControl(header = ['foo','bar'], table = (x for x in [['1','2'],['3','4']]))

    ## construct database explorer ui

    # TODO: how to refresh these when files are updated?
    menu_items=(
        [ MenuItem(
            text="Tables",
            children=[
                MenuItem(text=x, handler=partial(show_table, x, db=db, component=table_view), children = [ MenuItem(text=y) for y in col_table.loc[lambda tbl: tbl.table_name==x].column_name.unique()])
                for x in col_table.table_name.unique()
        ])]
    )

    # TODO: create a custom MenuContainer with
    # - VSplit instead of floats
    # - Only one submenu thingy
    root_container = VSplit([
        MenuContainer(body=main_window, menu_items=menu_items),
        Window(table_view)
    ])

    layout = Layout(root_container)
    tui = Application(key_bindings=kb, layout=layout, full_screen=True)

    # create kernel with asyncio ui support
    from ipykernel.kernelapp import IPKernelApp
    app = IPKernelApp.instance(kernel_class=Kernel)
    app.initialize(["-f","foo.json","--gui", "asyncio"])
    
    # set up the relevant variables to pass to the embedded kernel; imitating ipykernel.embed here
    f = sys._getframe(0)
    global_ns = f.f_globals
    module = sys.modules[global_ns['__name__']]
    app.kernel.user_module = module
    app.kernel.user_ns = f.f_locals
    app.shell.set_completer_frame()

    # hack from https://github.com/ipython/ipykernel/issues/319#issuecomment-661951992
    # otherwais complains about ioloop missing
    ipykernel.kernelbase.Kernel.start(app.kernel)

    # looks like this will start the kernel event loop also
    tui.run()

    return



if __name__ == '__main__':
    main()