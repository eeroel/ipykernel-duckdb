import IPython
from IPython import get_ipython
import ipykernel

import duckdb
import pandas as pd

from pathlib import Path
import glob
import sys

from types import SimpleNamespace

from ipykernel.ipkernel import IPythonKernel

# 
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.layout.containers import VSplit, Window
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.layout.layout import Layout

from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit import Application
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.layout.containers import VSplit, Window
from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl
from prompt_toolkit.layout.layout import Layout

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


        

def main():
    ## Initialize state that will be available from within the kernel
    db, tables = init_db()
    table_list = ", ".join(list([x for x in tables.__dict__.keys()]))



    ## create UI; TODO: database explorer
    root_container = VSplit([
    # One window that holds the BufferControl with the default buffer on
    # the left.
    Window(content=BufferControl(buffer=Buffer())),
    Window(width=1, char='|'),
    Window(content=FormattedTextControl(text=table_list)),
    ])
    layout = Layout(root_container)
    tui = Application(key_bindings=kb, layout=layout, full_screen=True)




    # create kernel with asyncio ui support
    from ipykernel.kernelapp import IPKernelApp
    app = IPKernelApp.instance(kernel_class=Kernel)
    app.initialize(["-f","foo.json","--gui", "asyncio"])
    
    # imitating ipykernel.embed
    f = sys._getframe(0)
    global_ns = f.f_globals
    module = sys.modules[global_ns['__name__']]
    app.kernel.user_module = module
    app.kernel.user_ns = f.f_locals
    app.shell.set_completer_frame()

    # hack from https://github.com/ipython/ipykernel/issues/319#issuecomment-661951992
    ipykernel.kernelbase.Kernel.start(app.kernel)

    # looks like this will start the kernel event loop also
    tui.run()

    return



if __name__ == '__main__':
    main()