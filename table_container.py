from re import S
from typing import Callable, Iterable, List, Optional, Sequence, Union

from prompt_toolkit.application.current import get_app
from prompt_toolkit.filters import Condition
from prompt_toolkit.formatted_text.base import OneStyleAndTextTuple, StyleAndTextTuples
from prompt_toolkit.key_binding.key_bindings import KeyBindings, KeyBindingsBase
from prompt_toolkit.key_binding.key_processor import KeyPressEvent
from prompt_toolkit.keys import Keys
from prompt_toolkit.layout.containers import (
    AnyContainer,
    Container,
    HSplit,
    Window,
)
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.mouse_events import MouseEvent, MouseEventType
from prompt_toolkit.utils import get_cwidth
from prompt_toolkit.widgets import Shadow

from prompt_toolkit.widgets.base import Border


E = KeyPressEvent


class TableContainer:
    """
    Goals for this:
    1. MVP: neatly laid out columns, table can be scrolled, and only visible data is fetched from source
    2. Sortable columns, but that will be mainly necessary if there's some query functionality to go with it...

    :param floats: List of extra Float objects to display.
    :param menu_items: List of `MenuItem` objects.
    """
    header=None
    table = None

    def __init__(
        self,
        body: AnyContainer,
        header: List[str],
        table: Iterable,
        key_bindings: Optional[KeyBindingsBase] = None,
    ) -> None:
        self.header = header
        self.table = list(table)

        self.body = body

        # Key bindings.
        kb = KeyBindings()

        # Controls.
        self.header_control = FormattedTextControl(
            self._show_header, key_bindings=kb, focusable=True, show_cursor=False
        )

        self.table_control = FormattedTextControl(
            self._show_table, key_bindings=kb, focusable=True, show_cursor=False
        )

        self.header_box = Window(content=self.header_control)
        self.rows_box = Window(content=self.table_control)

        @Condition
        def has_focus() -> bool:
            return get_app().layout.current_window == self.header_box

        self.container = HSplit(
            [
                self.header_box,
                self.rows_box,
            ],
            key_bindings=key_bindings,
        )

    def _show_header(self):
        return '     '.join(self.header)

    def _show_table(self):
        result = []
        for row in self.table:
            result += ['     '.join(row)]
        return '\n'.join(result)


    def __pt_container__(self) -> Container:
        return self.container
