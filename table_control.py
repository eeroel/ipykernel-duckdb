from prompt_toolkit.layout.controls import UIControl, UIContent
from typing import Optional, Callable, Hashable, List, Iterable



from prompt_toolkit.application.current import get_app
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.cache import SimpleCache
from prompt_toolkit.data_structures import Point
from prompt_toolkit.document import Document
from prompt_toolkit.filters import FilterOrBool, to_filter
from prompt_toolkit.formatted_text import (
    AnyFormattedText,
    StyleAndTextTuples,
    to_formatted_text,
)
from prompt_toolkit.formatted_text.utils import (
    fragment_list_to_text,
    split_lines,
)

from prompt_toolkit.mouse_events import MouseEvent, MouseEventType
from prompt_toolkit.utils import get_cwidth



class TableControl(UIControl):
    """
    Modified from FormattedTextControl
    """
    def __init__(
        self,
        header: List[str],
        table: Iterable,
        style: str = "",
        focusable: FilterOrBool = False,
        key_bindings= None,
        show_cursor: bool = True,
        modal: bool = False,
        get_cursor_position: Optional[Callable[[], Optional[Point]]] = None,
    ) -> None:
        self.header = header
        self.table = list(table)

        self.style = style
        self.focusable = to_filter(focusable)

        # Key bindings.
        self.key_bindings = key_bindings
        self.show_cursor = show_cursor
        self.modal = modal
        self.get_cursor_position = get_cursor_position

        #: Cache for the content.
        self._content_cache: SimpleCache[Hashable, UIContent] = SimpleCache(maxsize=18)
        self._fragment_cache: SimpleCache[int, StyleAndTextTuples] = SimpleCache(
            maxsize=1
        )
        # Only cache one fragment list. We don't need the previous item.

        # Render info for the mouse support.
        self._fragments: Optional[StyleAndTextTuples] = None

    def render_rows(self, tbl, style):
        tbl_text = '\n'.join(['    '.join(x) for x in tbl])
        return to_formatted_text(tbl_text, style)

    def reset(self) -> None:
        self._fragments = None

    def is_focusable(self) -> bool:
        return self.focusable()

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.text)

    def _get_formatted_text_cached(self) -> StyleAndTextTuples:
        """
        Get fragments, but only retrieve fragments once during one render run.
        (This function is called several times during one rendering, because
        we also need those for calculating the dimensions.)
        """
        return self._fragment_cache.get(
            get_app().render_counter, lambda: self.render_rows(self.table, self.style)
        )

    def preferred_width(self, max_available_width: int) -> int:
        """
        Return the preferred width for this control.
        That is the width of the longest line.
        """
        text = fragment_list_to_text(self._get_formatted_text_cached())
        line_lengths = [get_cwidth(l) for l in text.split("\n")]
        return max(line_lengths)

    def preferred_height(
        self,
        width: int,
        max_available_height: int,
        wrap_lines: bool,
        get_line_prefix,
    ) -> Optional[int]:
        """
        Return the preferred height for this control.
        """
        content = self.create_content(width, None)
        if wrap_lines:
            height = 0
            for i in range(content.line_count):
                height += content.get_height_for_line(i, width, get_line_prefix)
                if height >= max_available_height:
                    return max_available_height
            return height
        else:
            return content.line_count

    def create_content(self, width: int, height: Optional[int]) -> UIContent:
        # Get fragments
        fragments_with_mouse_handlers = self._get_formatted_text_cached()
        fragment_lines_with_mouse_handlers = list(
            split_lines(fragments_with_mouse_handlers)
        )

        # Strip mouse handlers from fragments.
        fragment_lines: List[StyleAndTextTuples] = [
            [(item[0], item[1]) for item in line]
            for line in fragment_lines_with_mouse_handlers
        ]


        # Keep track of the fragments with mouse handler, for later use in
        # `mouse_handler`.
        self._fragments = fragments_with_mouse_handlers

        # If there is a `[SetCursorPosition]` in the fragment list, set the
        # cursor position here.
        def get_cursor_position(
            fragment: str = "[SetCursorPosition]",
        ) -> Optional[Point]:
            for y, line in enumerate(fragment_lines):
                x = 0
                for style_str, text, *_ in line:
                    if fragment in style_str:
                        return Point(x=x, y=y)
                    x += len(text)
            return None

        # If there is a `[SetMenuPosition]`, set the menu over here.
        def get_menu_position() -> Optional[Point]:
            return get_cursor_position("[SetMenuPosition]")

        cursor_position = (self.get_cursor_position or get_cursor_position)()

        # Create content, or take it from the cache.
        key = (tuple(fragments_with_mouse_handlers), width, cursor_position)

        fragment_lines_with_header = [self.render_rows([self.header], self.style)]+fragment_lines

        def get_content() -> UIContent:
            return UIContent(
                get_line=lambda i: fragment_lines_with_header[i],
                line_count=len(fragment_lines),
                show_cursor=self.show_cursor,
                cursor_position=cursor_position,
                menu_position=get_menu_position(),
            )

        return self._content_cache.get(key, get_content)

    def mouse_handler(self, mouse_event: MouseEvent):
        """
        Handle mouse events.

        (When the fragment list contained mouse handlers and the user clicked on
        on any of these, the matching handler is called. This handler can still
        return `NotImplemented` in case we want the
        :class:`~prompt_toolkit.layout.Window` to handle this particular
        event.)
        """
        if self._fragments:
            # Read the generator.
            fragments_for_line = list(split_lines(self._fragments))

            try:
                fragments = fragments_for_line[mouse_event.position.y]
            except IndexError:
                return NotImplemented
            else:
                # Find position in the fragment list.
                xpos = mouse_event.position.x

                # Find mouse handler for this character.
                count = 0
                for item in fragments:
                    count += len(item[1])
                    if count > xpos:
                        if len(item) >= 3:
                            # Handler found. Call it.
                            # (Handler can return NotImplemented, so return
                            # that result.)
                            handler = item[2]  # type: ignore
                            return handler(mouse_event)
                        else:
                            break

        # Otherwise, don't handle here.
        return NotImplemented

    def is_modal(self) -> bool:
        return self.modal

    def get_key_bindings(self):
        return self.key_bindings
