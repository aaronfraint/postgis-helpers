"""
Import the necessary ``Rich`` objects, and then
import this module as needed elsewhere.
"""
from rich.style import Style as RichStyle
from rich.console import Console as RichConsole
from rich.progress import Progress as RichProgress
from rich.syntax import Syntax as RichSyntax

_console = RichConsole()
