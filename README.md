# ipykernel-duckdb

A wrapper of IPykernel that is aware of a duckdb connection and provides
* Autocompletion of table and column names
* Magics `%sql`, `%%sql` for querying the database

## Installation (from source)
```
git clone https://github.com/eeroel/ipykernel-duckdb.git .
pip install .
```
This will install the Python module and configure a Jupyter kernel `ipykernel_duckdb`.

## Known issues
* Also in Visual Studio Code, to get the most of autocompletion, you may want to tweak the setting "jupyter.pythonCompletionTriggerCharacters".
In IPython and JupyterLab the completion is suggested when you press TAB, but in VSCode it's character-based. Adding space (" ") to the list of characters works,
but you will end up with a lot of suggestions.
