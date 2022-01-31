import pytest
from ipykernel_duckdb import get_sql_matches

tables_and_columns = lambda: [
    ('spells', 'abra cadabra'),
    ('spells', 'foo'),
    ('spells', 'id'),
    ('books', 'spell_id'),
    ('books', 'name'),
]

@pytest.mark.skip("not fixed yet")
def test_col_with_space():
    code = 'select a'
    assert 'abra cadabra' in get_sql_matches(tables_and_columns(), code, len(code))

def test_simple_col():
    code = 'select f_ from spells'
    assert 'foo' in get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

def test_table_prefix():
    code = 'select spells.f_ from spells'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

    assert all([x.startswith('spells.') for x in matches])
    assert 'spells.foo' in matches

def test_alias_prefix():
    code = 'select s.f_ from spells s'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

    assert all([x.startswith('s.') for x in matches])
    assert 's.foo' in get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

def test_no_prompt_cols():
    code = 'select _ from spells'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

    # spells is mentioned so we get its columns
    assert 'foo' in matches
    # books is mentioned so we should not get its columns
    assert 'spell_id' not in matches

def test_select_with_join():
    code = 'select _ from spells join books on spells.id = books.spell_id'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

    assert 'id' in matches
    assert 'spell_id' in matches

def test_join():
    code = 'select * from spells join books on spells.id = books._'
    assert 'books.spell_id' in get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

def test_alias_join():
    code = 'select * from spells s join books b on s.id = b._'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

    assert 'b.spell_id' in matches
    assert 'b.id' not in matches

def test_select_multi_alias():
    code = 'select _ from spells s join books b on s.id = b.spell_id'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]
    assert 'id' in matches
    assert 'name' in matches


def test_select_multi_alias_prefix():
    code = 'select b._ from spells s join books b on s.id = b.spell_id'
    matches = get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]
    
    assert all([x.startswith('b.') for x in matches])
    assert 'b.spell_id' in matches
    assert 'b.id' not in matches

def test_table_suggestion():
    code = 'select from _'
    assert 'spells' in get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]
    assert 'books' in get_sql_matches(tables_and_columns(), code.replace('_',''), code.find('_'))[0]

