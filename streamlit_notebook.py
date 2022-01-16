"""
Streamlit app: a simple database explorer on top of duckdb on top of CSV files
"""
import streamlit as st
from duckdb_shell import init_db

db, tables = init_db()
selection = st.sidebar.radio('Select table:', tables.__dict__.keys())
rows = tables.__dict__[selection].df()
st.dataframe(rows)

query = st.text_area("Query database:")
st.dataframe(db.query(query).df())