# -*- coding: utf-8 -*-
import csv
import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

st.sidebar.title("Auswahl der Variablen")
st.sidebar.selectbox("Variable A", ["A", "B", "C"])
st.sidebar.selectbox("Variable B", ["A", "B", "C"], index=1)
st.sidebar.slider("Anzahl ")

st.title("Willkommen auf unserem Dashboard")

user_input = st.text_input("1. In welchen Filmen hat Schauspieler X gespielt?")





"""
#TODO


- Datenbank
    - filtern
    - speichern
    - Suchmaske
    - Rückgabetyp (Pandas?)
    
- Kartendarstellung (Streamlit/eher Excel?)

- UI mit Streamlit/Flask

- Graphen (Interaktion mit UI)

- Objektorientierung prüfen

- Video aufnehmen/schneiden
    - 4MAT
    
    
    
DV:
- Dashboard mit matplotlib/streamlit/excel


"""