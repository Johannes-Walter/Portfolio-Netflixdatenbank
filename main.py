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
    

WAS SOLL MAN DARSTELLEN KÖNNEN?
WAS DAVON BRINGT ETWAS?
- In wie vielen Filmen hat der Schauspieler mitgemacht?
- Wie viele Filme pro Land gibt es? (Heatmap)
- Wie viele Filme pro Einwohner?
- Wie viele Saisons hat eine Serie im Schnitt?
- Wie lang sind Filme aus USA im Schnitt im Gegensatz zu Indien? (Boxplot mit min,avg,max)
- Wie viel Schauspieler gibt es im Durchschnitt in einer Serie?

    
DV:
- Dashboard mit matplotlib/streamlit/excel


"""