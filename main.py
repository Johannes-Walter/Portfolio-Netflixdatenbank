# -*- coding: utf-8 -*-
import csv
import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import random
st.sidebar.title("Auswahl der Variablen")
st.sidebar.selectbox("Variable A", ["A", "B", "C"])
st.sidebar.selectbox("Variable B", ["A", "B", "C"], index=1)

st.title("Willkommen auf unserem Dashboard")

user_input1 = st.text_input("1. In welchen Filmen hat Schauspieler X gespielt? (Name des Schauspielers eingeben)")

if user_input1:
    st.subheader(f"{user_input1} hat in folgenden Filmen mitgespielt:")
    with st.spinner(text="This may take a moment..."):
        movies = ["King Kong", "Terminator 2", "UwU"]
    st.write(movies)


user_input2 = st.text_input("2. In welchen Filmen war X Regisseur? (Name des Regisseurs eingeben)")

if user_input2:
    st.subheader(f"{user_input2} hat bei folgenden Filmen Regie geführt:")
    with st.spinner(text="This may take a moment..."):
        directors = random.sample(range(0, 10000), 4000)
    st.write(directors)

user_input3 = st.text_input("3. Welche Filme sind in der Kategorie X (z.B. Action, Drama, etc...) ?")

if user_input3:
    st.subheader(f"Folgende Filme sind in der Katerogie {user_input3}")
    with st.spinner(text="This may take a moment..."):
        categories = ["King Kong", "Terminator 2", "UwU"]
    st.write(categories)

st.text("4. Welche Filme sind zwischen Datum X und Y erschienen?")

x = st.slider("Zeitraum:", 1925, 2021, (1925, 2021))

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
