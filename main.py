# -*- coding: utf-8 -*-
import csv

import folium as folium
import streamlit as st
from streamlit_folium import folium_static
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import random
import folium
import reader

st.set_page_config(layout="wide")
st.title("Netflix Dashboard")


con = reader.db_connector()
con.reset_database()
con.import_file("netflix_titles.csv")
con.con.close()

fragen=["1. In welchen Filmen hat Schauspieler X gespielt?",
        "2. In welchen Filmen war X Regisseur?",
        "3. Welche Filme sind in der Kategorie X?",
        "4. Welche Filme sind zwischen Datum X und Y erschienen?"]

y = st.selectbox("Wähle aus", [fragen[0], fragen[1], fragen[2], fragen[3]])
if y == fragen[0]:
    z = st.text_input("Name des Schauspielers eingeben")
    if z:
        st.subheader(f"{z} hat in folgenden Filmen mitgespielt:")
        movies = ["King Kong", "Terminator 2", "UwU"]
        st.write(movies)
elif y == fragen[1]:
    z = st.text_input("Name des Regisseurs eingeben")
    if z:
        st.subheader(f"{z} hat bei folgenden Filmen Regie geführt:")
        directors = random.sample(range(0, 10000), 4000)
        st.write(directors)
elif y == fragen[2]:
    z = st.text_input("Bitte wählen Sie ein Genre aus (z.B. Action, Drama, etc...)")
    if z:
        st.subheader(f"Folgende Filme gehören zu dem Genre {z}:")
        directors = random.sample(range(0, 10000), 4000)
        st.write(directors)
elif y == fragen[3]:

    z = st.slider("Bitte wählen Sie den Zeitraum aus:", 1925, 2021, (1970, 1980))

    if z:
        movies = random.sample(range(0, 10000), 4000)
        movies = movies[z[0]:z[1]]
        st.write(movies)

# geodata-source: https://geojson-maps.ash.ms/
json1 = open(r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json', 'r')
m = folium.Map(location=[47.54, 7.58], tiles="CartoDB position", name="Light Map",
               zoom_start=5, attr="Data Attribution")
d = {'state_code': ["12", "134", "5"], 'Country': [1, 2, 3], 'Movies': ["2", "3", "4"], "Actors": ["1", "2", "19"]}
df = pd.DataFrame(data=d)
df = df.astype({col: int for col in df})
choice = ["Country", "Movies", "Actors"]
choice_selected = st.selectbox("Select choice", choice)
folium.Choropleth(
    geo_path=json1,
    geo_data=r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json',
    name="choropleth",
    data=df,
    columns=["state_code", choice_selected],
    key_on="feature.properties.name",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.1,
    legend_name=choice_selected
).add_to(m)
folium.features.GeoJson(r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json',
                        name="Countries", popup=folium.features.GeoJsonPopup(fields=["name"])).add_to(m)

folium_static(m, width=1600, height=950)


