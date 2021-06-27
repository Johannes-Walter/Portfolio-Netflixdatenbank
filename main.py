# -*- coding: utf-8 -*-
import streamlit as st
from streamlit_folium import folium_static
import pandas as pd
import matplotlib.pyplot as plt
import random
import folium
import reader
import plotly.express as px


def hard_reset():
    con.reset_database()
    con.import_file(r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\netflix_titles.csv')


def showmap():
    # geodata-source: https://geojson-maps.ash.ms/
    json1 = open(r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json', 'r')
    # Create a Map instance
    m = folium.Map(location=[47.54, 7.58], tiles="none", name="Light Map",
                   zoom_start=2, attr="SpinalMap", min_zoom="2", prefer_canvas="true", max_zoom="10")

    choice = ["count"]

    # choice_selected = st.selectbox("Select choice", choice)
    # add chloropleth layer
    folium.Choropleth(
        geo_path=json1,
        geo_data=r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json',
        name="choropleth",
        data=con.get_shows_per_country(),
        columns=["country", choice[0]],
        key_on="feature.properties.name",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.1,
        legend_name=choice[0]
    ).add_to(m)
    folium.features.GeoJson(r'C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\custom_geo.json',
                            name="Countries", smooth_factor=2.0,
                            popup=folium.features.GeoJsonPopup(fields=["name"])).add_to(m)
    folium_static(m, width=1755, height=950 * 0.75)


con = reader.db_connector()
con.reset_database()
con.import_file(r"C:\Users\darke\PycharmProjects\Portfolio-Netflixdatenbank\netflix_titles.csv")

st.set_page_config(layout="wide")

st.button("Reset Database", hard_reset())

hide_streamlit_style = """
            <style>
            footer {visibility: hidden;}
            #MainMenu {visibility: shown;}
            footer:after {
	        content:'Eine Kooperation der Matrikelnummern: 2177693, 4869673, 5058121'; 
	        visibility: visible;
	        display: block;
	        position: relative;
	        color: red;
	        padding: 50px;
	        top: 1px;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.title("Netflix Dashboard")

fragen = ["1. In welchen Filmen hat Schauspieler X gespielt?",
          "2. In welchen Filmen war X Regisseur?",
          "3. Welche Filme sind in der Kategorie X?",
          "4. Welche Filme sind zwischen Datum X und Y erschienen?",
          "5. Wie viele Filme gibt es pro Land?",
          "6. Weitere Visualisierungen"]

y = st.selectbox("Wähle aus", [i for i in fragen])

if y == fragen[0]:

    z = st.text_input("Name des Schauspielers eingeben")
    if z:
        st.subheader(f"{z} hat in folgenden Filmen mitgespielt:")
        cast = con.get_shows_by_cast(z)[["title", "release_year"]]
        cast.index += 1
        st.write(cast)

elif y == fragen[1]:

    z = st.text_input("Name des Regisseurs eingeben")
    if z:
        st.subheader(f"{z} hat bei folgenden Filmen Regie geführt:")
        directors = con.get_shows_by_director(z)["title"]
        directors.index += 1
        st.write(directors)

elif y == fragen[2]:

    z = st.text_input("Bitte wählen Sie ein Genre aus (z.B. Action, Drama, etc...)")
    if z:
        st.subheader(f"Folgende Filme gehören zu dem Genre {z}:")
        genre = con.get_shows_by_listing(z)["title"]
        st.write(genre)

elif y == fragen[3]:

    z = st.slider("Bitte wählen Sie den Zeitraum aus:", int(min(con.get_all_shows()["release_year"].values)),
                  int(max(con.get_all_shows()["release_year"].values)), (1970, 1980))

    if z:
        movies = con.get_all_shows()[["title", "release_year"]]
        movies = movies[movies['release_year'].between(z[0], z[1])]
        st.write(movies)

elif y == fragen[4]:
    showmap()
    movies_per_country = con.get_shows_per_country()
    st.write(movies_per_country.drop(index=0))

elif y == fragen[5]:

    selections = ["Anzahl Serien vs Anzahl Filme",
                  "Anzahl der Veröffentlichungen von Filmen/Serien pro Release Jahr",
                  "Schauspieler mit den meisten Filmen/Serien"]

    selection = st.selectbox("", [i for i in selections])

    if selection == selections[0]:

        pie_col, x = st.beta_columns(2)

        fig = px.pie(con.get_type_count(), values="count", names="type",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        pie_col.write(fig)

    elif selection == selections[1]:

        st.sidebar.header("Filteroptionen")
        bar_col, x = st.beta_columns(2)

        slider = st.sidebar.slider("Bitte wählen Sie den Zeitraum aus:",
                                   int(min(con.get_all_shows()["release_year"].values)),
                                   int(max(con.get_all_shows()["release_year"].values)), (1970, 1980))

        if slider:
            years = con.get_type_count_per_year()
            years = years[years['release_year'].between(slider[0], slider[1])]
            fig = px.bar(years, x='release_year', y='count', labels={"release_year": "Jahr der Veröffentlichung",
                                                                     "count": "Anzahl der Veröffentlichungen"})
            bar_col.write(fig)

    elif selection == selections[2]:

        pie_col, x = st.beta_columns(2)

        fig = px.pie(con.get_type_count(), values="count", names="type",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        pie_col.write(fig)
