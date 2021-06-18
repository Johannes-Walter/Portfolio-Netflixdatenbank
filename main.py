# -*- coding: utf-8 -*-
import csv

data = []

# In UTF-8 öffnen, weil Sonderzeichen
with open("netflix_titles.csv", encoding="UTF-8") as file:
    reader = csv.reader(file)
    for row in reader:
        # Zeilenumbrüche Löschen, um aufzuräumen
        for iter, item in enumerate(row):
            row[iter] = item.strip("\n")
        data.append(row)



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