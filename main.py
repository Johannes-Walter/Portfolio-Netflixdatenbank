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
