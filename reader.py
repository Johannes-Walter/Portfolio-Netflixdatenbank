# -*- coding: utf-8 -*-

import pandas as pd
import sqlite3




class db_connector:

    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)

    def __del__(self):
        self.con.close()

    def read_file(self, file_path: str):
        cur = self.con.cursor()

        # In UTF-8 öffnen, weil Sonderzeichen
        data = pd.read_csv(file_path, encoding="UTF-8")

        # Zeilenumbrüche Löschen, um aufzuräumen
        data = data.replace("\\n", " ", regex=True)

        cur.executemany(
            """INSERT INTO [shows] (show_id, type, title, date_added, release_year, rating, duration, description)
            values(?, ?, ?, ?, ?, ?, ?, ?)""",
            data.loc[:, ["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description"]].values.tolist())

        self.con.commit()
        return

    def reset_database(self):
        cur = self.con.cursor()
        cur.execute("DROP TABLE IF EXISTS [shows]")
        cur.execute("""CREATE TABLE [shows] (
            show_id STRING PRIMARY KEY,
            type STRNG,
            title STRING,
            date_added STRING,
            release_year INT,
            rating STRING,
            duration STRING,
            description STRING)""")
        self.con.commit()


if __name__ == "__main__":
    #test = data.loc[:,["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description"]].values.tolist()
    con = db_connector("database")
    con.reset_database()
    con.read_file("netflix_titles.csv")
    con.con.close()
