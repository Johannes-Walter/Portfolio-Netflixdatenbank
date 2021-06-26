import pandas as pd
import numpy as np
import sqlite3


class db_connector:

    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)

    def __del__(self):
        self.con.close()

    def import_file(self, file_path: str):
        cur = self.con.cursor()

        # In UTF-8 öffnen, weil Sonderzeichen
        data = pd.read_csv(file_path, encoding="UTF-8", dtype={"director":str})

        # Zeilenumbrüche Löschen, um aufzuräumen
        data = data.replace("\\n", " ", regex=True)
        data = data.replace(np.nan, "", regex=True)


        cur.executemany("""
            INSERT INTO [shows] (show_id, type, title, date_added, release_year, rating, duration, description)
            values(?, ?, ?, ?, ?, ?, ?, ?)""",
            data.loc[:, ["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description"]].values.tolist())

        for iter, row in data.iterrows():
            if row["director"] != "":
                for director in str.split(row["director"], ","):
                    cur.execute("INSERT OR IGNORE INTO [director] (name) values(?)",
                                [director])
                    cur.execute("""
                        INSERT INTO [show_director] (show_id, director)
                        VALUES (?, ?)""",
                        (row["show_id"], director))

            

        self.con.commit()
        return

    def reset_database(self):
        cur = self.con.cursor()
        cur.execute("DROP TABLE IF EXISTS [shows]")
        cur.execute("""CREATE TABLE [shows] (
            show_id STRING PRIMARY KEY,
            type STRING,
            title STRING,
            date_added STRING,
            release_year INTEGER,
            rating STRING,
            duration STRING,
            description STRING)""")

        cur.execute("DROP TABLE IF EXISTS [director]")
        cur.execute("""CREATE TABLE [director] (
            name STRING PRIMARY KEY
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_director]")
        cur.execute("""CREATE TABLE [show_director] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            director STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES show (show_id),
            FOREIGN KEY (director) REFERENCES director (name)
            )""")

        cur.execute("DROP TABLE IF EXISTS [cast]")
        cur.execute("""CREATE TABLE [cast] (
            name STRING PRIMARY KEY
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_cast]")
        cur.execute("""CREATE TABLE [show_cast] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            cast STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (cast) REFERENCES [cast] (name)
            )""")

        cur.execute("DROP TABLE IF EXISTS [country]")
        cur.execute("""CREATE TABLE [country](
            name STRING PRIMARY KEY
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_country]")
        cur.execute("""CREATE TABLE [show_country] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            country STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (country) REFERENCES [country] (name)
            )""")

        cur.execute("DROP TABLE IF EXISTS [listing]")
        cur.execute("""CREATE TABLE [listing](
            name STRING PRIMARY KEY
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_listing]")
        cur.execute("""CREATE TABLE [show_listing] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            listing STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (listing) REFERENCES [listing] (name)
            )""")

        self.con.commit()

    def get_shows_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute("SELECT ")
        pass

    def get_directors_by_show(self, show: str):
        pass

    def get_all_directors(self):
        pass

    def get_all_shows(self):
        pass

    def get_all_cast(self):
        pass

    def get_all_listings(self):
        pass

    def get_shows_by_cast(self, cast: str):
        pass

    def get_cast_by_show(self, show: str):
        pass

    def get_shows_by_country(self, country: str):
        pass

    def get_countries_by_show(self, show: str):
        pass

    def get_shows_by_listing(self, listing: str):
        pass

    def get_listings_by_show(self, show: str):
        pass

    def get_listings_by_director(self, director: str):
        pass

    def get_listings_by_cast(self, cast: str):
        pass

    def get_cast_by_director(self, director: str):
        pass

    def get_directors_by_cast(self, director: str):
        pass

if __name__ == "__main__":
    con = db_connector("database")
    con.reset_database()
    con.import_file("netflix_titles.csv")
    con.con.close()
