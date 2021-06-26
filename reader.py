"""
Matrikelnummern der Gruppe:
    4869673
    5058121
    2177693
"""
import pandas as pd
import numpy as np
import sqlite3


class db_connector:
    SHOW_COLUMNS = ["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description"]
    DIRECTOR_COLUMNS = ["id", "name"]
    COUNTRY_COLUMNS = ["id", "name"]
    LISTING_COLUMNS = ["id", "name"]
    CAST_COLUMNS = ["id", "name"]

    def __init__(self, db_path = "database"):
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
            data.loc[:, self.SHOW_COLUMNS].values.tolist())

        for iter, row in data.iterrows():
            if row["director"] != "":
                for director in str.split(row["director"], ","):
                    director = director.strip()
                    cur.execute("INSERT OR IGNORE INTO [director] (name) values(?)",
                                (director,))
                    cur.execute("SELECT id FROM [director] WHERE name = ?",
                                (director,))
                    cur.execute("""
                        INSERT INTO [show_director] (show_id, director_id)
                        VALUES (?, ?)""",
                        (row["show_id"], cur.fetchone()[0]))

            if row["cast"] != "":
                for director in str.split(row["cast"], ","):
                    pass

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
            id INTEGER PRIMARY KEY,
            name STRING UNIQUE
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_director]")
        cur.execute("""CREATE TABLE [show_director] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            director_id STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES show (show_id),
            FOREIGN KEY (director_id) REFERENCES director (id)
            )""")

        cur.execute("DROP TABLE IF EXISTS [cast]")
        cur.execute("""CREATE TABLE [cast] (
            id INTEGER PRIMARY KEY,
            name STRING UNIQUE
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_cast]")
        cur.execute("""CREATE TABLE [show_cast] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            cast_id STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (cast_id) REFERENCES [cast] (id)
            )""")

        cur.execute("DROP TABLE IF EXISTS [country]")
        cur.execute("""CREATE TABLE [country](
            id INTEGER PRIMARY KEY,
            name STRING UNIQUE
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_country]")
        cur.execute("""CREATE TABLE [show_country] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            country_id STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (country_id) REFERENCES [country] (id)
            )""")

        cur.execute("DROP TABLE IF EXISTS [listing]")
        cur.execute("""CREATE TABLE [listing](
            id INTEGER PRIMARY KEY,
            name STRING UNIQUE
            )""")

        cur.execute("DROP TABLE IF EXISTS [show_listing]")
        cur.execute("""CREATE TABLE [show_listing] (
            id INTEGER PRIMARY KEY,
            show_id STRING NOT NULL,
            listing_id STRING NOT NULL,
            FOREIGN KEY (show_id) REFERENCES [show] (show_id),
            FOREIGN KEY (listing_id) REFERENCES [listing] (id)
            )""")

        self.con.commit()


    def get_all_cast(self):
        pass

    def get_all_listings(self):
        pass

    def get_shows_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT s.{", s.".join(self.SHOW_COLUMNS)}
                    FROM [shows] as s, [director] as d, [show_director] as sd
                    WHERE s.show_id = sd.show_id
                    AND d.id = sd.director_id
                    AND d.name = ?
                    """,
                    (director,))
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

    def get_directors_by_show(self, show: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT d.name
                    FROM [shows] as s, [director] as d, [show_director] as sd
                    WHERE s.show_id = sd.show_id
                    AND d.id = sd.director_id
                    AND s.title = ?
                    """,
                    (show,))
        return pd.DataFrame(cur.fetchall(), columns=("director",))

    def get_all_directors(self):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT d.name
                    FROM [director] as d
                    """)
        return pd.DataFrame(cur.fetchall(), columns=("director",))

    def get_all_shows(self):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT {', '.join(self.SHOW_COLUMNS)}
                    FROM [shows]
                    """)
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

    def get_shows_by_cast(self, cast: str):
        pass

    def get_cast_by_show(self, show: str):
        pass

    def get_shows_by_country(self, country: str):
        pass

    def get_countries_by_show(self, show: str):
        pass

    def get_shows_per_country(self):
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
    con = db_connector()
    con.reset_database()
    con.import_file("netflix_titles.csv")
    data = con.get_shows_by_director("George Ford")
    con.con.close()
