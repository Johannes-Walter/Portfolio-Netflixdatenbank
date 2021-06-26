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

        cur.executemany(f"""
            INSERT INTO [shows] ({", ".join(self.SHOW_COLUMNS)})
            values(?, ?, ?, ?, ?, ?, ?, ?)""",
            data.loc[:, self.SHOW_COLUMNS].values.tolist())

        for iter, row in data.iterrows():
            self.__insert_relational_row(show_id=row["show_id"],
                                         data_name="director",
                                         data=row["director"])
            self.__insert_relational_row(show_id=row["show_id"],
                                         data_name="cast",
                                         data=row["cast"])
            self.__insert_relational_row(show_id=row["show_id"],
                                         data_name="country",
                                         data=row["country"])
            self.__insert_relational_row(show_id=row["show_id"],
                                         data_name="listing",
                                         data=row["listed_in"])

        self.con.commit()
        return

    def __insert_relational_row(self, show_id, data_name, data):
        cur = self.con.cursor()

        for value in str.split(data, ","):
            value = value.strip()
            # insert data in data-table
            cur.execute(f"""INSERT OR IGNORE INTO [{data_name}] (name)
                        values (?)""",
                        (value,))

            # Fetch data-id
            cur.execute(f"SELECT id FROM [{data_name}] WHERE name = ?",
                        (value,))

            cur.execute(f"""
                        INSERT INTO [show_{data_name}] (show_id, {data_name}_id)
                        VALUES (?, ?)""",
                        (show_id, cur.fetchone()[0]))

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

# Welche Schauspieler gibt es bei Netflix-Filmen?
    def get_all_cast(self):
        cur = self.con.cursor()
        cur.execute("""SELECT c.name
                    FROM [cast] as c
                    """)
        return pd.DataFrame(cur.fetchall(), columns=("cast",))

# Welche Kategorien gibt es auf Netflix?
    def get_all_listings(self):
        cur = self.con.cursor()
        cur.execute("""SELECT l.name
                    FROM [listing] as l
                    """)
        return pd.DataFrame(cur.fetchall(), columns=("listed_in",))

# Welche Regisseure gibt es bei Netflix-Filmen?
    def get_all_directors(self):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT d.name
                    FROM [director] as d
                    """)
        return pd.DataFrame(cur.fetchall(), columns=("director",))

# Welche Filme gibt es auf Netflix?
    def get_all_shows(self):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT {', '.join(self.SHOW_COLUMNS)}
                    FROM [shows]
                    """)
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

# In welchen Ländern wurden Filme produziert?
    def get_all_countries(self):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT c.name
                    FROM [country] as c
                    """)
        return pd.DataFrame(cur.fetchall(), columns=("country",))

# Welche Filme sind vom Regisseur X?
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

# In welchen Filmen hat Schauspieler X gespielt?
    def get_shows_by_cast(self, cast: str):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT s.{", s.".join(self.SHOW_COLUMNS)}
                    FROM [shows] as s, [cast] as c, [show_cast] as sc
                    WHERE s.show_id = sc.show_id
                    AND c.id = sc.cast_id
                    AND c.name = ?
                    """,
                    (cast,))
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

# Welche Filme sind in der Kategorie X?
    def get_shows_by_listing(self, listing: str):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT s.{", s.".join(self.SHOW_COLUMNS)}
                    FROM [shows] as s, [listing] as l, [show_listing] as sl
                    WHERE s.show_id = sl.show_id
                    AND l.id = sl.listing_id
                    AND l.name = ?
                    """,
                    (listing,))
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

# Welche Filme sind aus Land X?
    def get_shows_by_country(self, country: str):
        cur = self.con.cursor()
        cur.execute(f"""
                    SELECT s.{", s.".join(self.SHOW_COLUMNS)}
                    FROM [shows] as s, [country] as c, [show_country] as sc
                    WHERE s.show_id = sc.show_id
                    AND c.id = sc.country_id
                    AND c.name = ?
                    """,
                    (country,))
        return pd.DataFrame(cur.fetchall(), columns=self.SHOW_COLUMNS)

# 5. Wie viele Filme gibt es pro Land?
    def get_shows_per_country(self):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT c.name, COUNT(c.name)
                    FROM [country] as c, [show_country] as sc
                    WHERE c.id = sc.country_id
                    GROUP BY c.name
                    """)
        return pd.DataFrame(cur.fetchall(), columns=["country", "count"])

#  An welchen Orten wurde Film X gedreht?
    def get_countries_by_show(self, show: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT c.name
                    FROM [shows] as s, [country] as c, [show_country] as sc
                    WHERE s.show_id = sc.show_id
                    AND c.id = sc.country_id
                    AND s.title = ?
                    """,
                    (show,))
        return pd.DataFrame(cur.fetchall(), columns=("country",))

#1. In welchen Filmen war X ein Regisseur?
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

# In welchen Filmen war X ein Schauspieler?
    def get_cast_by_show(self, show: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT c.name
                    FROM [shows] as s, [cast] as c, [show_cast] as sc
                    WHERE s.show_id = sc.show_id
                    AND c.id = sc.cast_id
                    AND s.title = ?
                    """,
                    (show,))
        return pd.DataFrame(cur.fetchall(), columns=("cast",))

# In welchen Kategorien ist Film X?
    def get_listings_by_show(self, show: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT l.name
                    FROM [shows] as s, [listing] as l, [show_listing] as sl
                    WHERE s.show_id = sl.show_id
                    AND l.id = sl.listing_id
                    AND s.title = ?
                    """,
                    (show,))
        return pd.DataFrame(cur.fetchall(), columns=("listings",))

# In welchen Kategorien ist X ein Regisseur? 
    def get_listings_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT l.name
                    FROM [listing] as l, [show_listing] as sl,
                         [director] as d, [show_director] as sd
                    WHERE sl.show_id = sd.show_id
                    AND l.id = sl.listing_id
                    AND d.id = sd.director_id
                    AND d.name = ?
                    """,
                    (director,))
        return pd.DataFrame(cur.fetchall(), columns=("listing",))

# In welchen Kategorien ist X ein Schauspieler?
    def get_listings_by_cast(self, cast: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT l.name
                    FROM [listing] as l, [show_listing] as sl,
                         [cast] as c, [show_cast] as sc
                    WHERE sl.show_id = sc.show_id
                    AND l.id = sl.listing_id
                    AND c.id = sc.cast_id
                    AND c.name = ?
                    """,
                    (cast,))
        return pd.DataFrame(cur.fetchall(), columns=("listing",))

# Welcher Schauspieler spielte bei Filmen des Regisseures X mit?
    def get_cast_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT c.name
                    FROM [cast] as c, [show_cast] as sc,
                         [director] as d, [show_director] as sd
                    WHERE sc.show_id = sd.show_id
                    AND c.id = sc.cast_id
                    AND d.id = sd.director_id
                    AND d.name = ?
                    """,
                    (director,))
        return pd.DataFrame(cur.fetchall(), columns=("cast",))

# Wer war Regie bei Filmen, bei denen Schauspieler X mitspielte?
    def get_directors_by_cast(self, cast: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT d.name
                    FROM [cast] as c, [show_cast] as sc,
                         [director] as d, [show_director] as sd
                    WHERE sc.show_id = sd.show_id
                    AND c.id = sc.cast_id
                    AND d.id = sd.director_id
                    AND c.name = ?
                    """,
                    (cast,))
        return pd.DataFrame(cur.fetchall(), columns=("director",))

# In welchen Ländern war X Regisseur?
    def get_country_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT c.name
                    FROM [country] as c, [show_country] as sc,
                         [director] as d, [show_director] as sd
                    WHERE sc.show_id = sd.show_id
                    AND c.id = sc.country_id
                    AND d.id = sd.director_id
                    AND d.name = ?
                    """,
                    (director,))
        return pd.DataFrame(cur.fetchall(), columns=("country",))

# Welche Regisseure waren in Land X?
    def get_director_by_country(self, country: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT d.name
                    FROM [country] as c, [show_country] as sc,
                         [director] as d, [show_director] as sd
                    WHERE sc.show_id = sd.show_id
                    AND c.id = sc.country_id
                    AND d.id = sd.director_id
                    AND c.name = ?
                    """,
                    (country,))
        return pd.DataFrame(cur.fetchall(), columns=("country",))

# In welchen Ländern war X Schauspieler?
    def get_country_by_cast(self, cast: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT country.name
                    FROM [country], [show_country] as scountry,
                         [cast], [show_cast] as scast
                    WHERE scountry.show_id = scast.show_id
                    AND country.id = scountry.country_id
                    AND [cast].id = scast.cast_id
                    AND [cast].name = ?
                    """,
                    (cast,))
        return pd.DataFrame(cur.fetchall(), columns=("country",))


# Pie Diagram: Anzahl Serien vs Anzahl Filme

# 2D-Graphen: Die Entwicklung von Filmen/Serien pro Release Jahr

# Mit Slidern die Anzahl (z.B. 10) betrachteter Schauspieler einstellen und dann ein Pie Chart erstellen
# mit den ersten z.B. 10 Schauspielern, wobei der Schauspieler mit den meisten Filmen den größten Platz einnimmt

# Wie viele Schauspieler gibt es im Durchschnitt in einem Film/Serie?

# Wie lang sind Filme aus Land X im Durchschnitt?

# Wie viele Saisons hat eine Serie im Schnitt?
# Balkendiagramm: Saisons nebeneinander aufstellen je nach Anzahl der Serie
# Y-Achse: Anzahl solche Serien
# X-Achse: Anzahl Saisons dieser Serien


# TODO
# - ,,gesäuberte" CSV exportieren
# - Dokumentation
# - 2x Video 
#
# Optional:
# - OOP


if __name__ == "__main__":
    con = db_connector()
    # con.reset_database()
    # con.import_file("netflix_titles.csv")
    data = con.get_cast_by_country("Germany")
    con.con.close()
