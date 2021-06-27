"""
Matrikelnummern der Gruppe:
    4869673
    5058121
    2177693
"""
import pandas as pd
import numpy as np
import sqlite3
import itertools


class db_connector:
    SHOW_COLUMNS = ["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description"]
    DIRECTOR_COLUMNS = ["id", "name"]
    COUNTRY_COLUMNS = ["id", "name"]
    listed_in_COLUMNS = ["id", "name"]
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
        data["show_id"] = data["show_id"].replace("s", "", regex=True).astype(int)

        cur.executemany(f"""
            INSERT INTO [show] ({", ".join(self.SHOW_COLUMNS)})
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
                                         data_name="listed_in",
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
        cur.execute("DROP TABLE IF EXISTS [show]")
        cur.execute("""
            CREATE TABLE [show] (
            show_id INTEGER PRIMARY KEY,
            type STRING,
            title STRING,
            date_added STRING,
            release_year INTEGER,
            rating STRING,
            duration STRING,
            description STRING)""")

        for table in ("country", "cast", "director", "listed_in"):
            cur.execute(f"DROP TABLE IF EXISTS [{table}]")
            cur.execute(f"""
                CREATE TABLE [{table}](
                    id INTEGER PRIMARY KEY,
                    name STRING UNIQUE
                )""")

            cur.execute(f"DROP TABLE IF EXISTS [show_{table}]")
            cur.execute(f"""
                CREATE TABLE [show_{table}] (
                    id INTEGER PRIMARY KEY,
                    show_id STRING NOT NULL,
                    {table}_id STRING NOT NULL,
                    FOREIGN KEY (show_id) REFERENCES [show] (show_id),
                    FOREIGN KEY ({table}_id) REFERENCES [{table}] (id)
                )""")
            cur.execute(f"DROP VIEW IF EXISTS [{table}_per_show]")
            cur.execute(f"""
                CREATE VIEW [{table}_per_show] AS
                    SELECT show_table.show_id, tbl.name
                    FROM [{table}] AS tbl
                    LEFT JOIN [show_{table}] AS show_table
                    ON tbl.id = show_table.{table}_id
                    WHERE tbl.name != ""
                    AND tbl.name NOT NULL
                """)
            cur.execute(f"DROP VIEW IF EXISTS [show_per_{table}]")
            cur.execute(f"""
                CREATE VIEW [show_per_{table}] AS
                    SELECT s.{", s.".join(self.SHOW_COLUMNS)}, tbl.name AS [{table}]
                    FROM [show] AS s
                    LEFT JOIN [show_{table}] AS show_table
                        ON s.show_id = show_table.show_id
                    LEFT JOIN [{table}] as tbl
                        ON show_table.{table}_id = tbl.id
                    WHERE tbl.name != ""
                    AND tbl.name NOT NULL
                """)

        for table1, table2 in itertools.permutations(("country", "cast", "director", "listed_in"), 2):
            cur.execute(f"DROP VIEW IF EXISTS [{table1}_per_{table2}]")
            cur.execute(f"""
                Create View [{table1}_per_{table2}] AS
                SELECT t1.show_id, t1.name AS {table1}, t2.name AS {table2}
                FROM [{table1}_per_show] AS t1
                LEFT JOIN [{table2}_per_show] AS t2
                ON t1.show_id = t2.show_id
                WHERE t1.name NOT NULL
                AND t1.name != ""
                AND t2.name NOT NULL
                AND t2.name != ""
                """)

        self.con.commit()

    def export_csv(self, filename: str = "export.csv"):
        data = self.get_full_table()
        print(data["show_id"])
        data["show_id"] = "s" + data["show_id"].astype(str)
        data.set_index("show_id", inplace=True)
        data.to_csv(filename, encoding="UTF-8")

    def __get_all(self, table: str):
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT *
            FROM [{table}]
            """)
        return pd.DataFrame(cur.fetchall())

    def __get_filtered(self, table: str, filter_table: str, filter_item: str):
        cur = self.con.cursor()
        cur.execute(f"""
            SELECT *
            FROM [{table}_per_{filter_table}]
            WHERE {filter_table} = ?
            """,
            (filter_item,))
        return pd.DataFrame(cur.fetchall())

# Welche Schauspieler gibt es bei Netflix-Filmen?
    def get_all_cast(self):
        data = self.__get_all("cast")
        data.columns = ("id", "cast")
        return data

# Welche Kategorien gibt es auf Netflix?
    def get_all_listed_in(self):
        data = self.__get_all("listed_in")
        data.columns = ("id", "listed_in")
        return data

# Welche Regisseure gibt es bei Netflix-Filmen?
    def get_all_directors(self):
        data = self.__get_all("director")
        data.columns = ("id", "director")
        return data

# Welche Filme gibt es auf Netflix?
    def get_all_shows(self):
        data = self.__get_all("show")
        data.columns = self.SHOW_COLUMNS
        return data

# In welchen Ländern wurden Filme produziert?
    def get_all_countries(self):
        data = self.__get_all("country")
        data.columns=(id, "country")
        return data

# Welche Filme sind vom Regisseur X?
    def get_shows_by_director(self, director: str):
        data = self.__get_filtered("show", "director", director)
        data.columns = self.SHOW_COLUMNS + ["director"]
        return data

# In welchen Filmen hat Schauspieler X gespielt?
    def get_shows_by_cast(self, cast: str):
        data = self.__get_filtered("show", "cast", cast)
        data.columns = self.SHOW_COLUMNS + ["cast"]
        return data

# Welche Filme sind in der Kategorie X?
    def get_shows_by_listed_in(self, listed_in: str):
        data = self.__get_filtered("show", "listed_in", listed_in)
        data.columns = self.SHOW_COLUMNS + ["listed_in"]
        return data

# Welche Filme sind aus Land X?
    def get_shows_by_country(self, country: str):
        data = self.__get_filtered("show", "country", country)
        data.columns = self.SHOW_COLUMNS + ["country"]
        return data

# 5. Wie viele Filme gibt es pro Land?
    def get_shows_per_country(self):
        cur = self.con.cursor()
        cur.execute("""
            SELECT name, count(name) AS count
            FROM [country_per_show]
            GROUP BY name
            """)
        return pd.DataFrame(cur.fetchall(),
                            columns=("name", "count"))

#  An welchen Orten wurde Film X gedreht?
    def get_countries_by_show(self, show: str):
        data = self.__get_all("show_per_country")
        data.columns = self.SHOW_COLUMNS + ["country"]
        return data[data["title"]==show]
        
# TODO: Kommentar sollte anders rum sein
#1. In welchen Filmen war X ein Regisseur?
    def get_directors_by_show(self, show: str):
        data = self.__get_all("show_per_director")
        data.columns = self.SHOW_COLUMNS + ["director"]
        return data[data["show"]==show]

# TODO: Auch hier sollte es anders rum sein
# In welchen Filmen war X ein Schauspieler?
    def get_cast_by_show(self, show: str):
        data = self.__get_all("show_per_cast")
        data.columns = self.SHOW_COLUMNS + ["cast"]
        return data[data["show"]==show]

# In welchen Kategorien ist Film X?
    def get_listed_ins_by_show(self, show: str):
        data = self.__get_all("show_per_listed_in")
        data.columns = self.SHOW_COLUMNS + ["listed_in"]
        return data[data["show"]==show]

# In welchen Kategorien ist X ein Regisseur? 
    def get_listed_ins_by_director(self, director: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT l.name
                    FROM [listed_in] as l, [show_listed_in] as sl,
                         [director] as d, [show_director] as sd
                    WHERE sl.show_id = sd.show_id
                    AND l.id = sl.listed_in_id
                    AND d.id = sd.director_id
                    AND d.name = ?
                    """,
                    (director,))
        return pd.DataFrame(cur.fetchall(), columns=("listed_in",))

# In welchen Kategorien ist X ein Schauspieler?
    def get_listed_ins_by_cast(self, cast: str):
        cur = self.con.cursor()
        cur.execute("""
                    SELECT DISTINCT l.name
                    FROM [listed_in] as l, [show_listed_in] as sl,
                         [cast] as c, [show_cast] as sc
                    WHERE sl.show_id = sc.show_id
                    AND l.id = sl.listed_in_id
                    AND c.id = sc.cast_id
                    AND c.name = ?
                    """,
                    (cast,))
        return pd.DataFrame(cur.fetchall(), columns=("listed_in",))

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

    def get_type_count(self):
        cur = self.con.cursor()
        cur.execute("""
                SELECT type, count(type)
                FROM show
                GROUP BY type
                """)
        return pd.DataFrame(cur.fetchall(),
                            columns=("type", "count"))

    def get_type_count_per_year(self):
        cur = self.con.cursor()
        cur.execute("""
                SELECT type, release_year, count(type)
                FROM show
                GROUP BY type, release_year
                """)
        return pd.DataFrame(cur.fetchall(),
                            columns=("type", "release_year", "count"))

    def get_full_table(self):
        # TODO: Fertig machen
        cur = self.con.cursor()
        cur.execute("""
                    SELECT s.show_id, s.type, s.title, group_concat(DISTINCT director.name) AS director, group_concat(DISTINCT casts.name) AS cast,
                        group_concat(DISTINCT country.name) AS country, s.date_added, s.release_year, s.rating,
                        s.duration, group_concat(DISTINCT listed_in.name) AS "listed in", s.description
                    FROM [show] as s
                    LEFT JOIN [cast_per_show] as casts
                        ON s.show_id = casts.show_id
                    LEFT JOIN [country_per_show] as country
                        ON s.show_id = country.show_id
                    LEFT JOIN [director_per_show] as director
                        ON s.show_id = director.show_id
                    LEFT JOIN [listed_in_per_show] as listed_in
                        ON s.show_id = listed_in.show_id
                    GROUP BY s.show_id, s.type, s.title, s.date_added,
                        s.release_year, s.rating, s.duration, s.description
                    """)
        return pd.DataFrame(cur.fetchall(),
                            columns=("show_id", "type", "title",
                                     "director", "cast", "country", "date_added",
                                     "release_year", "rating", "duration",
                                     "listed_in", "description"))


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
# - 2x Video + Upload
# - README fertigstellen
# - Streamlit-Application deployen
#
# Optional:
# - OOP


if __name__ == "__main__":
    con = db_connector()
    #con.reset_database()
    #con.import_file("netflix_titles.csv")
    #con.export_csv()
    test = con.get_type_count_per_year()
    #test = con._db_connector__get_all("show_per_country")
    #test.columns = con.SHOW_COLUMNS + ["country"]
    #test = test[test["title"]=="Houston, We Have a Problem!"]
    con.con.close()
