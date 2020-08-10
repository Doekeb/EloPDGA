#!/usr/bin/env python3

import requests
import pandas as pd
import psycopg2 as ps
from bs4 import BeautifulSoup
import datetime as dt
import re
import numpy as np

pd.set_option('display.max_rows', None)

# CONSTANTS
pdgaSecureHeader = "https://www.pdga.com/tour/event/"
pdgaHeader = "http://www.pdga.com/tour/event/"
header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"}
date_junk = '<strong>Date</strong>: '
# END CONSTANTS

class Updater:
    def __init__(self, dbname):
        self.dbname = dbname

    def __enter__(self):
        self.connection = ps.connect(dbname=self.dbname, user="postgres")
        self.cursor = self.connection.cursor()
        self.setup()
        return self

    def __exit__(self, type, value, traceback):
        self.cursor.close()
        self.connection.close()

    def open(self):
        return self.__enter__()

    def setup(self):
        """
        Create the database schemas (if they do not already exist)
        """

        self.cursor.execute("""
        CREATE SEQUENCE IF NOT EXISTS player_id_decrement
        INCREMENT BY -1
        MAXVALUE -1
        START WITH -1;

        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY DEFAULT NEXTVAL('player_id_decrement'),
            name TEXT
        );

        ALTER SEQUENCE player_id_decrement
        OWNED BY players.player_id;

        CREATE TABLE IF NOT EXISTS events (
            event_id INT PRIMARY KEY,
            title TEXT,
            tier TEXT,
            start_date DATE,
            n_rounds INT
        )
        """)
        self.connection.commit()

    def getHTML(self, source):
        """
        Get HTML from source. Input source is a url to a PDGA results page or a PDGA
        event number
        """
        if source[:len(pdgaSecureHeader)] == pdgaSecureHeader or source[:len(pdgaHeader)] == pdgaHeader:
            try:
                r = requests.get(source, headers=header)
                r.raise_for_status()
            except requests.exceptions.HTTPError:
                raise
        else:
            try:
                r = requests.get(pdgaSecureHeader + source, headers=header)
                r.raise_for_status()
            except requests.exceptions.HTTPError:
                raise

        return r

    def getDataFrames(self, html):
        """
        Get data from source. Input source is a url to a PDGA results page or a
        PDGA event number (or in the future, a csv file containing round
        results)
        """
        return [table.filter(regex="PDGA#|Name|Rd") for table in
                pd.read_html(html, header=0, keep_default_na=False)[1:]]
        # First "table" on each results page doesn't conatin useful information


    def update_players(self, table):
        if "PDGA#" not in table.columns:
            table.insert(1, "PDGA#", "")

        for row in range(len(table)):
            id, name = map(str, table[["PDGA#", "Name"]].iloc[row])

            if id == "": # If the person has no PDGA# yet

                # We will use the result of this line to see if the person is already in the database and if so, get their id number
                self.cursor.execute("""
                SELECT player_id FROM players
                WHERE name = %s AND player_id <= 0
                """, (name,))

                existing_id = self.cursor.fetchone()

                if existing_id == None:
                    self.cursor.execute("""
                    INSERT INTO players (name)
                    VALUES (%s)
                    """, (name,))

                    self.cursor.execute("""
                    SELECT player_id FROM players
                    WHERE name = %s AND player_id <= 0
                    """, (name,))

                    existing_id = self.cursor.fetchone()

                table.at[row, "PDGA#"] = existing_id[0]

            else: # The case when the person already has a PDGA#
                self.cursor.execute("""
                INSERT INTO players
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """, (id, name))

        self.connection.commit()

    def update_events(self, html):
        soup = BeautifulSoup(html, "lxml")

        title = soup.find("meta", property="og:title")["content"]
        event_id = int(soup.find("meta", property="og:url")["content"][len(pdgaSecureHeader):])
        tier = soup.find("h4").decode_contents()

        months = {"Jan": 1, "Feb": 2, "Mar": 3,
                  "Apr": 4, "May": 5, "Jun": 6,
                  "Jul": 7, "Aug": 8, "Sep": 9,
                  "Oct": 10, "Nov": 11, "Dec": 12}
        dates = soup.find("li", attrs={"class":"tournament-date"}).decode_contents()[len(date_junk):]
        start_date = dt.date(int(dates[-4:]), months[dates[3:6]], int(dates[:2]))

        n_rounds = int(soup.find_all(string=re.compile("Rd[0-9]"))[-1][2:])

        self.cursor.execute("""
        INSERT INTO events
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """, (event_id, title, tier, start_date, n_rounds))

        self.connection.commit()

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_%s (
            player_id INT REFERENCES players(player_id) UNIQUE
        )""", (event_id,))

        for n in range(1, n_rounds+1):
            self.cursor.execute("""
            ALTER TABLE IF EXISTS event_%s
            ADD COLUMN IF NOT EXISTS round_%s INT
            """, (event_id, n))

        self.connection.commit()

        return event_id

    def update_event(self, table, event_id):
        t = table.filter(regex="PDGA#|Rd").replace(("888", "999"), "")
        n_entries = len(t.columns)
        substitution_string = "(" + ", ".join(("%s",) * n_entries) + ")"
        update_string = ", ".join(["round_%s = COALESCE(excluded.round_%s, event.round_%s)" % (i, i, i)
                                   for i in range(1, n_entries)])
        for row in range(len(t)):
            self.cursor.execute("""
            INSERT INTO event_%s as event
            VALUES %s
            ON CONFLICT (player_id) DO
            UPDATE SET %s
            """ % (event_id, substitution_string, update_string), tuple(map(lambda x: int(x) if x else None, t.iloc[row])))

        self.connection.commit()

    def update(self, *sources):
        """
        Update database with data from source. Input source is a url to a PDGA
        results page or a PDGA event number (or in the future, a csv file
        containing round results)
        """
        for source in sources:
            html = self.getHTML(source).text

            event_id = self.update_events(html) # Update the "events" table

            for table in self.getDataFrames(html):
                self.update_players(table)
                self.update_event(table, event_id) # Update the table specific to *this* event

class Calculator:
    def __init__(self, dbname, k_factor=32, initial_rating=1000, field_size_multiplier=None):
        self.dbname = dbname
        self.k_factor = k_factor
        self.initial_rating = np.float64(initial_rating)
        if field_size_multiplier:
            self.fsm = field_size_multiplier
        else:
            self.fsm = lambda x: x**2 / 10

    def __enter__(self):
        self.connection = ps.connect(dbname=self.dbname, user="postgres")
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, type, value, traceback):
        self.cursor.close()
        self.connection.close()

    def get_event_info(self, *events):
        self.cursor.execute("""
        SELECT event_id, n_rounds FROM events
        WHERE event_id IN %s
        ORDER BY start_date, n_rounds
        """, (events,))
        return pd.DataFrame(self.cursor.fetchall()).set_index(0)

    def get_result_weights(self, event, round):
        self.cursor.execute("""
        SELECT player_id FROM event_%s
        WHERE round_%s IS NOT NULL
        ORDER BY round_%s
        """, (event, round, round))

        players = pd.Index([item[0] for item in self.cursor.fetchall()])
        n_players = len(players)

        places = np.linspace(n_players - 1, 0, n_players) * self.fsm(n_players) / ((n_players * (n_players - 1))/2)

        self.cursor.execute("""
        SELECT COUNT(*) FROM event_%s
        WHERE round_%s IS NOT NULL
        GROUP BY round_%s
        ORDER BY round_%s
        """, (event, round, round, round))

        n_ties = self.cursor.fetchone()

        i = 0
        while n_ties:
            n_ties = n_ties[0]
            places[i : i + n_ties] = [np.average(places[i : i + n_ties])] * n_ties
            i += n_ties
            n_ties = self.cursor.fetchone()

        return pd.Series(places, index=players)

    def calculate_round(self, event, round):
        actual_results = self.get_result_weights(event, round)
        n_players = len(actual_results)
        old_ratings = self.ratings.filter(actual_results.index, axis=0).iloc[:,-1]
        expected_results = self.fsm(n_players) * old_ratings / sum(old_ratings)
        new_ratings = old_ratings + self.k_factor * (actual_results - expected_results)
        new_ratings.name = old_ratings.name + 1

        self.ratings[old_ratings.name + 1] = self.ratings[old_ratings.name]
        self.ratings.update(new_ratings)

    def get_players(self, event):
        self.cursor.execute("""
        SELECT DISTINCT player_id FROM event_%s
        """, (event,))

        players = pd.Index([item[0] for item in self.cursor.fetchall()])

        self.players = self.players.union(players, sort=False)

    def calculate(self, *events):
        events = self.get_event_info(*events)

        self.players = pd.Index([])

        for event in events.index:
            self.get_players(event)

        self.ratings = pd.DataFrame(index=self.players)
        self.ratings.insert(0, 0, self.initial_rating)

        for event in events.index:
            for round in range(1, events[1][event] + 1):
                self.calculate_round(event, round)

        return self.ratings
