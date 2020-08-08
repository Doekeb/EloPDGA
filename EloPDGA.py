#!/usr/bin/env python3

import requests
import pandas as pd
import psycopg2 as ps
import psycopg2.extras as extras
from bs4 import BeautifulSoup
import datetime as dt
import re
import io

# TEST VARIABLES
url = "https://www.pdga.com/tour/event/45744"
event = "45744"
events = ["45744", "45745"]
dbname = "Elo"
# END TEST VARIABLES

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

    # WORKING ON THIS METHOD
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


if __name__ == '__main__':
    with Updater(dbname) as u:
        u.update("45744", "45745")
