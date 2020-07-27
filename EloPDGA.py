#!/usr/bin/env python3

import requests
import pandas as pd
import psycopg2 as ps
import psycopg2.extras as extras

# TEST VARIABLES
url = "https://www.pdga.com/tour/event/45744"
event = "45744"
dbname = "Elo"
# END TEST VARIABLES

# CONSTANTS
pdgaSecureHeader = "https://www.pdga.com/tour/event/"
pdgaHeader = "http://www.pdga.com/tour/event/"
header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"}
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
        MAXVALUE 0
        START WITH 0;

        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY DEFAULT NEXTVAL('player_id_decrement'),
            name TEXT
        );

        ALTER SEQUENCE player_id_decrement
        OWNED BY players.player_id;

        CREATE TABLE IF NOT EXISTS events (
            event_id INT PRIMARY KEY,
            title TEXT,
            n_rounds INT,
            tier VARCHAR(10),
            start_date DATE
        );

        CREATE TABLE IF NOT EXISTS rounds (
            event_id INT REFERENCES events(event_id),
            round_num INT,
            round_date DATE
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

    def getDataFrames(self, source):
        """
        Get data from source. Input source is a url to a PDGA results page or a
        PDGA event number (or in the future, a csv file containing round
        results)
        """
        return [table.filter(regex="PDGA#|Name|Rd.*") for table in
                pd.read_html(self.getHTML(source).text,
                             header=0,
                             keep_default_na=False)[1:]]
        # First "table" on each results page doesn't conatin useful information


    def update_players(self, table):
        for row in range(len(table)):

            id, name = map(str, table[["PDGA#", "Name"]].iloc[row])

            if id == "": # If the person has no PDGA# yet

                # We will use the result of this line to see if the person is already in the database and if so, get their id number
                self.cursor.execute("""
                SELECT player_id FROM players
                WHERE name = %s AND player_id <= 0
                """, (name,))

                # If next line is None, then the person doesn't exist in the database
                existing_id = self.cursor.fetchone()

                if existing_id:
                    table.at[row, "PDGA#"] = existing_id

                else:
                    self.cursor.execute("""
                    INSERT INTO players (name)
                    VALUES (%s)
                    """, (name,))

            else: # The case when the person already has a PDGA#
                self.cursor.execute("""
                INSERT INTO players
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """, (id, name))

        self.connection.commit()

    def update(self, source):
        """
        Update database with data from source. Input source is a url to a PDGA
        results page or a PDGA event number (or in the future, a csv file
        containing round results)
        """
        for table in self.getDataFrames(source):
            self.update_players(table)


if __name__ == '__main__':
    with Updater(dbname) as u:
        u.update(event)
