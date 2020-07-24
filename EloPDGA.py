#!/usr/bin/env python3

import requests
import pandas as pd
import psycopg2 as ps

# TEST VARIABLES
url = "https://www.pdga.com/tour/event/45744"
events = "45744 45745"
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
        self.conn = ps.connect(dbname=self.dbname, user="postgres")
        self.cur = self.conn.cursor()
        self.setup()
        return self

    def __exit__(self, type, value, traceback):
        self.cur.close()
        self.conn.close()

    def setup(self):
        """
        Create the database schemas (if they do not already exist)
        """
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id int,
            name TEXT
        )
        """)

    def getHTML(self, source):
        """
        Get data from source. Input source is a url to a PDGA results page or a PDGA
        event number (or in the future, a csv file containing round results)
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

    def getDataFrames(self, sources):
        """
        Get data from sources. Input sources is a space-separated string or a list
        of strings, each being a url to a PDGA results page or a PDGA event number
        (or in the future, a csv file containing round results)
        """
        if type(sources) == str:
            self.sources = sources.split()
        else:
            self.sources = sources

        tables = []

        if type(sources) == str:
            sources = sources.split()

        for source in sources:
            tables += pd.read_html(self.getHTML(source).text, header=0, keep_default_na=False)[1:]

        return tables


if __name__ == '__main__':
    with Updater(dbname) as u:
        print(u.getDataFrames(events))
