#!/usr/bin/env python3

from EloPDGA import Updater

dbname = "Elo"

def Ex_1():
    """
    Update the database to include results from six events
    """
    with Updater(dbname) as u:
        u.update("41276", "42410", "45744", "45745", "46340", "46407")

if __name__ == '__main__':
    Ex_1()
