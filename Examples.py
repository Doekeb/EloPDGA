#!/usr/bin/env python3

from EloPDGA import Updater, Calculator

dbname = "Elo"

def Ex_1():
    """
    Update the database to include results from six events
    """
    with Updater(dbname) as u:
        u.update("41276", "42410", "45744", "45745", "46340", "46407")

def Ex_2():
    """
    Query the database for n_round info
    """
    with Calculator(dbname) as c:
        print(c.get_event_info(41276, 42410, 45744, 45745, 46340, 46407))

def Ex_3():
    """
    Query the database for results of a round
    """
    with Calculator(dbname) as c:
        print(c.get_round(45744, 3))

def Ex_4():
    """
    Query the database for the result weights of a round
    """
    with Calculator(dbname) as c:
        print(c.get_result_weights(45744, 1))

def Ex_5():
    with Calculator(dbname) as c:
        print(c.calculate(45745, 45744, 46340, 46407).sort_values(24))


c = Calculator("Elo")

if __name__ == '__main__':
    Ex_5()
