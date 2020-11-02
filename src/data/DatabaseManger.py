# @Created on  : 28/10/2020 18:55
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : DatabaseManger.py
import datetime

import mysql.connector
from mysql.connector import errorcode
from mysql.connector.errors import ProgrammingError
import pandas as pd
import os
import numpy as np


class DataBaseManager():
    """
    This class can handle all the operations on a database.
    """
    def __init__(self, db_name='myspotify', logging_level=0):
        self.DB_NAME = db_name
        print(f'Connecting to {self.DB_NAME} database..')
        try:
            self.cnx = mysql.connector.connect(user='root', password='Jacopo87',
                                               host='localhost',
                                               database='myspotify')
            self.cursor = self.cnx.cursor()
            print(f'Connected to {self.DB_NAME}!')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"Database \"{self.DB_NAME}\" does not exist")
            else:
                print(err)
        self.logging_level = logging_level
        self.query = ''

    def create_tables(self, tables):
        """
        Create a new table in the database
        :param tables: dict, the keys are the name of the tables and the values contains
                the actual query strings needed to create the tables.
        """
        for table in tables.keys():
            print(f'Creating {table} table in {self.DB_NAME}..', end='')
            try:
                self.cursor.execute(tables[table])
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("\nERROR: Table already exists.")
                else:
                    print(err.msg)
            else:
                print("Done")

    def drop_table(self, table, confirm=False):
        """
        Drops a table from the database to which the DatabaseManager is connected.
        :param table: str, the name of the table to drop.
        :param confirm: bool, if True asks for user confirmation of deletion.
        """
        if confirm:
            confirmation = input(f'Are you sure you want to delete the table \'{table}\'?')
        else:
            confirmation = 'y'
        if confirmation == 'yes' or confirmation[0] == 'y':
            try:
                self.cursor.execute(f'drop table {table}')
                print(f'Deleting {table} table from {self.DB_NAME}..')
            except ProgrammingError:
                print(f' The table \'{table}\' does not exist')
            else:
                return

    def insert_value(self, table, columns, values):
        """
        Insert a row into the database.
        The method, to be versatile, needs to parse lists and tuples arguments into a query string.
        :param table:  str, name of the table
        :param columns: tuple, tuple of strings corresponding to the columns to insert data in
        :param values: tuple, tuple of mixed types containing the values to add
        """
        values = self.input_check_for_values(values)
        if self.logging_level > 0:
            print(f'Values: {values}')
            print(f'Columns: {columns}')
        columns = self.remove_chars_from_string(str(columns), '\'')  # removing string apostrophe
        if '(' not in columns:  # handles the case of single element
            columns = '(' + columns + ')'

        values_types = self.create_tuple_of_placeholders(values)
        if len(values_types) == 1:
            values_types = self.remove_chars_from_string(
                str(values_types), to_remove=[',', '\''])  # removing trailing comma
        else:
            values_types = self.remove_chars_from_string(str(values_types), to_remove='\'')
        # execute MySql statement
        self.cursor.execute(f'insert into {table} ' +
                            str(columns) +
                            f' values {values_types}', values)

    def insert_values_from_file(self, table, filepath, primary_key):
        """
        Inserts values read from a file into a table.
        The columns where the values are inserted are the columns that are
        both in the file (e.g. csv) and the table we want to insert into.
        :param table: str, name of the table
        :param filepath: str, path (relative to the project) of the file containing the data to insert
        :param primary_key: str, name of the primary key in the table. It has to be also in the file.
        """
        df = pd.read_csv(filepath)
        if primary_key not in list(df.columns):
            print(f'ERROR: the primary key {primary_key} is not a column of the file '
                  f'from which the data is supposed to be inserted in the table {table}')
        df = df.drop_duplicates(subset=primary_key)
        file_columns = df.columns
        columns = self.get_columns_names(table)
        columns_in_common = set(columns).intersection(set(file_columns))
        print(f'Adding values from {filepath.split(os.sep)[-1]} from these columns: \n   {columns_in_common}')
        df = df[columns_in_common]
        for i in range(len(df)):
            values = list(df.loc[i].values)
            self.insert_value(table=table,
                              columns=tuple(columns_in_common),
                              values=values)
            self.cnx.commit()

    def get_columns_names(self, table):
        """
        Get all the column names from a table into a list.
        :param table: str, table name
        :return: list, containing all column names from table
        """
        self.cursor.execute(f"SHOW columns FROM {table}")
        return [column[0] for column in db.cursor.fetchall()]

    def insert_values_from_dict(self, table, d):
        """
        This method inserts values from the dictionary.
        The keys are the columns to insert and the values is the list of values to insert.
        :param d: dict
        """
        columns = tuple(d.keys())
        columns = self.remove_chars_from_string(str(columns), '\'')  # removing string apostrophe
        if '(' not in columns:  # handles the case of single element
            columns = '(' + columns + ')'
        values = pd.DataFrame(d).values
        values_types = self.create_tuple_of_placeholders(values[0])

        values = self.remove_chars_from_string(str([str(v).replace('[', '(').replace(']', ')')  for v in values])[:-1], ['[', ']', '"'])

        if len(values_types) == 1:
            values_types = self.remove_chars_from_string(
                str(values_types), to_remove=[',', '\''])  # removing trailing comma
        else:
            values_types = self.remove_chars_from_string(str(values_types), to_remove='\'')

        self.query = 'INSERT INTO ' + table + ' ' + str(columns)  + ' VALUES ' + values_types
        print(self.query)
        print([tuple(v) for v in pd.DataFrame(d).values])
        self.cursor.execute(self.query, [tuple(v) for v in pd.DataFrame(d).values])

    def select(self, columns):
        self.query = 'SELECT ' + self.remove_chars_from_string(str(columns), ['\'', '\"'])
        return self

    def fromm(self, table):
        self.query = self.query + ' FROM ' + self.remove_chars_from_string(str(table), ['\'', '\"'])
        return self

    def where(self, condition, args):
        self.query = self.query + ' WHERE ' + self.remove_chars_from_string(str(condition), ['\'', '\"'])
        self.args=args
        return self

    def run_query(self, verbose=True):
        self.query = self.query + ';'
        print(self.query)
        self.cursor.execute(self.query, self.args)
        if verbose:
            for col in self.cursor:
                print(col)

    @staticmethod
    def create_tuple_of_placeholders(values):
        """
        This function matches each object type in values to the key of the type_dict
        :param values: list, list of objects.
        :return: tuple, containing the identifiers for each type to insert.
        """
        # return tuple(type_dict[type(val)] for val in values)
        return tuple("%s" for val in values)

    @staticmethod
    def remove_chars_from_string(s, to_remove):
        if type(to_remove) != list:
            to_remove = [to_remove]
        for character in to_remove:
            s = s.replace(character, '')
        return s

    def input_check_for_values(self, values):
        print(values)
        v2 = []
        for v in values:
            try:
                v2.append(int(v))
            except:
                v2.append(v)
        print([type(v) for v in v2])
        return v2

#
db = DataBaseManager(logging_level=1)

db.drop_table('Albums')
TABLES = {}
TABLES['Albums'] = ('create table Albums '
                    '(album_id CHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200), '
                    'total_tracks INT,'
                    'album_type VARCHAR(10),'
                    'release_date DATE);')
db.cnx.commit()

db.create_tables(TABLES)
# db.insert_values_from_dict('Albums', {'name': ['a', 'b'], 'total_tracks': [1,2]})


do = 1
if do:

    db.select('*') \
        .fromm('Albums') \
        .where(condition='release_date = %s', args=(datetime.date(2020,10,22),)) \
        .run_query()

    db.drop_table('Albums')
    TABLES = {}
    TABLES['Albums'] = ('create table Albums '
                        '(album_id CHAR(22) PRIMARY KEY,'
                        'name VARCHAR(200), '
                        'total_tracks INT,'
                        'album_type VARCHAR(10),'
                        'release_date DATE);')
    db.cnx.commit()

    db.create_tables(TABLES)
    db.insert_values_from_file(table='Albums', filepath='../../resources/data/trial.csv', primary_key='album_id')
    db.cnx.commit()
