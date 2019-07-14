#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
from vertica_python import connect as vp_connect, errors


class VerticaConnection:
    def __init__(self, conn_attributes, host, schema_name='public', resource_pool='general'):
        try:
            self.connection = vp_connect(**{
                'host': host,
                'port': conn_attributes['port'],
                'user': conn_attributes['user'],
                'password': conn_attributes['password'],
                'database': conn_attributes['dbname'],
                'read_timeout': conn_attributes['timeout']
            })
        except Exception as e:
            print('action=connect_db status=error Unable to connect to DB: {0}'.format(e))
            raise
        else:
            self._cursor = self.connection.cursor('dict')
            self._exec('select 1')
            stmt = 'set search_path to "{0}"'.format(schema_name)
            self.exec_noresult(stmt)
            stmt = 'set resource_pool to "{0}"'.format(resource_pool)
            self.exec_noresult(stmt)

    def close(self):
        """
        Close database connection, if it is opened
        """
        if self.connection:
            self.connection.close()

    def _reset_cursor(self):
        """
        Reset cursor is needed after COPY.

        :return:
        """
        if self._cursor:
            self._cursor = self.connection.cursor('dict')

    def _exec(self, stmt):
        """
        Executes query
        :param stmt: query statement
        """
        try:
            self._cursor.execute(stmt)
        except errors.TimedOutError:
            #
            # See: https://github.com/uber/vertica-python/pull/45
            #
            raise Exception('Timeout occurred when executing statement {}'.format(stmt))

        except errors.ConnectionError as e:
            #
            # vertica-python does not correctly raise TimedOutError to client code. It comes in here
            # as ConnectionError with 'Connection timed out' string in it.
            #
            if 'Connection timed out' in str(e):
                raise Exception('Timeout occurred when executing statement {}'.format(stmt))
        except Exception as e:
            print('action=query_db status=error Unable to execute query "{0}": {1}'.format(stmt, e))
            raise

    def exec_default(self, stmt):
        """
        Execute query against Vertica database.
        Default scenario - return result in same structure, which is produced by vertica-python

        :param stmt: SQL statement to be executed
        :return: Tuple of rows(dictionaries)
        :rtype: Tuple
        """
        self._exec(stmt)
        return self._cursor.fetchall()

    def exec_simple(self, stmt):
        """
        Execute query against Vertica database.
        Simple scenario - save only first row into simple dictionary.
        Used for queries like 'select sum() ...'
        Dictionary key is filled with column_name.

        :param stmt: SQL statement to be executed
        :return: Dictionary containing result of query
        :rtype: dict
        """
        self._exec(stmt)
        rows = self._cursor.fetchall()
        result = collections.defaultdict()
        # Only first row. Typically used for queries like "SELECT SUM() ....."
        for colkey, colvalue in rows[0].items():
            result[colkey] = colvalue
        return result

    def exec_noresult(self, stmt):
        """
        Execute query against Vertica database.
        NoResult scenario - no result returns, just executing query.
        Used for DML, DDL.

        :param stmt: SQL statement to be executed
        :type stmt: str
        :return: Dictionary containing result of query
        :rtype: dict
        """
        self._exec(stmt)

    def exec_complex(self, stmt, key_list):
        """
        Execute query against Vertica database.
        Complex scenario - save all results into complex dictionary.
        Using INDEX as dictionary key on first level
        INDEX must be name of one column, should be unique key for row

        :param stmt: SQL statement to be executed
        :param key_list: set of columns representing unique key for row. Keys of returned dictionary (in the same order)
        :type key_list: list[str]
        :return: Dictionary containing result of query
        :rtype: dict
        """
        self._exec(stmt)
        rows = self._cursor.fetchall()
        result = self.tree()

        for rowValue in rows:
            key_list_values = []
            for key in key_list:
                key_list_values.append(rowValue[key])

            for colKey, colValue in rowValue.iteritems():
                # copy to key_list_values_temp, not reference
                key_list_values_temp = key_list_values[:]
                key_list_values_temp.append(str(colKey))
                # key_list_values_string = '->'.join(key_list_values_temp)
                # logging.lg.debug('{0} -> {1}'.format(key_list_values_string, str(colValue)))
                self.set_key_chain(result, key_list_values_temp, colValue)
        return result

    def exec_copy_fh(self, stmt, fh, buffer_size=1024**2):
        """
        Executes COPY FROM STDIN command implemented into new vertica_python (> v0.5x)
        :param stmt: COPY statement
        :type stmt: unicode
        :param fh: handle to the file to be loaded
        :type fh: BinaryIO
        :param buffer_size: buffer size
        :type buffer_size: int
        """
        try:
            self._cursor.copy(stmt, fh, buffer_size=buffer_size)
        except errors.TimedOutError:
            raise VerticaTimeout(stmt)
        except Exception as e:
            # NOTE: Fix - non-stored message leads to unread message in queue
            # remove once https://github.com/uber/vertica-python/issues/213 is addressed
            if isinstance(e, errors.QueryError):
                self._cursor._message = e.error_response
            raise

    def exec_copy(self, stmt, file_path, buffer_size=1024**2):
        """
        Executes COPY FROM STDIN command implemented into new vertica_python (> v0.5x)
        :param stmt: COPY statement
        :type stmt: unicode
        :param file_path: path to the file to be loaded
        :type file_path: unicode
        :param buffer_size: buffer size
        :type buffer_size: int
        """
        with open(file_path, str('r')) as f:
            self.exec_copy_fh(stmt, f, buffer_size)

    def tree(self):
        """
        Returns complex dictionary structure.
        Used collections library.

        :return: empty initialized dictionary
        :rtype: dict
        """
        return collections.defaultdict(self.tree)

    def set_key_chain(self, cur, key_list, value):
        if len(key_list) == 1:
            cur[key_list[0]] = value
            return
        # see http://stackoverflow.com/questions/17462011/python-generate-a-dynamic-dictionary-from-the-list-of-keys
        # if not cur.has_key(list[0]):
        if key_list[0] not in cur:
            cur[key_list[0]] = {}
        self.set_key_chain(cur[key_list[0]], key_list[1:], value)


class VerticaUtils(object):
    def __init__(self, conn):
        self._conn = conn

    def schema_exists(self, schema_name):
        result = self._conn.exec_default("SELECT count(*) as cnt FROM schemata WHERE schema_name = '{0}'".format(
            schema_name))
        return result[0]['cnt'] > 0

    def drop_schema_if_exists(self, schema_name, cascade=False):
        if self.schema_exists(schema_name):
            self._conn.exec_noresult('DROP SCHEMA {0} {1}'.format(
                schema_name,
                'CASCADE' if cascade else ''
            ))

    def create_schema(self, schema_name):
        self._conn.exec_noresult("CREATE SCHEMA {0}".format(schema_name))

    def resource_pool_exists(self, resource_pool_name):
        result = self._conn.exec_default("SELECT count(*) as cnt FROM resource_pools WHERE name = '{0}'".format(
            resource_pool_name))
        return result[0]['cnt'] > 0

    def drop_resource_pool_if_exists(self, resource_pool_name):
        if self.resource_pool_exists(resource_pool_name):
            self._conn.exec_noresult('DROP RESOURCE POOL {0}'.format(resource_pool_name))

    def create_resource_pool(self, resource_pool_name, rp_settings):
        self._conn.exec_noresult(
            "CREATE RESOURCE POOL {0} maxmemorysize '{1}' maxconcurrency {2} plannedconcurrency {3}".format(
                resource_pool_name, rp_settings['maxmemorysize'],
                rp_settings['maxconcurrency'], rp_settings['plannedconcurrency']))


class VerticaTimeout(Exception):
    """
    This exception is thrown when VerticaDriver encounters time out when interfacing
    with Vertica.
    """

    def __init__(self, stmt):
        Exception.__init__(self, 'Vertica connection timed out while executing query "{0}")'.format(stmt))
