#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yaml
import argparse
import time
from threading import Thread
from queue import Queue, Empty
import os
import re
from threading import Event
from vertica import VerticaConnection, VerticaUtils
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(conflict_handler="resolve")
    parser.add_argument('-c', '--config',
                        default='pex_test_solution.yaml',
                        help='YAML config file with static configuration, default pex_test_solution.yaml')
    parser.add_argument('-p', '--parallel', help='Override parallel from config file')
    parser.add_argument('-ph', '--phase', help='Start from this phase. Check the config file for phase names.')
    parser.add_argument('-s', '--skip-init-db', action='store_const', default=False, const=True,
                        help='Skip init (recreate) DB (schema). Valuable, when you skip phases.')
    parser.add_argument('-d', '--debug', action='store_const', default=False, const=True, help='Turn on debug')
    return parser.parse_args()


def read_config(config_file):
    with open(config_file) as f:
        config_content = yaml.safe_load(f)
    return config_content


def get_datetime():
    return time.strftime("%d/%m/%Y %H:%M:%S")


def info(debug_msg):
    print('{} - {}'.format(get_datetime(), debug_msg))


def debug(debug_on, debug_msg):
    if debug_on:
        print('{} - {}'.format(get_datetime(), debug_msg))


def get_line(line, value):
    if not line:
        return value
    else:
        return ',{}'.format(value)


def print_separator():
    print(60 * "-")


def get_conn(config, host):
    conn_info = {'host': host,
                 'port': config['port'],
                 'user': config['user'],
                 'password': config['password'],
                 'dbname': config['dbname'],
                 'timeout': config['timeout']}
    conn = VerticaConnection(conn_info, host)
    return conn


def init_db(conn, config):
    utils = VerticaUtils(conn)
    utils.drop_schema_if_exists(config['schema_name'], cascade=True)
    utils.create_schema(config['schema_name'])
    for pool in config['resource_pools']:
        utils.drop_resource_pool_if_exists(pool['pool_name'])
        utils.create_resource_pool(pool['pool_name'], pool)


def read_sql_file(file_path):
    with open(file_path) as fp:
        sql_text = fp.read()
    re_queries = re.compile(r'^\s*;\s*$', re.M)
    return [q for q in re_queries.split(sql_text) if q]


def get_queue_cancel(cancel_event, process_queue, cancel_check_time=1):
    while not cancel_event.is_set():
        try:
            data = process_queue.get(True, cancel_check_time)
            return data
        except Empty:
            pass
    return None


def _execute_analyze_statistics(conn, table_name):
    statement = "select /*+ label(analyze_stats_{0}) */ analyze_statistics('{0}') as analyze_result"
    result = conn.exec_simple(statement.format(table_name))
    if 'analyze_result' not in result or result['analyze_result'] != 0:
        raise Exception('execute_analyze_stmt did not finish with result 0, result={}'.format(result))


def _execute_analyze_constraints(conn, table_name):
    statement = "select /*+ label(analyze_constraints_{0}) */ analyze_constraints('{0}') as analyze_result"
    result = conn.exec_default(statement.format(table_name))
    if len(result) > 0:
        raise Exception('analyze_constraints found issues, table={} first_issue={}'.format(table_name, result[0]))


def _analyze_stats_constraints(conn, phase, config_database, action):
    setup_connection(conn, config_database['schema_name'], phase['pool_name'])
    for table_name in phase[action]:
        _execute_analyze_statistics(conn, table_name)
        _execute_analyze_constraints(conn, table_name)


def analyze_stats_constraints(conn, phase, config_database):
    if 'analyze_tables' in phase:
        _analyze_stats_constraints(conn, phase, config_database, 'analyze_tables')
    if 'analyze_constraints' in phase:
        _analyze_stats_constraints(conn, phase, config_database, 'analyze_constraints')


def _execute_copy(conn, statement):
    """
    This is hacky way, how to workaround missing COPY LOCAL in vertica-python driver.

    :param conn: DB connection
    :param statement: COPY statement to be executed
    :return:
    """
    re_from = re.compile(r'from\s+local\s+\'([^\']+)\'', re.I | re.M)
    re_exception = re.compile(r'exceptions\s+\'([^\']+)\'', re.I | re.M)
    file_name = re_from.search(statement).group(1)
    statement = re_from.sub('from stdin', statement) + ';'
    exception_file = re_exception.search(statement).group(1)
    conn.exec_copy(statement, file_name)
    if os.path.isfile(exception_file):
        with open(exception_file) as fp:
            raise Exception(
                "Copy failed with exceptions, first exception: {}\nstatement: {}".format(
                    fp.readline(), statement))


def _exec_select(conn, statement):
    result = conn.exec_default(statement)
    header = False
    lines = []
    for row in result:
        if not header:
            header_line = ''
            for col_name in row.keys():
                header_line += get_line(header_line, col_name)
            lines.append(header_line)
            header = True
        line = ''
        for col_value in row.values():
            line += get_line(line, str(col_value))
        lines.append(line)
    return lines


def execute_query(conn, request):
    statement = request['sql_statement']
    phase = request['phase']
    query_type = phase['query_type']
    result = []
    if query_type == 'dml':
        conn.exec_noresult(statement)
        conn.exec_noresult('commit;')
    elif query_type == 'ddl':
        conn.exec_noresult(statement)
    elif query_type == 'load':
        _execute_copy(conn, statement)
    elif query_type == 'select':
        result = _exec_select(conn, statement)

    return result


def setup_connection(conn, schema_name, pool_name):
    conn.exec_noresult('set search_path to {}'.format(schema_name))
    conn.exec_noresult('set resource_pool to {}'.format(pool_name))


def execute_queries_thread(request_queue, report_queue, cancel_event, args):
    while not cancel_event.is_set():
        request = get_queue_cancel(cancel_event, request_queue)
        if not request:
            continue
        start = time.time()
        result = []
        try:
            conn = get_conn(request['config_database'], request['host'])
            setup_connection(conn, request['config_database']['schema_name'], request['phase']['pool_name'])

            result = execute_query(conn, request)

            request['status'] = 'ok'
            request['error'] = ''
        except Exception as e:
            request['error'] = str(e)
            request['status'] = 'error'
        finally:
            request['duration'] = int((time.time() - start) * 1000)
            request['result'] = result
            debug(args.debug, 'query_name="{}" status={} duration={} result_size={} error={}'.format(
                request['query_name'], request['status'],
                request['duration'], len(request['result']), request['error']))
            report_queue.put(request)
            request_queue.task_done()


def populate_request(request_queue, sql_statement, host, phase, config_database):
    re_label = re.compile(r'label\(([^)]+)\)', re.I)
    re_hint_remove = re.compile(r'/\*\+[^*]+\*/', re.I)
    label_groups = re_label.search(sql_statement)
    if not label_groups:
        # Label is mandatory, so we can debug and search for queries in DB
        raise Exception('query does not contain label - {}'.format(sql_statement))
    else:
        label = label_groups.group(1)
    if phase['query_type'] == 'ddl':
        # DDL does not support labels
        sql_statement = re_hint_remove.sub('', sql_statement)
    elif phase['query_type'] == 'load':
        sql_statement = re_hint_remove.sub('', sql_statement)
        sql_statement += "\nSTREAM NAME '{}'".format(label)
    request_queue.put({
        'host': host,
        'query_name': label,
        'sql_statement': sql_statement,
        'phase': phase,
        'config_database': config_database
    })


def start_threads(request_queue, report_queue, cancel_event, parallelism, args):
    workers = []
    for i in range(parallelism):
        worker = Thread(
            target=execute_queries_thread,
            args=[request_queue, report_queue, cancel_event, args]
        )
        worker.setDaemon(True)
        worker.start()
        workers.append(worker)
    return workers


def check_progress(report_queue, cancel_event, request_count):
    results = []
    for i in range(request_count):
        results.append(get_queue_cancel(cancel_event, report_queue))
    return results


def execute_queries(conn, cancel_event, host, phase, config_database, args, parallelism):
    report_queue = Queue()
    request_queue = Queue()
    cancel_event.clear()

    workers = start_threads(request_queue, report_queue, cancel_event, parallelism, args)
    request_count = 0
    for sql_statement in read_sql_file(phase['sql_file']):
        populate_request(request_queue, sql_statement, host, phase, config_database)
        request_count += 1

    results = check_progress(report_queue, cancel_event, request_count)

    # After everything finished, we can finally analyze stats and constraints on required tables
    analyze_stats_constraints(conn, phase, config_database)

    cancel_event.set()

    for worker in workers:
        worker.join()

    return results


def create_dir(directory):
    if not os.path.isdir(directory):
        os.mkdir(directory)


def report_results(results, result_dir):
    for result in results:
        print_separator()
        print('-- host: {}'.format(result['host']))
        print('-- phase: {}'.format(result['phase']['name']))
        print('-- query: {}'.format(result['query_name']))
        print('-- return status: {}'.format(result['status']))
        print('-- duration: {}'.format(result['duration']))

        if result['phase']['query_type'] == 'select':
            result_file_name = Path(result_dir) / '{}.csv'.format(result['query_name'])
            print('-- result_file_name: {}'.format(result_file_name))
            with open(result_file_name, 'w') as fp:
                for line in result['result']:
                    fp.write('{}\n'.format(line))


def execute_host(host, config, args, cancel_event, result_dir):
    conn = None
    results = []
    config_database = config['database']
    result_host_dir = Path(result_dir) / host
    create_dir(result_host_dir)
    try:
        info('START host={}'.format(host))
        start_host = time.time()

        conn = get_conn(config_database, host)
        if not args.skip_init_db:
            init_db(conn, config_database)

        phase_continue = False
        for phase in config['sql_pipeline']:
            # Either phase is NOT required or current phase is the required one or any following one
            if not args.phase or args.phase == phase['name'] or phase_continue:
                phase_continue = True
                parallelism = args.parallel or int(phase['parallel']) if 'parallel' in phase else 1
                info('START host={} phase={} parallelism={}'.format(host, phase['name'], parallelism))
                start_phase = time.time()

                results += execute_queries(conn, cancel_event, host, phase, config_database, args, parallelism)

                duration_phase = int((time.time() - start_phase)*1000)
                info('END host={} phase={} duration={}'.format(host, phase['name'], duration_phase))

        duration_host = int((time.time() - start_host)*1000)
        info('END host={} duration={}'.format(host, duration_host))
    finally:
        cancel_event.set()
        if conn:
            conn.close()
        report_results(results, result_host_dir)
        print_separator()


def main():
    args = parse_args()
    config = read_config(args.config)
    hosts = config['hosts']
    start_all = time.time()
    cancel_event = Event()
    result_dir = config['results']['directory']
    create_dir(result_dir)
    info('START')

    for host in hosts:
        execute_host(host, config, args, cancel_event, result_dir)

    duration = int((time.time() - start_all)*1000)
    info('END time={}'.format(duration))


if __name__ == "__main__":
    main()
