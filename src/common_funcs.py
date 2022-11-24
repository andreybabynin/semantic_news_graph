"""Module for common project functions
"""
import sys
import re
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values

# GLOBAL COMMON CONSTANTS
URL_WIKIDATA_API = "https://www.wikidata.org/w/api.php"
RE_WIKIDATA_CLEAN_QUERY = re.compile(r"[\"!'«».()+?]")  # clean symbols


def safe_pg_write_query(pg_conn_cfg, sql_query, placeholder=None, verbose=False):
    """Support only qmark style placeholders"""
    try:
        pg_con = psycopg2.connect(
            dbname=pg_conn_cfg["dbname"],
            user=pg_conn_cfg["user"],
            password=pg_conn_cfg["password"],
            host=pg_conn_cfg["host"],
            port=pg_conn_cfg["port"],
        )
        pg_cur = pg_con.cursor()

        if isinstance(sql_query, list):
            for query in sql_query:
                pg_cur.execute(query)

        elif placeholder is None:
            pg_cur.execute(sql_query)

        elif isinstance(placeholder, tuple):
            pg_cur.execute(sql_query, placeholder)

        elif isinstance(placeholder, list):
            pg_cur.executemany(sql_query, placeholder)

        pg_con.commit()
        pg_cur.close()
        if verbose:
            print("All operation complete.")

    except (Exception, Error) as error:
        print("Error connection to PostgreSQL:\n", error)
        sys.exit(str(error))
    finally:
        if pg_con:
            pg_cur.close()
            pg_con.close()
            if verbose:
                print("Connection to PostgreSQL closed.")


def safe_pg_read_query(pg_conn_cfg, sql_query, placeholder=None, verbose=False):
    """Support only qmark style placeholders"""

    fetchall_list = None

    try:
        pg_con = psycopg2.connect(
            dbname=pg_conn_cfg["dbname"],
            user=pg_conn_cfg["user"],
            password=pg_conn_cfg["password"],
            host=pg_conn_cfg["host"],
            port=pg_conn_cfg["port"],
        )
        pg_cur = pg_con.cursor()

        if placeholder is None:
            pg_cur.execute(sql_query)

        elif isinstance(placeholder, tuple):
            pg_cur.execute(sql_query, placeholder)

        fetchall_list = pg_cur.fetchall()
        pg_cur.close()
        if verbose:
            print("All operation complete.")

    except (Exception, Error) as error:
        print("Error connection to PostgreSQL:\n", error)
        sys.exit(str(error))
    finally:
        if pg_con:
            pg_cur.close()
            pg_con.close()
            if verbose:
                print("Connection to PostgreSQL closed.")

    return fetchall_list


def safe_pg_execute_values(pg_conn_cfg, sql_query, placeholder, verbose=False):
    """Support only qmark style placeholders"""
    try:
        pg_con = psycopg2.connect(
            dbname=pg_conn_cfg["dbname"],
            user=pg_conn_cfg["user"],
            password=pg_conn_cfg["password"],
            host=pg_conn_cfg["host"],
            port=pg_conn_cfg["port"],
        )
        pg_cur = pg_con.cursor()

        execute_values(pg_cur, sql_query, placeholder)

        pg_con.commit()
        pg_cur.close()
        if verbose:
            print("All operation complete.")

    except (Exception, Error) as error:
        print("Error connection to PostgreSQL:\n", error)
        sys.exit(str(error))
    finally:
        if pg_con:
            pg_cur.close()
            pg_con.close()
            if verbose:
                print("Connection to PostgreSQL closed.")


def wbsearchentities(name, session):
    """Search entity in wikidata. Return raw results. Docs:
    https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
        Args:
            name (str): string to search
            session: requests session get by "session = requests.Session()"

        Returns:
            dict or None: return results in dict (from json response, first entity) or
                  None if not found
    """

    res = session.post(
        URL_WIKIDATA_API,
        data={
            "action": "wbsearchentities",
            "search": name,
            "language": "ru",
            "limit": "1",
            "format": "json",
        },
    )
    try:
        res_json = res.json()["search"][0]
    except:  # noqa E722
        res_json = None
    return res_json


def get_wikidata_qid(name, session):
    """Search QID wikidata of entity. Return final results.

    Args:
        name (str): string to search
        session: requests session get by "session = requests.Session()"

    Returns:
        str or None: QID (e.g. "Q123353") or None
    """

    res_json = wbsearchentities(name, session)

    # attempt to search by clean name if not found by raw text
    if res_json is None:
        clean_name = RE_WIKIDATA_CLEAN_QUERY.sub("", name)
        if clean_name != name:
            res_json = wbsearchentities(clean_name, session)

    return res_json["id"] if res_json is not None else None
