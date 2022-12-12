"""Script to put custom ners to database.
Custom ners in /data/inherim/custom_ners.csv
"""
import os
import requests
import pandas as pd
import numpy as np
import pymorphy2
from src.common_funcs import safe_pg_write_query, get_wikidata_qid
from src.common_classes import SynNamedEntities

# Hyperparameters
PG_CONN_CFG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
}
with open(os.environ.get("POSTGRES_PASSWORD_FILE"), "r") as f:
    PG_CONN_CFG["password"] = f.readlines()[0].rstrip("\n")


def put_custom_ners(pg_conn_cfg, fname_custom_ners_csv):
    """
    Put custom ners to database.
    Args:
        pg_conn_cfg: dict with cfg connect to database
        fname_custom_ners_csv: path and filename to custom ners .csv
            usually is "/data/inherim/custom_ners.csv"
    """

    custom_ners = pd.read_csv(fname_custom_ners_csv)
    custom_ners

    session = requests.Session()
    custom_ners["qid_wikidata"] = custom_ners.apply(
        lambda x: get_wikidata_qid(x.ner_name, session)
        if x.qid_wikidata is np.NaN
        else x.qid_wikidata,
        axis=1,
    )

    # check custom_ners not exist different ner_names with same qid_wikidata
    assert (
        custom_ners.dropna(subset=["qid_wikidata"])
        .qid_wikidata.duplicated(keep=False)
        .sum()
        == custom_ners.dropna(subset=["qid_wikidata"])
        .duplicated(["ner_name", "qid_wikidata"], keep=False)
        .sum()
    )

    to_ner_table = list(
        set(
            custom_ners[["ner_name", "qid_wikidata"]].itertuples(index=False, name=None)
        )
    )

    to_ner_table = [{"name": row[0], "qid": row[1]} for row in to_ner_table]

    # Update ners to custom, if ner with same Qid exist
    query = """
    UPDATE ner
        SET ner_name = %(name)s,
            name_is_custom = 1
    WHERE (qid_wikidata IS NOT Null AND
        qid_wikidata = %(qid)s) OR
        (ner_name = %(name)s);
    """
    safe_pg_write_query(pg_conn_cfg, query, to_ner_table)

    # INSERT custom ners if ner not exist
    query = """
    INSERT INTO ner(ner_name, qid_wikidata, name_is_custom)
    SELECT %(name)s, %(qid)s, 1
    WHERE NOT EXISTS(SELECT 1 FROM ner
                    WHERE ner_name = %(name)s AND
                    qid_wikidata = %(qid)s);
    """
    safe_pg_write_query(pg_conn_cfg, query, to_ner_table)

    to_ner_synonyms_table = list(
        custom_ners[["ner_synonym", "ner_name"]].itertuples(index=False, name=None)
    )
    to_ner_synonyms_table.extend(
        list(custom_ners[["ner_name", "ner_name"]].itertuples(index=False, name=None))
    )
    to_ner_synonyms_table = list(set(to_ner_synonyms_table))

    morph = pymorphy2.MorphAnalyzer()
    to_ner_synonyms_table = [
        {
            "syn": row[0],
            "name": row[1],
            "for_match": SynNamedEntities.gen_name_for_match(row[0], morph),
        }
        for row in to_ner_synonyms_table
    ]
    to_ner_synonyms_table[:5]

    # Update synonyms to custom ners, if synonym exist
    query = """
    UPDATE ner_synonyms
        SET id_ner = (SELECT id_ner FROM ner
                    WHERE ner_name = %(name)s)
    WHERE ner_synonym = %(syn)s;
    """
    safe_pg_write_query(pg_conn_cfg, query, to_ner_synonyms_table)

    # INSERT custom synonyms if synonym not exist
    query = """
    INSERT INTO ner_synonyms(id_ner, ner_synonym, name_for_match)
    SELECT (SELECT id_ner FROM ner
            WHERE ner_name = %(name)s),
            %(syn)s,
            %(for_match)s
    WHERE NOT EXISTS(SELECT 1 FROM ner_synonyms
                    WHERE ner_synonym = %(syn)s);
    """
    safe_pg_write_query(pg_conn_cfg, query, to_ner_synonyms_table)


if __name__ == "__main__":
    put_custom_ners(PG_CONN_CFG, "/data/inherim/custom_ners.csv")
