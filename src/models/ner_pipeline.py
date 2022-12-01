"""Ner-pipeline script (run from cli).

Description of the algorithm:
1. We select news for their transfer to the ner-pipeline (the main selection
   criteria: its summary is already available for the news, the news has not
   yet been processed by the ner-pipeline)
2. We extract ner and bring them to normal form (note: first of all, we try
   to extract NER from the summary, if in total we have >= 2 NER, then we stop
   there, otherwise (NER < 2), we try to extract NER from full text of the news)
3. We carry out the Entity linking procedure, we try to match based on the
   local database, if it doesn't work, through an external request to wikidata
   (we additionally save the results of the request to wikidata in local databases)
4. We enter the results of the work into the database (tables news_links, ner,
   ner_synonyms, synonyms_stats)
5. We review the default names for ner in the ner table (based on usage statistics,
   and it is desirable to review only for ners for which new values were added to
   the ner_synonyms table as a result of the pipeline)
"""
import re
import os
import sys
import requests
import datetime
import warnings
from collections import defaultdict
from src.common_funcs import safe_pg_read_query, safe_pg_write_query, get_wikidata_qid
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
import stanza
import natasha

warnings.filterwarnings("ignore")


# Hyperparameters
PG_CONN_CFG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
}
with open(os.environ.get("POSTGRES_PASSWORD_FILE"), "r") as f:
    PG_CONN_CFG["password"] = f.readlines()[0].rstrip("\n")


def select_news_to_ner_pip(pg_conn_cfg):
    """
    Select news to transfer them to the ner-pipeline.
    Selection criteria:
    1) The news has already passed through the summary pipeline (i.e. there is
       a summary text)
    2) The news has not previously passed through the ner-pipeline, in the
    news_links table there are no records for it, i.e. missing id_news (for
    news for which we cannot extract any ner, in the table news_links is
    written with id_news and id_ner = Null)
    """

    query_news_to_ner_pipeline = """
    SELECT news.id_news, news_text, summary_text
    FROM (SELECT id_news, news_text
          FROM news
          WHERE id_news NOT IN (SELECT DISTINCT id_news
                                FROM news_links)) AS news
         INNER JOIN news_summary
         ON news.id_news = news_summary.id_news;
    """
    # list of tuples(id_news, news_text, summary_text)
    return safe_pg_read_query(pg_conn_cfg, query_news_to_ner_pipeline)


def get_norm_ners_from_news(news_to_ner):  # noqa C901
    """Get normalized ners for list news (from summary or full text). If amount
    of ners from summary < 2, trying to get ners from full text.

    news_to_ner list of tuples(id, text, summary)
    return list of tuples(id_news, tuple("norm_ner1", "norm_ner2", ...))
    """

    def ners_extract_normalize(text, min_count_ner=False, verbose=False):
        """Extract and normalize ners from one doc"""

        stanza_ners = stanza_nlp(text).ents  # 90% CPU time

        natasha_doc = natasha.Doc(text)
        natasha_doc.segment(natasha_segmenter)
        natasha_doc.tag_morph(natasha_morph_tagger)
        natasha_doc.tag_ner(natasha_ner_tagger)

        natasha_ners_text = tuple([span.text for span in natasha_doc.spans])
        only_stanza_ners = [
            stanza_ner
            for stanza_ner in stanza_ners
            if stanza_ner.text not in natasha_ners_text
        ]

        for ent in only_stanza_ners:
            id_start, id_stop = None, None
            for i in range(len(natasha_doc.tokens)):
                if natasha_doc.tokens[i].start == ent.start_char:
                    id_start = i
                    char_start = natasha_doc.tokens[i].start
                if natasha_doc.tokens[i].stop >= ent.end_char:
                    id_stop = i
                    char_stop = natasha_doc.tokens[i].stop
                    break
            if id_start is not None and id_stop is not None:

                natasha_doc.spans.append(
                    natasha.doc.DocSpan(
                        start=char_start,
                        stop=char_stop,
                        type=ent.type,
                        text=text[char_start:char_stop],
                        tokens=natasha_doc.tokens[id_start : id_stop + 1],  # noqa E203
                    )
                )
            elif verbose:
                print("Info message: Not compatible tokens:")
                print(ent)
                print("\n".join(map(str, natasha_doc.tokens)))
                print("\n".join(map(str, natasha_doc.spans)))

        # early stop when count ner < min_count_ner
        if min_count_ner is not None and len(natasha_doc.spans) < min_count_ner:
            return None

        for span in natasha_doc.spans:
            span.normalize(natasha_morph_vocab)

        return natasha_doc.spans

    def get_norm_ners_from_one_news(news: tuple):
        """Get normalized ners from one news (from summary or full text). If amount
        of ners from summary < 2, trying to get ners from full text.

        news (tuple(id, text, summary)): one news
        return: tuple of normalized ners"""

        # get ners from summary
        natasha_doc_spans = ners_extract_normalize(
            clean_re.sub("", news[2]), min_count_ner=2
        )

        # if ners from summary < 2, trying to get ners from full text
        if natasha_doc_spans is None:
            natasha_doc_spans = ners_extract_normalize(clean_re.sub("", news[1]))

        return tuple(set(map(lambda x: x.normal, natasha_doc_spans)))

    # pattern to clean news text from ⚡️🎾❗️🌏... and other
    clean_re = re.compile(r"[^\x20-\xFFа-яА-ЯёЁ№\n]+|__|\*\*")

    # for stanza nlp-pipline
    stanza_nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner")

    # for natasha nlp-pipline
    natasha_segmenter = natasha.Segmenter()
    natasha_morph_vocab = natasha.MorphVocab()
    natasha_emb = natasha.NewsEmbedding()
    natasha_morph_tagger = natasha.NewsMorphTagger(natasha_emb)
    natasha_ner_tagger = natasha.NewsNERTagger(natasha_emb)

    # output is list of tuple(id_news, tuple("norm_ner1", "norm_ner2", ...))
    return list(map(lambda x: (x[0], get_norm_ners_from_one_news(x)), news_to_ner))


def entity_linking(pg_conn_cfg, ner_tuples):
    """Entity linking and preparation of data for writing to the database.
    Algorithm: We try to match based on the local database, if it doesn’t
    work, through an external request to wikidata (we additionally save
    the results of the request to wikidata in local databases).

    ner_tuples: list of tuple(id_news, tuple("norm_ner1", "norm_ner2", ...))

    return: (new_ner_wikidata_ids, ner_wikidata_ids, ner_ids_news,
            rows_to_news_links, ner_syn_news_count)

        new_ner_wikidata_ids (list of tuples(ner_name, qid_wikidata)): list
            of new ners to insert to ner db table; qid_wikidata=None, if is unknown;
        ner_wikidata_ids (list of tuples(ner_name, qid_wikidata)): ners without
            found in local synonym database;
        ner_ids_news (dict("ner1":[news_id2, news_id33],
                           "ner2":[news_id2, news_id11, ...], ...): ners without
            found in local synonym database;
        rows_to_news_links (list of tuples(id_news, id_ner)): to insert into
            news_links db table, at this stage, here are only ners found in the
            local database of synonyms;
        ner_syn_news_count (list of tuples(syn_name, news_count)): for add news_counts
            into table synonyms_stats (all synonyms found in current processing news);

    """

    # Unloading our dictionary of synonyms into memory
    query = """
    SELECT ner_synonym, id_synonim, id_ner FROM ner_synonyms;
    """
    # list of tuples(ner_synonym, id_synonim, id_ner)
    synonims_dict = safe_pg_read_query(pg_conn_cfg, query)
    # convert to dict { 'ner_synonym': (id_synonim, id_ner)}
    synonims_dict = {
        ner_syn: (id_syn, id_ner) for ner_syn, id_syn, id_ner in synonims_dict
    }

    # default value for defaultdict
    def def_value():
        return []

    # ner_ids_news is dict("ner1":[news_id2, news_id33],
    #                      "ner2":[news_id2, news_id11, ...],
    #                      ...)
    ner_ids_news = defaultdict(def_value)
    news_without_ners = []
    for news_id, ner_tuple in ner_tuples:
        if ner_tuple != ():
            for ner in ner_tuple:
                ner_ids_news[ner].append(news_id)
        else:
            news_without_ners.append(news_id)

    # from defaultdict to normal dict
    ner_ids_news = dict(ner_ids_news)

    # list of tuples(synonym_name, news_count)
    # for add news_count into table synonyms_stats (will come later)
    ner_syn_news_count = [(syn, len(news)) for syn, news in ner_ids_news.items()]

    #######################################
    # find ner synonyms in local database

    # list of tuples(id_news, id_ner) to insert into news_links db table
    rows_to_news_links = [(id_news, None) for id_news in news_without_ners]
    found_ners = []
    for ner, ids_news in ner_ids_news.items():
        if ner in synonims_dict:
            rows_to_news_links.extend(
                list(zip(ids_news, [synonims_dict[ner][1]] * len(ids_news)))
            )
            found_ners.append(ner)

    # delele ners found in local database
    for ner in found_ners:
        del ner_ids_news[ner]

    session = requests.Session()
    ner_wikidata_ids = list(
        zip(
            ner_ids_news.keys(),
            map(lambda x: get_wikidata_qid(x, session), ner_ids_news.keys()),
        )
    )

    #######################################
    # Form data to be added to the ner database table
    # (new ners that are not in the database)

    # first, we get a list of qid_wikidata that are already in the table ner
    query = """
    SELECT qid_wikidata FROM ner
    WHERE qid_wikidata IS NOT Null;
    """
    # list of exist qid_wikidata
    exist_qid_wikidata = list(
        map(lambda x: x[0], safe_pg_read_query(pg_conn_cfg, query))
    )

    ner_without_qid = []
    new_ner_wikidata_ids = defaultdict(def_value)
    for ner, qid in [
        row for row in ner_wikidata_ids if row[1] not in exist_qid_wikidata
    ]:
        if qid is not None:
            new_ner_wikidata_ids[qid].append(ner)
        else:
            ner_without_qid.append(ner)

    # for main ner_name select a synonym that is mentioned in the maximum number of news
    new_ner_wikidata_ids = [
        (max(ners, key=lambda x: len(ner_ids_news[x])), qid)
        for qid, ners in new_ner_wikidata_ids.items()
    ]
    # add ners without qid
    new_ner_wikidata_ids.extend([(ner, None) for ner in ner_without_qid])

    return (
        new_ner_wikidata_ids,
        ner_wikidata_ids,
        ner_ids_news,
        rows_to_news_links,
        ner_syn_news_count,
    )


def write_db_results_ner_pipeline(
    pg_conn_cfg,
    new_ner_wikidata_ids,
    ner_wikidata_ids,
    ner_ids_news,
    rows_to_news_links,
    ner_syn_news_count,
):
    """Write the results of the pipeline to the database.

    Parameters are described in the entity_linking function.
    """

    ############################
    # START SINGLE TRANSACTION #

    try:
        pg_con = psycopg2.connect(
            dbname=pg_conn_cfg["dbname"],
            user=pg_conn_cfg["user"],
            password=pg_conn_cfg["password"],
            host=pg_conn_cfg["host"],
            port=pg_conn_cfg["port"],
        )
        pg_cur = pg_con.cursor()

        # Adding data to ner table
        if len(new_ner_wikidata_ids) > 0:
            query = """
            INSERT INTO ner(ner_name, qid_wikidata) VALUES %s;
            """
            execute_values(pg_cur, query, new_ner_wikidata_ids)

        print(
            "Info: Table ner: {} rows were successfully added.".format(
                len(new_ner_wikidata_ids)
            )
        )

        # Unload the updated table ner into memory
        query = """
        SELECT * FROM ner;
        """
        pg_cur.execute(query)
        db_ner_table = pg_cur.fetchall()
        db_ner_table = list(zip(*db_ner_table))  # flip table

        # Adding the missing data to the ner_synonyms database table
        rows_to_ner_synonyms = []
        for name, qid in ner_wikidata_ids:
            # If qid = None, then match by name, otherwise by qid
            if qid is None:
                rows_to_ner_synonyms.append(
                    (db_ner_table[0][db_ner_table[1].index(name)], name)
                )
            else:
                rows_to_ner_synonyms.append(
                    (db_ner_table[0][db_ner_table[2].index(qid)], name)
                )

        # Adding data to the ner_synonyms table
        if len(rows_to_ner_synonyms) > 0:
            query = """
            INSERT INTO ner_synonyms(id_ner, ner_synonym) VALUES %s;
            """
            execute_values(pg_cur, query, rows_to_ner_synonyms)

        print(
            "Info: Table ner_synonyms: {} rows were successfully added.".format(
                len(rows_to_ner_synonyms)
            )
        )

        # Unloading our augmented dictionary of synonyms into memory
        query = """
        SELECT ner_synonym, id_synonim, id_ner FROM ner_synonyms;
        """
        pg_cur.execute(query)
        # list of tuples(ner_synonym, id_synonim, id_ner)
        synonims_dict = pg_cur.fetchall()
        # convert to dict { 'ner_synonym': (id_synonim, id_ner)}
        synonims_dict = {
            ner_syn: (id_syn, id_ner) for ner_syn, id_syn, id_ner in synonims_dict
        }

        # repeat find ner synonyms in updated local database

        # list of tuples(id_news, id_ner) to insert into news_links db table
        found_ners = []
        for ner, ids_news in ner_ids_news.items():
            if ner in synonims_dict:
                rows_to_news_links.extend(
                    list(zip(ids_news, [synonims_dict[ner][1]] * len(ids_news)))
                )
                found_ners.append(ner)  # only for assert check

        assert len(found_ners) == len(ner_ids_news)

        # Adding data to the news_links table
        query = """
        INSERT INTO news_links(id_news, id_ner) VALUES %s;
        """
        execute_values(pg_cur, query, rows_to_news_links)

        print(
            "Info: Table news_links: {} rows were successfully added.".format(
                len(rows_to_news_links)
            )
        )

        # add news_count to synonyms_stats
        date_processed = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        id_model = 2  # task: change to get id_model by type/stage
        rows_to_synonyms_stats = [
            (synonims_dict[ner_syn][0], news_count, date_processed, id_model)
            for ner_syn, news_count in ner_syn_news_count
        ]
        if len(rows_to_synonyms_stats) > 0:
            query = """
            INSERT INTO synonyms_stats(id_synonim, news_count, date_processed, id_model)
            VALUES %s;
            """
            execute_values(pg_cur, query, rows_to_synonyms_stats)

        print(
            "Info: Table synonyms_stats: {} rows were successfully added.".format(
                len(rows_to_synonyms_stats)
            )
        )

        ##########################
        # END SINGLE TRANSACTION #
        pg_con.commit()
        print("Info: Data has been successfully committed to the database.")
        pg_cur.close()

    except (Exception, Error) as error:
        print("Error connection to PostgreSQL:\n", error)
        sys.exit(str(error))
    finally:
        if pg_con:
            pg_cur.close()
            pg_con.close()


def update_main_ner_names_last_processed(pg_conn_cfg):
    query = """
    UPDATE ner
    SET ner_name = ner_synonym
    FROM
        (SELECT id_ner, ner_synonym
         FROM
            (SELECT DISTINCT ON (id_ner)
                id_ner, ner_name, ner_synonym
            FROM
                (SELECT id_ner, ner_name, ner_synonym,
                    SUM(news_count) AS news_count
                 FROM
                    (SELECT DISTINCT id_ner
                     FROM
                        (SELECT id_synonim
                         FROM synonyms_stats
                         WHERE date_processed = (SELECT MAX(date_processed)
                                                 FROM synonyms_stats)) syns
                        LEFT JOIN ner_synonyms USING(id_synonim)) ner_ids
                    INNER JOIN (SELECT id_ner, ner_name
                               FROM ner
                               WHERE name_is_custom IS Null) ners USING(id_ner)
                    LEFT JOIN ner_synonyms USING(id_ner)
                    INNER JOIN synonyms_stats USING(id_synonim)
                 GROUP BY id_ner, ner_name, ner_synonym) ner_syn
            ORDER BY id_ner, news_count DESC) ners_syns
         WHERE ner_name != ner_synonym) ners_to_update
    WHERE ner.id_ner = ners_to_update.id_ner;
    """
    safe_pg_write_query(pg_conn_cfg, query)


def ner_pipeline(pg_conn_cfg):
    """All ner pipeline function."""

    # 1. Select news for their transfer to the ner-pipeline
    news_to_ner = select_news_to_ner_pip(pg_conn_cfg)

    # # Unit-Test
    # news_to_ner = news_to_ner[:50]

    print("Info: {} news selected to ner pipeline.".format(len(news_to_ner)))

    if len(news_to_ner) > 0:
        # 2. Ner extraction and normilization
        ner_tuples = get_norm_ners_from_news(news_to_ner)
        # 3. Entity linking
        entity_linking_output = entity_linking(pg_conn_cfg, ner_tuples)
        # 4. Write results to database
        write_db_results_ner_pipeline(pg_conn_cfg, *entity_linking_output)
        # 5. Update default тук names if needed
        update_main_ner_names_last_processed(pg_conn_cfg)


if __name__ == "__main__":
    ner_pipeline(PG_CONN_CFG)
