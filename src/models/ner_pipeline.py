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
import warnings
from src.common_funcs import safe_pg_read_query, safe_pg_write_query
from src.common_classes import SynNamedEntities
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

    Returns:
        list of tuples(id_news, news_text, summary_text)
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

    Args:
        news_to_ner: list of tuples(id, text, summary)
    Returns:
        SynNamedEntities: with .ents (filled .name_syn, .ntype, .news_ids),
                               .news_without_ents
    """

    def ners_extract_normalize(
        text, min_count_ner=False, verbose=False, only_stanza=True
    ):
        """Extract and normalize ners from one doc"""

        stanza_ners = stanza_nlp(text).ents  # 90% CPU time

        natasha_doc = natasha.Doc(text)
        natasha_doc.segment(natasha_segmenter)
        natasha_doc.tag_morph(natasha_morph_tagger)
        natasha_doc.tag_ner(natasha_ner_tagger)

        # UPDATE! We leave only the entities received by stanza,
        # natasha is used only for normalization (to reduce the
        # number of duplicates)
        if only_stanza:
            natasha_doc.spans = [
                span
                for span in natasha_doc.spans
                if span.text in tuple([ent.text for ent in stanza_ners])
            ]

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

        Args:
            news (tuple(id, text, summary)): one news
        Returns:
            tuple of tuples((norm ner01, ner_type01),
                            (norm ner02, ner_type02), ...)
        """

        # get ners from summary
        natasha_doc_spans = ners_extract_normalize(
            clean_re.sub("", news[2]), min_count_ner=2
        )

        # if ners from summary < 2, trying to get ners from full text
        if natasha_doc_spans is None:
            natasha_doc_spans = ners_extract_normalize(clean_re.sub("", news[1]))

        return tuple(set(map(lambda x: (x.normal, x.type), natasha_doc_spans)))

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

    # list of tuple(id_news, ((norm ner01, ner_type01), (norm ner02, ner_type02), ...)
    news_with_ents = list(
        map(lambda x: (x[0], get_norm_ners_from_one_news(x)), news_to_ner)
    )

    synonyms = SynNamedEntities()
    synonyms.add_ents_from_news(news_with_ents)

    return synonyms


def entity_linking(pg_conn_cfg, synonyms):
    """Entity linking and preparation of data for writing to the database.
    Algorithm: We try to match based on the local database, if it doesn’t
    work, through an external request to wikidata (we additionally save
    the results of the request to wikidata in local databases).

    Args:
        synonyms (SynNamedEntities): with
                 .ents (for each ent filled:
                        ent.name_syn,
                        ent.ntype,
                        ent.news_ids),
                 .news_without_ents

    Returns:
        synonyms (SynNamedEntities): with
                 .ents (for each ent filled:
                        ent.name_syn,
                        ent.ntype,
                        ent.news_ids,
                        ent.in_synonym_table,
                        self.wiki_qid - for ents not .in_synonym_table = False
                                        and succesfull get qid from wikidata API,
                        self.id_ner - for ents founded in local database by
                                      syn_name or wikidata_qid),
                 .news_without_ents
    """
    # 01. Attempt to find ner synonyms in local database (ner_synonyms table),
    # get dict {ner_synonym01: id_ner01, ...}
    query = """
    SELECT DISTINCT ner_synonym, id_ner FROM ner_synonyms;
    """
    db_syn_name2id = dict(safe_pg_read_query(pg_conn_cfg, query))

    # get dict {name_for_match01: id_ner01, ...}
    query = """
    SELECT DISTINCT name_for_match, id_ner
    FROM ner_synonyms
    WHERE name_for_match IS NOT Null;
    """
    db_syn_match2id = dict(safe_pg_read_query(pg_conn_cfg, query))
    synonyms.search_in_synonym_table(db_syn_name2id, db_syn_match2id)

    # 02. Attempt to match entity by qid_wikidata in ner table

    if synonyms.count_without_id_ner > 0:
        # query to wikidata API for get qid for entities without id_ner
        synonyms.search_wikidata_qid()

        query = """
        SELECT qid_wikidata, id_ner FROM ner
        WHERE qid_wikidata IS NOT Null;
        """
        # dict of ents with exist qid_wikidata {"qid_wikidata01": id_ner01, ...}
        db_ner_qid2id = dict(safe_pg_read_query(pg_conn_cfg, query))
        synonyms.match_by_wikidata_qid(db_ner_qid2id)

    return synonyms


def write_db_results_ner_pipeline(pg_conn_cfg, synonyms):
    """Write to the database in single transaction the results of the ner-pipeline.

    Args:
        synonyms (SynNamedEntities): with
                 .ents (for each ent filled:
                        ent.name_syn,
                        ent.ntype,
                        ent.news_ids,
                        ent.in_synonym_table,
                        self.wiki_qid - for ents not .in_synonym_table = False
                                        and succesfull get qid from wikidata API,
                        self.id_ner - for ents founded in local database by
                                      syn_name or wikidata_qid),
                 .news_without_ents

    Returns:
        None: Results write to database.

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

        # 01. Insert new ents (without .id_ner) to ner table
        if synonyms.count_without_id_ner > 0:
            rows_to_ner_table = synonyms.get_rows_to_ner_table()
            query = """
            INSERT INTO ner(ner_name, id_ner_type, qid_wikidata) VALUES %s;
            """
            execute_values(pg_cur, query, rows_to_ner_table)

            # 02. Getting missing id_ners's from padded db ner table
            # get dict {ner_name01: id_ner01, ...}
            query = """
            SELECT DISTINCT ner_name, id_ner FROM ner;
            """
            pg_cur.execute(query)
            db_ner_name2id = dict(pg_cur.fetchall())

            # get dict {qid_wikidata01: id_ner01, ...}
            query = """
            SELECT DISTINCT qid_wikidata, id_ner
            FROM ner
            WHERE qid_wikidata IS NOT Null;
            """
            pg_cur.execute(query)
            db_ner_qid2id = dict(pg_cur.fetchall())

            synonyms.match_by_ner_table(db_ner_name2id, db_ner_qid2id)

        assert synonyms.count_without_id_ner == 0

        print(f"Info: Table ner: {len(rows_to_ner_table)} rows added.")

        # 03. Insert new ents to ner_synonyms table
        rows_to_ner_syn = synonyms.get_rows_to_synonyms_table()

        if len(rows_to_ner_syn) > 0:
            query = """
            INSERT INTO ner_synonyms(id_ner, ner_synonym, name_for_match) VALUES %s;
            """
            execute_values(pg_cur, query, rows_to_ner_syn)

        print(f"Info: Table ner_synonyms: {len(rows_to_ner_syn)} rows added.")

        # 04. Insert rows to db news_links table
        rows_to_news_links = synonyms.get_rows_to_news_links_table()
        query = """
        INSERT INTO news_links(id_news, id_ner) VALUES %s;
        """
        execute_values(pg_cur, query, rows_to_news_links)
        print(f"Info: Table news_links: {len(rows_to_news_links)} rows added.")

        # 05. Insert statistic (e.g. news_count) to synonyms_stats table
        # Get ner_synonym ids from database
        query = """
        SELECT ner_synonym, id_synonim FROM ner_synonyms;
        """
        pg_cur.execute(query)
        # dict('ner_synonym01': id_synonim01, ...)
        syn_ids_dict = dict(pg_cur.fetchall())
        rows_to_syn_stats = synonyms.get_rows_to_synonyms_stats(syn_ids_dict)

        if len(rows_to_syn_stats) > 0:
            query = """
            INSERT INTO synonyms_stats(id_synonim, news_count,
                                       date_processed, id_model, id_ner_type)
            VALUES %s;
            """
            execute_values(pg_cur, query, rows_to_syn_stats)

        print(f"Info: Table synonyms_stats: {len(rows_to_syn_stats)} rows added.")

        ##########################
        # END SINGLE TRANSACTION #
        pg_con.commit()
        print("Info: Data has been successfully committed to the database.")
        pg_cur.close()

    except (Exception, Error) as error:
        print("Error connection to PostgreSQL:\n", error)
        sys.exit(str(error))
    finally:
        if "pg_con" in locals() and pg_con:
            pg_cur.close()
            pg_con.close()


def update_main_ner_names_and_types(pg_conn_cfg):
    # Update main ner names
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

    # Update main ner types
    query = """
    UPDATE ner
    SET id_ner_type = mode_id_ner_type
    FROM
        (SELECT id_ner, mode_id_ner_type, id_ner_type
         FROM
            (SELECT id_ner, mode() WITHIN GROUP (ORDER BY synonyms_stats.id_ner_type)
                            AS mode_id_ner_type
             FROM
                (SELECT DISTINCT id_ner
                 FROM
                    (SELECT id_synonim
                     FROM synonyms_stats
                     WHERE date_processed = (SELECT MAX(date_processed)
                                             FROM synonyms_stats)) syns
                    LEFT JOIN ner_synonyms USING(id_synonim)) ner_ids
                LEFT JOIN ner_synonyms USING(id_ner)
                INNER JOIN synonyms_stats USING(id_synonim)
             GROUP BY id_ner) mode_types
             INNER JOIN ner USING(id_ner)
         WHERE id_ner_type IS Null OR
               mode_id_ner_type <> id_ner_type) types_to_update
    WHERE ner.id_ner = types_to_update.id_ner;
    """
    safe_pg_write_query(pg_conn_cfg, query)


def ner_pipeline(pg_conn_cfg):
    """All ner pipeline function."""

    # 1. Select news for their transfer to the ner-pipeline
    news_to_ner = select_news_to_ner_pip(pg_conn_cfg)

    # Unit-Test
    # news_to_ner = news_to_ner[:5000]

    print("Info: {} news selected to ner pipeline.".format(len(news_to_ner)))

    if len(news_to_ner) > 0:
        # 2. Ner extraction and normilization
        synonyms = get_norm_ners_from_news(news_to_ner)
        # 3. Entity linking
        synonyms = entity_linking(pg_conn_cfg, synonyms)
        # 4. Write results to database
        write_db_results_ner_pipeline(pg_conn_cfg, synonyms)
        # 5. Update default тук names if needed
        update_main_ner_names_and_types(pg_conn_cfg)


if __name__ == "__main__":
    ner_pipeline(PG_CONN_CFG)
