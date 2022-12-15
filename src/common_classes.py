"""Module for common project classes
"""
import re
import requests
import datetime
import pymorphy2
from src.common_funcs import get_wikidata_qid


class SynNamedEntity:
    def __init__(self, name_syn) -> None:
        self.name_syn = name_syn
        self.ntypes = []
        self.news_ids = set()
        self.in_synonym_table = None
        self.id_ner = None
        self.wiki_qid = None
        self.name_for_match = None

    def __repr__(self):
        return f"(name_syn='{self.name_syn}')"

    def __str__(self):
        return f"(name_syn='{self.name_syn}')"

    def __eq__(self, other):
        if not isinstance(other, SynNamedEntity):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.name_syn == other.name_syn

    def __hash__(self):
        # necessary for instances to behave sanely in dicts and sets.
        return hash((self.name_syn,))

    def ntype(self):
        # get ntype as mode of list ntypes
        return (
            max(set(self.ntypes), key=self.ntypes.count) if self.ntypes != [] else None
        )


class SynNamedEntities:

    # ntypes name: id (in database)
    ntype_ids = {"PER": 1, "LOC": 2, "ORG": 3, "MISC": 4}

    # for get name_for_match
    re_match_to_hyphen = re.compile(r"\s*-\s*")
    re_match_to_whitespace = re.compile(r"\s+")
    re_match_remove = re.compile(r"[^a-zA-Zа-яА-Я0-9 -]+")

    @staticmethod
    def gen_name_for_match(text, morph):
        """
        Generate name for match (without punctuation and each word
        nodmalized by pymorphy2).

        Args:
            text (str): text/name for normalize for match
            morph: instance of pymorphy2.MorphAnalyzer()
        Returns:
            normalized for match name/text
        """
        clean_text = SynNamedEntities.re_match_remove.sub(
            "",
            SynNamedEntities.re_match_to_whitespace.sub(
                " ", SynNamedEntities.re_match_to_hyphen.sub("-", text)
            ),
        )
        return " ".join(
            [morph.parse(word)[0].normal_form for word in clean_text.split()]
        )

    def __init__(self) -> None:
        self._ents = {}
        self.news_without_ents = []
        self.count_without_id_ner = 0

    def add_ents_from_one_news(self, news_id, ents):
        """
        news_id: id news in db
        ents: tuple of tuples((norm ner01, ner_type01),
                              (norm ner02, ner_type02), ...)
        """
        if len(ents) > 0:
            for ent in ents:
                if ent[0] not in self._ents:
                    self._ents[ent[0]] = SynNamedEntity(ent[0])
                    self.count_without_id_ner += 1

                self._ents[ent[0]].ntypes.append(ent[1])
                self._ents[ent[0]].news_ids.add(news_id)
        else:
            self.news_without_ents.append(news_id)

    def add_ents_from_news(self, news):
        """
        news: list of tuple(id_news, tuple of tuples((norm ner01, ner_type01),
                                                    (norm ner02, ner_type02), ...))
        """
        for news_id, ents in news:
            self.add_ents_from_one_news(news_id, ents)

    def search_in_synonym_table(self, db_syn_name2id, db_syn_match2id):
        """
        Search (and get id_ner) synonyms in local db ner_synonym table:
            - first, by ner_synonym
            - second (if not found by ner_synonym), by name_for_match
        Args:
            db_syn_name2id: dict {'ner_synonym01': id_ner01, ...}
            db_syn_match2id: dict {'name_for_match01': id_ner01, ...}

        Returns:
            None, but set id_ner in ._ents for found entities
        """
        morph = pymorphy2.MorphAnalyzer()
        for ent in self._ents.values():
            if ent.name_syn in db_syn_name2id:
                ent.in_synonym_table = True
                ent.id_ner = db_syn_name2id[ent.name_syn]
                self.count_without_id_ner -= 1
            else:
                ent.in_synonym_table = False
                ent.name_for_match = SynNamedEntities.gen_name_for_match(
                    ent.name_syn, morph
                )
                if ent.name_for_match in db_syn_match2id:
                    ent.id_ner = db_syn_match2id[ent.name_for_match]
                    self.count_without_id_ner -= 1

    def search_wikidata_qid(self):
        """
        Query to wikidata for get qid for entities without id_ner
        """
        if self.count_without_id_ner > 0:
            session = requests.Session()
            for ent in self._ents.values():
                if ent.id_ner is None:
                    ent.wiki_qid = get_wikidata_qid(ent.name_syn, session)

    def match_by_wikidata_qid(self, db_ner_qid2id):
        """
        Match entity by qid_wikidata in ner table. Fill .id_ner for matched ents.
        Args:
            db_ner_qid2id: dict of ents with exist qid_wikidata
                    {"qid_wikidata01": id_ner01, ...}
        """
        if self.count_without_id_ner > 0:
            for ent in self._ents.values():
                if (
                    ent.id_ner is None
                    and ent.wiki_qid is not None
                    and ent.wiki_qid in db_ner_qid2id
                ):
                    ent.id_ner = db_ner_qid2id[ent.wiki_qid]
                    self.count_without_id_ner -= 1

    def match_by_ner_table(self, db_ner_name2id, db_ner_qid2id):
        """
        Match entity by ner_name and qid_wikidata in ner table. Fill .id_ner
        for matched ents.

        Args:
            db_ner_name2id: dict {ner_name01: id_ner01, ...}
            db_ner_qid2id: dict {qid_wikidata01: id_ner01, ...}
        """
        for ent in self._ents.values():
            if ent.id_ner is None and ent.name_syn in db_ner_name2id:
                ent.id_ner = db_ner_name2id[ent.name_syn]
                self.count_without_id_ner -= 1

        if self.count_without_id_ner > 0:
            self.match_by_wikidata_qid(db_ner_qid2id)

    def get_rows_to_ner_table(self):
        """
        Generate list of tuples to insert new ents in db ner table.
        For several synonyms with the same qid, we add only first entity
        (the main name for such entities will be revised at the last step
        of the ner-pipeline, based on the frequency of occurrence of synonyms).

        Returns:
            list of tuple(ner_name, id_ner_type, qid_wikidata)
        """
        if self.count_without_id_ner > 0:
            rows_to_ner_table = []
            added_qid = []
            for ent in self._ents.values():
                if ent.id_ner is None and ent.wiki_qid not in added_qid:
                    rows_to_ner_table.append(
                        (ent.name_syn, self.ntype_ids[ent.ntype()], ent.wiki_qid)
                    )
                    if ent.wiki_qid is not None:
                        added_qid.append(ent.wiki_qid)

        else:
            rows_to_ner_table = []

        return rows_to_ner_table

    def get_rows_to_synonyms_table(self):
        """
        Generate list of tuples to insert new ents in db ner_synonym table.

        Returns:
            list of tuple(id_ner, ner_synonym, name_for_match)
        """
        return [
            (ent.id_ner, ent.name_syn, ent.name_for_match)
            for ent in self._ents.values()
            if not ent.in_synonym_table
        ]

    def get_rows_to_news_links_table(self):
        """
        Generate list of tuples to insert new rows in db news_links table.

        Returns:
            list of tuple(id_news, id_ner)
        """
        rows_to_news_links = [(id_news, None) for id_news in self.news_without_ents]

        for ent in self._ents.values():
            rows_to_news_links.extend(
                [(id_news, ent.id_ner) for id_news in list(ent.news_ids)]
            )

        return list(set(rows_to_news_links))

    def get_rows_to_synonyms_stats(self, syn_ids_dict):
        """
        Generate list of tuples to insert new rows in db news_links table.
        Args:
            syn_ids_dict: dict('ner_synonym01': id_synonim01, ...)
        Returns:
            list of tuple(id_synonim, news_count, date_processed, id_model, id_ner_type)
        """
        date_proc = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        id_model = 2  # task: change to get id_model by type/stage
        return [
            (
                syn_ids_dict[ent.name_syn],
                len(ent.news_ids),
                date_proc,
                id_model,
                self.ntype_ids[ent.ntype()],
            )
            for ent in self._ents.values()
        ]
