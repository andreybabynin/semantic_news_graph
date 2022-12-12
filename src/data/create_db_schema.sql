/* 
db_news schema v0.1
SQL script to create postgres db_news schema.
*/

--Create news table
CREATE TABLE news (
    id_news SERIAL NOT NULL PRIMARY KEY,
    news_text TEXT NOT NULL,
    news_date timestamp NOT NULL,
    news_source TEXT NOT NULL,
    date_collected timestamp NOT NULL
    );

--Create ner_types table
CREATE TABLE ner_types (
    id_ner_type SERIAL NOT NULL PRIMARY KEY,
    ner_type VARCHAR(30) NOT NULL
    );

--Create ner table
CREATE TABLE ner (
    id_ner SERIAL NOT NULL PRIMARY KEY,
    ner_name TEXT NOT NULL,
    qid_wikidata TEXT,
    name_is_custom INTEGER,
    id_ner_type INTEGER,
    FOREIGN KEY (id_ner_type) REFERENCES ner_types (id_ner_type);
    );

--Create news_links table
CREATE TABLE news_links (
    id_news_links SERIAL NOT NULL PRIMARY KEY,
    id_news INTEGER NOT NULL,
    id_ner INTEGER,
    FOREIGN KEY (id_news) REFERENCES news (id_news) ON DELETE CASCADE,
    FOREIGN KEY (id_ner) REFERENCES ner (id_ner) ON DELETE CASCADE
    );

--Create ner_synonims table
CREATE TABLE ner_synonyms (
    id_synonim SERIAL NOT NULL PRIMARY KEY,
    id_ner INTEGER NOT NULL,
    ner_synonym TEXT NOT NULL,
    name_for_match TEXT,
    FOREIGN KEY (id_ner) REFERENCES ner (id_ner) ON DELETE CASCADE
    );

--Create models table
CREATE TABLE model_types (
    id_model_type SERIAL NOT NULL PRIMARY KEY,
    model_type VARCHAR(30) NOT NULL
    );

--Create models table
CREATE TABLE model_stages (
    id_model_stage SERIAL NOT NULL PRIMARY KEY,
    model_stage VARCHAR(30) NOT NULL
    );

--Create models table
CREATE TABLE models (
    id_model SERIAL NOT NULL PRIMARY KEY,
    id_model_type INTEGER NOT NULL,
    id_model_stage INTEGER NOT NULL,
    model_name TEXT NOT NULL,
    model_ver VARCHAR(30),
    date_added timestamp  NOT NULL,
    model_settings JSONB,
    FOREIGN KEY (id_model_type) REFERENCES model_types (id_model_type),
    FOREIGN KEY (id_model_stage) REFERENCES model_stages (id_model_stage)
    );

--Create news_summary table
CREATE TABLE news_summary (
    id_summary SERIAL NOT NULL PRIMARY KEY,
    id_news INTEGER NOT NULL,
    date_generated timestamp NOT NULL,
    summary_text TEXT NOT NULL,
    id_model INTEGER NOT NULL,
    FOREIGN KEY (id_news) REFERENCES news (id_news) ON DELETE CASCADE
    FOREIGN KEY (id_model) REFERENCES models (id_model)
    );

--Create synonims_stats table
CREATE TABLE synonyms_stats (
    id_synonyms_stats SERIAL NOT NULL PRIMARY KEY,
    id_synonim INTEGER NOT NULL,
    news_count INTEGER NOT NULL,
    date_processed timestamp NOT NULL,
    id_model INTEGER NOT NULL,
    id_ner_type INTEGER,
    FOREIGN KEY (id_synonim) REFERENCES ner_synonyms (id_synonim) ON DELETE CASCADE,
    FOREIGN KEY (id_model) REFERENCES models (id_model),
    FOREIGN KEY (id_ner_type) REFERENCES ner_types (id_ner_type)
    );

--Add default model stages
INSERT INTO model_stages(model_stage)
VALUES ('production'),
        ('archived');

--Add default model types
INSERT INTO model_types(model_type)
VALUES ('summarization'),
        ('ner');

--Add default ner types
INSERT INTO ner_types(id_ner_type, ner_type)
VALUES (1, 'PER'),
        (2, 'LOC'),
        (3, 'ORG'),
        (4, 'MISC');

--for fuzzy search by SIMILARITY
CREATE EXTENSION pg_trgm;
