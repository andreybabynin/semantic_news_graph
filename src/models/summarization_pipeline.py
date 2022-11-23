"""Summarization pipeline script (run from cli)"""

import os
import re
import datetime
from src.common_funcs import safe_pg_read_query, safe_pg_execute_values
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM


# Hyperparameters
MODEL_CFG = {
    "name": "IlyaGusev/rut5_base_headline_gen_telegram",
    "version": "v2022.10.19",
    "text_prefix": "",
    "tokenizer_kwargs": {
        "max_length": 600,
        "padding": True,
        "truncation": True,
        "add_special_tokens": True,
    },
    "generate_kwargs": {},
}

PG_CONN_CFG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "port": 5432,
    "host": "postgres",
}
with open(os.environ.get("POSTGRES_PASSWORD_FILE"), "r") as f:
    PG_CONN_CFG["password"] = f.readlines()[0]


def inference(
    texts, model, tokenizer, tokenizer_kwargs={}, generate_kwargs={}, num_beams=5
):

    input_ids = tokenizer(texts, return_tensors="pt", **tokenizer_kwargs)

    output_ids = model.generate(**input_ids, num_beams=num_beams, **generate_kwargs)

    summary = tokenizer.batch_decode(
        output_ids,
        skip_special_tokens=True,
    )
    return summary


def summarization_pipeline():

    # select news to summarisation (news without summary)
    query = """
    SELECT id_news, news_text, news_source
    FROM news
    WHERE id_news NOT IN (SELECT id_news FROM news_summary);
    """
    # list of tuples(id_news, news_text, summary_text)
    news_to_summary = safe_pg_read_query(PG_CONN_CFG, query)

    current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # get summarization model
    tokenizer = AutoTokenizer.from_pretrained(MODEL_CFG["name"])
    model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_CFG["name"])

    # processing one news at a time (do not use batch > 1 to save memory)
    result = []
    for news_id, text, source in news_to_summary:

        # clean text before input to model (custom by source or for all)
        # if source = "src1":
        #    text = re.sub(r"[^\x20-\xFFа-яА-ЯёЁ№\n]+|__|\*\*", '', text)
        text = re.sub(r"[^\x20-\xFFа-яА-ЯёЁ№\n]+|__|\*\*", "", text)

        # task: add if news is one simple sentence, then summary = clean text

        summary = inference(
            [text],
            model,
            tokenizer,
            MODEL_CFG["tokenizer_kwargs"],
            MODEL_CFG["generate_kwargs"],
        )[0]

        # summary by model is incorrect if summary length > input text length
        # in this case, use the original text
        if len(summary) >= len(text):
            summary = text

        result.append(
            (
                news_id,
                current_date,
                " ".join([MODEL_CFG["name"], MODEL_CFG["version"]]),
                summary,
            )
        )

    del model, tokenizer

    # save summarization results in database
    query = """
        INSERT INTO news_summary(id_news, date_generated,
                                 model_version, summary_text)
        VALUES %s
    """
    safe_pg_execute_values(PG_CONN_CFG, query, result)


if __name__ == "__main__":
    summarization_pipeline()
