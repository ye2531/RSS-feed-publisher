CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    published TIMESTAMP NOT NULL,
    title VARCHAR(200) NOT NULL,
    link VARCHAR(300) UNIQUE NOT NULL,
    article_text VARCHAR NOT NULL,
    dag_run_time DATE NOT NULL
);