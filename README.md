# RSS feed publisher

Automates the process of monitoring and capturing updates from Google Alerts' RSS feed.

## Description

 This project utilizes Apache Airflow to periodically check Google Alerts' RSS feed for updates. When updates are detected, new articles are downloaded, preprocessed, stored in a PostgreSQL database, and published to a Telegram channel.

## Table of Contents
- [RSS feed publisher](#rss-feed-publisher)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
  - [Configuration](#configuration)
  - [Execution](#execution)
  - [License](#license)

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Docker: [Installation Guide](https://docs.docker.com/get-docker/)
- Docker Compose: [Installation Guide](https://docs.docker.com/compose/install/)

## Getting Started
To get started with the project, follow these steps:

1. Clone this repository: `git clone https://github.com/ye2531/RSS-feed-publisher.git`
2. Navigate to the project directory: `cd RSS-feed-publisher`

## Configuration 

Before running the project, you need to set up the following configurations:

1. **PostgreSQL Connection:**
   - Create a PostgreSQL connection in Apache Airflow with the name `postgres_default` and appropriate credentials.

2. **Telegram Token:**
   - Set up an Apache Airflow Variable named `TELEGRAM_BOT_TOKEN` with your Telegram bot token value.

3. **Environment variables:**
   
   Inside of `settings.py` file set
   - `RSS_FEED_URL` to the URL of the RSS feed you want to fetch and process
   - `NAMED_ENTITY` to the named entity for which updates are to be tracked. This could be a person, organization, product, etc.
   - `CHAT_ID` to the unique identifier of the Telegram channel where you want to send notifications.

## Execution

1. Navigate to the project directory: `cd RSS-feed-publisher`
2. Start the containers: `docker compose up`
3. Access the Airflow UI in your web browser (default: http://localhost:8080) to manage and trigger your DAGs.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.