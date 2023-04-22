import psycopg2
import os
import re
import pandas as pd
import logging


def db_connection(host: str, dbname: str, user: str, password: str):
    """Connect to Postgres db
    Args:
        host (str): database host
        dbname (str): database name
        user (str): username for the database
        password (str): password for the database
    Returns: database object
    """
    logging.info("Connecting to database...")
    conn_string = f"host='{host}' dbname='{dbname}' user='{user}' password='{password}'"
    conn = psycopg2.connect(conn_string)

    return conn


def create_tables(conn, drop_table=True):
    """Create schema and tables before loading data
    Args:
        conn: db connection
        drop_table (bool): drop old tables
    """
    conn.autocommit = True
    cursor = conn.cursor()

    sql = """CREATE SCHEMA IF NOT EXISTS etl"""
    cursor.execute(sql)

    if drop_table:
        logging.info("Dropping tables if they exists")
        sql = """DROP TABLE IF EXISTS etl.ad_group, etl.ad_click, etl.ad_search, etl.ad_type, etl.customer"""
        cursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS etl.customer
            (
                account_number varchar primary key,
                customer       varchar,
                account_name   varchar,
                account_status varchar
            )"""
    cursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS etl.ad_group
            (
                ad_group_id     varchar primary key,
                campaign_name   varchar,
                campaign_status varchar,
                ad_group        varchar,
                ad_group_status varchar

            );"""
    cursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS etl.ad_type
            (
                ad_id             varchar primary key,
                ad_description    varchar,
                ad_distribution   varchar,
                ad_status         varchar,
                ad_title          varchar,
                ad_type           varchar,
                tracking_template varchar,
                custom_parameters varchar,
                final_mobile_url  varchar,
                final_url         varchar,
                display_url       varchar,
                final_app_url     varchar,
                destination_url   varchar
            );"""
    cursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS etl.ad_search
            (
                click_id             serial primary key,
                account_number       varchar,
                top_vs_other         varchar,
                gregorian_date       date,
                device_type          varchar,
                device_os            varchar,
                delivered_match_type varchar,
                bidmatchtype         varchar,
                language             varchar,
                network              varchar,
                currency_code        varchar
            );"""
    cursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS etl.ad_click
            (
                click_id        serial primary key,
                ad_id           varchar,
                ad_group_id     varchar,
                account_number  varchar,
                impressions     numeric,
                clicks          numeric,
                spend           numeric,
                avg_position    numeric,
                conversions     numeric,
                assists         numeric
            );"""
    cursor.execute(sql)


def sanitize_data(raw_path: str) -> str:
    """Sanitize data. Remove unwanted columns and varcharacters
    Args:
        raw_path (str): path of the raw data file
    Returns:
        sanitized_path (str): path of the sanitized file
    """

    logging.info("Sanitizing data")
    current_dir = "/opt/airflow/dags/app"  # volume as defined in docker-compose.yaml
    sanitized_path = os.path.join(current_dir, "sanitized_data.csv")

    with open(raw_path, "r", encoding="utf-8") as fin:
        with open(sanitized_path, "w", encoding="utf-8") as fout:
            for line in fin:
                if line.split(",")[0].strip('"') == "Gregorian date":
                    line = line.lower().replace(" ", "_").replace(".", "")
                    fout.write(line)
                if re.match(r'^"\d+-\d+-\d+', line) != None:
                    fout.write(line)
    return sanitized_path


def split_data(file_path: str, file_dict: dict):
    """Split sanitized data into multiple table as defined in the star schema
    Args:
        file_path (str): path of the sanitized file
    """

    logging.info("Splitting raw data")
    df = pd.read_csv(file_path)
    # Clean square brackets from id fields
    df["ad_group_id"] = df["ad_group_id"].str.replace("[", "").str.replace("]", "")
    df["ad_id"] = df["ad_id"].str.replace("[", "").str.replace("]", "")

    customer_df = df[["account_number", "customer", "account_name", "account_status"]]
    customer_df = customer_df.drop_duplicates()  # drop duplicate customer records
    customer_df.to_csv(file_dict["customer"], index=False)

    ad_group_df = df[
        [
            "ad_group_id",
            "campaign_name",
            "campaign_status",
            "ad_group",
            "ad_group_status",
        ]
    ]

    ad_group_df = ad_group_df.drop_duplicates()
    ad_group_df.to_csv(file_dict["ad_group"], index=False)

    ad_type_df = df[
        [
            "ad_id",
            "ad_description",
            "ad_distribution",
            "ad_status",
            "ad_title",
            "ad_type",
            "tracking_template",
            "custom_parameters",
            "final_mobile_url",
            "final_url",
            "display_url",
            "final_app_url",
            "destination_url",
        ]
    ]

    ad_type_df = ad_type_df.drop_duplicates()
    ad_type_df.to_csv(file_dict["ad_type"], index=False)

    index_df = df[
        [
            "ad_id",
            "ad_group_id",
            "account_number",
            "top_vs_other",
            "gregorian_date",
            "device_type",
            "device_os",
            "delivered_match_type",
            "bidmatchtype",
            "language",
            "network",
            "currency_code",
            "impressions",
            "clicks",
            "spend",
            "avg_position",
            "conversions",
            "assists",
        ]
    ]
    index_df["click_id"] = index_df.index + 1

    ad_search_df = index_df[
        [
            "click_id",
            "account_number",
            "top_vs_other",
            "gregorian_date",
            "device_type",
            "device_os",
            "delivered_match_type",
            "bidmatchtype",
            "language",
            "network",
            "currency_code",
        ]
    ]
    ad_search_df = ad_search_df.sort_values(by=["click_id"])
    ad_search_df.to_csv(file_dict["ad_search"], index=False)

    ad_click_df = index_df[
        [
            "click_id",
            "ad_id",
            "ad_group_id",
            "account_number",
            "impressions",
            "clicks",
            "spend",
            "avg_position",
            "conversions",
            "assists",
        ]
    ]
    ad_click_df = ad_click_df.sort_values(by=["click_id"])
    ad_click_df.to_csv(file_dict["ad_click"], index=False)

    os.remove(file_path)

    return file_dict


def file_to_table(file_dict: dict, conn):
    """Copy csv file to Postgres table
    Args:
        conn: database connection
        file_dict (Dict[str, str]): list of file paths
    """

    logging.info("Loading csv files into database")
    cursor = conn.cursor()

    for table, file in file_dict.items():
        logging.info(f"Copying {file} into {table}")

        with open(file, "r") as f:
            header = next(f)
            print(header)
            cursor.copy_expert(
                f"COPY etl.{table} FROM STDIN WITH CSV HEADER DELIMITER ','", f
            )
        os.remove(file)
    conn.commit()
    conn.close()
