from dags.app.etl import (
    db_connection,
    sanitize_data,
    split_data,
    create_tables,
    file_to_table,
)

# Create db connection
conn = db_connection(
    host="postgres", dbname="airflow", user="airflow", password="airflow"
)
current_dir = (
    "/opt/airflow/dags/app"  # volume directory as defined in docker-compose.yaml
)
# define table names and their respective csv file
file_dict = {
    "customer": f"{current_dir}/customer.csv",
    "ad_group": f"{current_dir}/ad_group.csv",
    "ad_type": f"{current_dir}/ad_type.csv",
    "ad_search": f"{current_dir}/ad_search.csv",
    "ad_click": f"{current_dir}/ad_click.csv",
}


def prepare_tables():
    create_tables(conn)


def transform_data():
    sanitized_path = sanitize_data(raw_path="/opt/airflow/dags/app/raw_data.csv")
    split_data(sanitized_path, file_dict)


def load_data():
    file_to_table(file_dict, conn)
