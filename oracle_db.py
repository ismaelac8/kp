import os
import pandas as pd
import oracledb as db

try:
    from ..config import logging, COMMONS_LEVEL_LOGGING
except:
    import logging
    logging.basicConfig(format='%(asctime)s:%(levelname)s: %(message)s', level=logging.DEBUG)
    COMMONS_LEVEL_LOGGING = logging.DEBUG

logger = logging.getLogger(__name__)
logger.setLevel(COMMONS_LEVEL_LOGGING)

ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__)) + '/../data_to_load/db'


class OracleDB:

    __slots__ = ['user', 'password', 'dsn', 'connection', 'cursor', 'files']

    def __init__(self, user, password, host, port, service_name):
        self.user = user
        self.password = password
        self.dsn = f"{host}:{port}/{service_name}"
        self.connection = None
        self.cursor = None
        self._connect()
        logger.debug(f"Connected -{self.connection != None}")

    # ----------------------------------------------------------------------------------
    # Internal methods - Do not use outside
    def _connect(self):
        self.connection = db.connect(user=self.user, password=self.password, dsn=self.dsn)
        self.cursor = self.connection.cursor()
    
    def connected(self):
        return True if self.cursor != None else False

    def _format_string(self, value):
        """
        Converts the text string to the special type for the database

        Return : int
                | str (with '' -> example = 'example') 
                | timestamp -> SYS_EXTRACT_UTC(SYSTIMESTAMP)
        """
        result = value

        try:
            result = int(value)
        except ValueError:
            # STR - 
            result = f"'{value}'" if value != 'null' else value
            result = "SYS_EXTRACT_UTC(SYSTIMESTAMP)" if result.lower() == "'now'" else result

        return result

    def _convert_to_dict(self, rows: list):
        result = []
        
        columns = [desc[0] for desc in self.cursor.description]
        
        for row in rows:
            row_dict = {}
            for i in range(len(columns)):
                row_dict[columns[i]] = row[i]
            result.append(row_dict)
        
        return result

    def _load_sorted_files(self, type_con) -> dict:

        path_to_check = os.path.join(ABSOLUTE_PATH, type_con)

        temp_files = [file for file in os.listdir(path_to_check) if os.path.isfile(os.path.join(path_to_check, file))]
        temp = {int(name_file.split('__')[0]): os.path.join(path_to_check, name_file) for name_file in temp_files}

        return dict(sorted(temp.items()))
    
    # ----------------------------------------------------------------------------------
    # Publics Methods
    def close(self):
        self.cursor.close()
        self.connection.close()
    
    def load_massive_data(self):

        for count, each_file in self.files.items():
            table_name = os.path.basename(each_file).split('__')[-1].split('.')[0]
            logger.info(f'Loading {count} - {table_name}')

            data_info = pd.read_csv(each_file, sep=';',na_values='')
            data_info = data_info.fillna('').astype(str)
            to_write = data_info.to_dict(orient="records")
            
            tmp_result = all([self.add_data(table_name, each_record) for each_record in to_write])
            if not tmp_result:
                return False
            
            logger.info(f"File {table_name}.csv loaded")

        return True
                
    def select(self, table_name: str, *argv, column_name: list = None) -> list:
        """
        The query conditions will be passed by the argv
        It is mandatory. If we want to use argv it must be a dictionaries

        Example:
            { "key": "column_name_1", "value": value_column_1 }, { "key": "column_name_2", "value": value_column_2 }, ...
        """
        query = f"SELECT {', '.join([item.upper() for item in column_name]) if column_name else '*'} FROM {table_name.upper()}"
        try:
            if conditions := " AND ".join([f"{item.get('key').upper()}={self._format_string(item.get('value'))}"for item in argv]):
                query += f" WHERE {conditions}"
        except:
            logger.error("You have not used *argv correctly")
        logger.debug(f"Query -> {query}")

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return self._convert_to_dict(rows)
    
    def select_by_query(self, table_name: str, condition_query: str, column_name: list = None) -> list:
        
        query = f"SELECT {', '.join([item.upper() for item in column_name]) if column_name else '*'} FROM {table_name.upper()} WHERE {condition_query}"
        logger.debug(f"Query -> {query}")

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return self._convert_to_dict(rows)

    def add_data(self, table_name: str, values = None, **kwargs) -> bool:
        is_added = False
    
        if values and isinstance(values, list):
            dict_with_values = {item.get("key"): item.get("value") for item in values } 
        elif values and isinstance(values, dict):
            dict_with_values = values
        else:
            dict_with_values = kwargs
        
        if dict_with_values == None:
            return is_added
        
        column_name = ", ".join([f"{key.upper()}" for key in dict_with_values.keys()])
        values_name = ", ".join([f"{self._format_string(value)}" for value in dict_with_values.values()])
        query = f"INSERT INTO {table_name.upper()} ({column_name}) VALUES ({values_name})"

        logger.debug(f"Query -> {query}")
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            is_added = True
        except Exception as exp:
            logger.error(f"Query -> {query}")
            logger.error(exp)

        return is_added

    def update_data(self, table_name: str, value_to_update: dict, **kwargs) -> bool:
        is_updated = False
        
        if value_to_update == None:
            return is_updated

        query = f"UPDATE {table_name.upper()} SET "
        values = ", ".join([f"{key.upper()}={self._format_string(value)}"for key, value in value_to_update.items()])
        query += values
        if conditions := " AND ".join([f"{key.upper()}={self._format_string(value)}"for key, value in kwargs.items()]):
            query += f" WHERE {conditions}"

        logger.debug(f"Query -> {query}")
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            is_updated = True

        except Exception as exp:
            logger.error(exp)

        return is_updated

    def remove_data(self, table_name: str, **kwargs) -> bool:
        is_removed = False
        
        query = f"DELETE FROM {table_name.upper()}"
        if conditions := " AND ".join([f"{key.upper()}={self._format_string(value)}"for key, value in kwargs.items()]):
            query += f" WHERE {conditions}"
        else:
            # Never DROP ALL
            return is_removed
        
        logger.debug(f"Query -> {query}")
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            is_removed = True

        except Exception as exp:
            logger.error(exp)

        return is_removed

    def remove_all_data(self, table_name: str) -> bool:
        is_removed = False
        
        query = f"DELETE FROM {table_name.upper()}"
        logger.debug(f"Query -> {query}")
        
        try:
            self.cursor.execute(query)
            self.connection.commit()
            is_removed = True
        except Exception as exp:
            logger.error(exp)

        return is_removed
    
    def execute_sql(self, query, parameter_list=None):
        with self.connection.cursor() as cursor:
            if parameter_list:
                cursor.executemany(query, parameter_list)
            else:
                cursor.execute(query)
            try:
                self.connection.commit()
            except Exception as e:
                logger.error(e)
    
    def create_table(self, table_name: str, columns: list):
        if self.table_exists(table_name):
            logger.debug(f"Table {table_name} exists")
            return
        cols = ", ".join(
            [f"{item['key'].upper()} {item['value']}" for item in columns]
        )
        query = f"CREATE TABLE {table_name} ({cols})"
        logger.debug(f"Query -> {query}")
        self.execute_sql(query)

    def delete_table(self, table_name: str):
        query = f"DROP TABLE {table_name}"
        logger.debug(f"Query -> {query}")
        self.execute_sql(query)

    def table_exists(self, table_name: str):
        rows = self.select(
            "ALL_TABLES",
            {"key": "TABLE_NAME", "value": table_name},
            column_name = ["TABLE_NAME"]
        )
        return len(rows) == 1 and rows[0]["TABLE_NAME"] == table_name

    def count_records(self, table_name: str):
        rows = self.select(
            table_name,
            column_name = ["COUNT (*)"]
        )
        return int(rows[0]['COUNT(*)'])
    
    def add_data_batch(self, data_dict=None):
        if not data_dict:
            return
        for table_name, file_name in data_dict.items():
            data_info = pd.read_csv(file_name, sep=';', na_values="")
            to_write = data_info.to_dict(orient="records")
            columns = ", ".join(to_write[0].keys())
            values = ", ".join([
                "(" + ", ".join([
                    "'" + str(value) + "'" for key, value in record.items()
                    ]) + ")"
                for record in to_write
            ])
            query = f"INSERT INTO {table_name} ({columns}) VALUES {values};"
            logger.debug(query)
            self.execute_sql(query)



   
# ----------------------------------------------------------------------------------
# To Test Class
if __name__ == "__main__":

    user = "GDSPWSC"
    pwd = "GDSPWSC"
    host = "localhost"
    port = 1521
    service_name = "XEPDB1"

    db = OracleDB(user=user, password=pwd, host=host, port=port, service_name=service_name, type_con="gdspwsc")
    
    db.remove_all_data("t_site")
    db.remove_all_data("t_wsc_customer")
    db.remove_all_data("t_wsc_customer_service_profile")
    db.remove_all_data("t_wsc_web_service")

    # db.add_data("t_site", site_id=1, site_name="prueba01", site_type="U", site_url="localhost:9091/")
    # result = db.select("t_site", {"key":"site_id","value":1},{"key":"site_name","value":"prueba01"})
    # result = db.select("t_site", {"key":"site_id","value":1}, {"key":"site_name","value":"prueba01"}, column_name=['site_id', 'updated_at', 'site_name'])
    # db.update_data("t_site", {"site_name":"nuevo", "site_type":"Z", "site_url":"laa"}, site_id=1)
    # db.remove_data("t_site", site_id=1)
    # db.remove_all_data("t_site")

    # if result:
    #     logger.info(result)

    # db.remove_all_data("t_site")
    # db.remove_all_data("t_wsc_customer")
    # db.remove_all_data("t_wsc_customer_service_profile")
    # db.remove_all_data("t_wsc_web_service")

    logger.info(f"Load massive data completed? -> {db.load_massive_data()}")

    db.close()