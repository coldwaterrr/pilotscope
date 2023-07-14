import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "common"))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "components"))

import unittest
import json

from DBController.SparkSQLController import SparkSQLController, SparkConfig, SUCCESS, FAILURE, SparkSQLDataSourceEnum
from Factory.DBControllerFectory import DBControllerFactory
from PilotConfig import PilotConfig
from PilotEnum import DatabaseEnum
from common.Index import Index
from pyspark.sql import SparkSession


class MyTestCase(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        datasource_type = SparkSQLDataSourceEnum.POSTGRESQL
        datasource_conn_info = {
            'host': 'localhost',
            'dbname': 'root',
            'user': 'root',
            'password': 'root'
        }
        self.config = SparkConfig(
            app_name="testApp",
            master_url="local[*]",
            datasource_type=datasource_type,
            datasource_conn_info=datasource_conn_info,
            other_configs={
                "spark.jars.packages": "org.postgresql:postgresql:42.6.0"
            }
        )
        self.config.db = "stats"
        self.config.set_db_type(DatabaseEnum.SPARK)
        self.table_name = "lero"
        self.db_controller: SparkSQLController = DBControllerFactory.get_db_controller(self.config)
        self.sql = "select * from badges limit 10;"
        self.table = "badges"
        self.column = "date"

    def test_get_hint_sql(self):
        self.db_controller.connect()
        # print(self.db_controller.connection.sparkContext.getConf().getAll())
        assert self.db_controller.get_hint_sql("spark.sql.autoBroadcastJoinThreshold", "1234") == SUCCESS
        assert self.db_controller.get_hint_sql("spark.execution.memory", "1234") == FAILURE
        self.db_controller.disconnect()

    def test_create_table(self):
        self.db_controller.connect()
        self.db_controller.create_table_if_absences("test_create_table", {"ID": 1, "name": "Tom"})
        self.db_controller.disconnect()

    def test_get_table_row_count(self):
        self.db_controller.connect()

        try:
            self.db_controller.get_table_row_count("test_create_table")
            assert False
        except Exception as e:
            assert (isinstance(e, RuntimeError))

        self.db_controller.create_table_if_absences("test_create_table", {})
        assert (self.db_controller.get_table_row_count("test_create_table") == 0)

        self.db_controller.disconnect()

    def test_insert(self):
        self.db_controller.connect()
        self.db_controller.create_table_if_absences("test_create_table", {"ID": 1, "name": "Tom"})
        self.db_controller.insert("test_create_table", {"ID": 1, "name": "Tom"})
        self.db_controller.disconnect()

    def test_set_and_recover_knobs(self):
        self.db_controller.connect()

        self.db_controller.write_knob_to_file(
            {"spark.sql.ansi.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "1234"})
        assert (self.db_controller.connection.conf.get("spark.sql.ansi.enabled") == 'true')
        assert (self.db_controller.connection.conf.get("spark.sql.autoBroadcastJoinThreshold") == '1234')

        self.db_controller.recover_config()
        assert (self.db_controller.connection.conf.get("spark.sql.ansi.enabled") == 'false')
        assert (self.db_controller.connection.conf.get("spark.sql.autoBroadcastJoinThreshold") == '10485760b')
        self.db_controller.disconnect()

    def test_plan_and_get_cost(self):
        self.db_controller.connect()

        self.db_controller.write_knob_to_file({
            "spark.sql.cbo.enabled": "true",
            "spark.sql.cbo.joinReorder.enabled": "true",
            "spark.sql.pilotscope.enabled": "true"
        })

        self.db_controller.create_table_if_absences("test_create_table", {"ID": 1, "name": "Tom"})
        self.db_controller.insert("test_create_table", {"ID": 2, "name": "Jerry"})
        self.db_controller.analyze_table_stats()

        sql = "SELECT * FROM test_create_table"
        print(json.dumps(self.db_controller.explain_logical_plan(sql), indent=2))

        print(self.db_controller.get_estimated_cost(sql))

        self.db_controller.disconnect()


if __name__ == '__main__':
    unittest.main()