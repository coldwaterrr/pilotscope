import psycopg2
from pilotscope.PilotConfig import PostgreSQLConfig
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor

# configure database settings 
db_port = "5432"  # database server port (default: "5432")
db_user = "zbw"  # database user name (default: "pilotscope")
db_user_pwd = "pilotscope"  # database user password (default: "pilotscope")
pg_bin_path = "/home/zbw/PilotScopePostgreSQL/bin"  # database bin path, i.e. $PG_PATH/bin (default: None)
pg_data_path = "/home/zbw/db_data"  # database data path, i.e. $PG_DATA  (default: None)


# 数据库连接参数
print("connecting...")
db_config = PostgreSQLConfig(db_port=db_port, db_user=db_user, db_user_pwd=db_user_pwd)
db_config.db = "stats_tiny"

# Please ensure that your database and pilotscope Core are in the same machine with the same user.
db_config.enable_deep_control_local(pg_bin_path, pg_data_path)
print("connect success")

sql = "Explain analyze select count(*) from badges as b, users as u where b.userid= u.id and u.upvotes>=0"
data_interactor = PilotDataInteractor(db_config)
data_interactor.pull_record()
data = data_interactor.execute(sql)
print(data)