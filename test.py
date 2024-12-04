from pilotscope.Dataset.ImdbTinyDataset import ImdbTinyDataset
from pilotscope.Dataset.StatsTinyDataset import StatsTinyDataset
from pilotscope.PilotConfig import PostgreSQLConfig
from pilotscope.PilotEnum import DatabaseEnum
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor

# configure database settings 
db_port = "5432"  # database server port (default: "5432")
db_user = "zbw"  # database user name (default: "pilotscope")
db_user_pwd = "pilotscope"  # database user password (default: "pilotscope")
pg_bin_path = "/home/zbw/PilotScopePostgreSQL/bin"  # database bin path, i.e. $PG_PATH/bin (default: None)
pg_data_path = "/home/zbw/db_data"  # database data path, i.e. $PG_DATA  (default: None)

db_config = PostgreSQLConfig(db_port=db_port, db_user=db_user, db_user_pwd=db_user_pwd)

# Please ensure that your database and pilotscope Core are in the same machine with the same user.
db_config.enable_deep_control_local(pg_bin_path, pg_data_path)
 
# # load stats_tiny
# ds = StatsTinyDataset(DatabaseEnum.POSTGRESQL, created_db_name="stats_tiny")
# ds.load_to_db(db_config)

# # load imdb_tiny
# ds = ImdbTinyDataset(DatabaseEnum.POSTGRESQL, created_db_name="imdb_tiny")
# ds.load_to_db(db_config)

# You can also instantiate a PilotConfig for other DBMSes. e.g. 
# config:PilotConfig = SparkConfig()
db_config.db = "imdb_tiny"
# Configure PilotScope here, e.g. changing the name of database you want to connect to.

# sql = "select count(*) from votes as v, badges as b, users as u where u.id = v.userid and v.userid = b.userid and u.downvotes>=0 and u.downvotes<=0"
# sql = '''
# select count(*) from votes as v, badges as b, users as u where u.id = v.userid and v.userid = b.userid and u.downvotes>=0 and u.downvotes<=0
# '''
sql = "select count(*) from movie_companies"
data_interactor = PilotDataInteractor(db_config)
# data_interactor.pull_estimated_cost()
# data_interactor.pull_subquery_card()
# data_interactor.pull_execution_time()
# data_interactor.pull_physical_plan()
data_interactor.pull_record()
data = data_interactor.execute(sql)
print(data)

