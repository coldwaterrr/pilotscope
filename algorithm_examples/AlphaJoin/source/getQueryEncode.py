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


# Please ensure that your database and pilotscope Core are in the same machine with the same user.
db_config.enable_deep_control_local(pg_bin_path, pg_data_path)
print("connect success")


# 获取对应关系
def getshorttolong(file_path, output_file_path):
    db_config.db = "imdb_tiny"
    # Read lines from a text file and split each line by spaces
    # file_path = "/home/zbw/PilotScopeCore/pilotscope/Dataset/StatsTiny/stats_train_time2int.txt"
    
    with open(file_path, 'r') as file:
        lines = file.readlines()

    alias_mapping = {}
    

    for line in lines:
        words = line.strip().split()
        i = 0
        while i < len(words):
            if words[i] == 'AS' and words[i-1].split('(')[0] != 'MIN':
                original_name = words[i - 1]
                alias = words[i + 1].split(',')[0]
                print(alias,end = ' ')
                print(original_name, end = '\n')
                if alias not in alias_mapping:
                    alias_mapping[alias] = original_name
                i += 2  # Skip the next word as it is already processed
            else:
                i += 1

    with open(output_file_path, 'w') as output_file:
        for alias, original_name in alias_mapping.items():
            output_file.write(f"{alias} : {original_name}\n")

# 将选择率存到文件里
def get_selectivity(file_path, output_file_path=None):
    db_config.db = "imdb_tiny"
    table_name = set()
    table_rowcount = dict()
    with open(file_path, 'r') as file:
        lines = file.readlines()

    for line in lines:
        words = line.strip().split(':')
        # print(words)
        table_name.add(words[0])

    # print(table_name)

    data_interactor = PilotDataInteractor(db_config)
    data_interactor.pull_record()


    # print(table_name,end='\n')
    # print(len(table_name))

    # 获取每个表的总行数
    for table in table_name:
        # print(table,end = "\n")
        # print(type(table))
        sql = "select count(*) from %s" % (table)
        data_interactor.pull_record()
        data = data_interactor.execute(sql)
        table_rowcount[table.strip()] = data.records['count'][0]  # 每个表的具体行数
        # print(data.records['count'][0])
    
    # print(table_rowcount)
    # sql='''
    # select n_distinct from pg_stats where tablename = '%s' and attname = '%s'
    # ''' % ("info_type", "info")
    # data_interactor.pull_record()
    # data = data_interactor.execute(sql)
    # print(data)

    # 获取每条语句涉及到的列
    attr = set()
    querypath = "pilotscope/Dataset/Imdb/job_train_ascii.txt"
    shorttolongpath = "algorithm_examples/AlphaJoin/source/shorttolong_imdb_tiny.txt"

    with open(shorttolongpath, 'r') as file:
        lines = file.readlines()

    shorttolong = dict()
    
    for line in lines:
        words = line.strip().split(':')
        short = words[0].strip()
        long = words[1].strip()
        shorttolong[short] = long
    
    # print(shorttolong)

    with open(querypath, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        words = line.strip().split()
        i = 0
        while i < len(words):
            if '.' in words[i]:
                # print(words[i])
                word = words[i].split('.')
                if word[0].split('(')[0] == 'MIN':
                    shorttable = word[0].split('(')[1]
                    column = word[1].strip(')')
                    if shorttable in shorttolong:
                        attr.add(shorttable+'.'+column)
                else:
                    shorttable = word[0]
                    column = word[1]
                    if shorttable in shorttolong:
                        attr.add(shorttable+'.'+column.strip(';'))

                

                
                # print("%s:"%(i),shorttable,end=' ')
                # print(column,end='\n')
            i = i+1
    # print(attr,end='\n')
    # print(len(attr))
    attrNames = list(attr)
    attrNames.sort()
    # print(attrNames)
    tableNames = list(table_name)
    tableNames.sort()

        # Mapping of table name abbreviations and numbers (list subscripts)
    # 表名缩写和编号（列表下标）的映射
    table_to_int = {}
    int_to_table = {}
    for i in range(len(tableNames)):
        int_to_table[i] = tableNames[i]
        table_to_int[tableNames[i]] = i

    # Mapping of attributes and numbers (list subscripts)
    # 属性和编号（列表下标）的映射
    attr_to_int = {}
    int_to_attr = {}
    for i in range(len(attrNames)):
        int_to_attr[i] = attrNames[i]
        attr_to_int[attrNames[i]] = i

    print(attr_to_int)

    predicatesEncode = [0 for _ in range(len(attrNames))]

    predicates =dict()

    for attrName in attrNames:
        # print(attrName)
        longtablename = shorttolong[attrName.split('.')[0]]
        column = attrName.split('.')[1]
        # print(longtablename,end=' ')
        # print(column,end = '\n')
        
        # 查出唯一值，计算选择率
        sql='''
        select n_distinct from pg_stats where tablename = '%s' and attname = '%s'
        ''' % (longtablename, column)
        data_interactor.pull_record()
        data = data_interactor.execute(sql)

        if (len(data.records['n_distinct']) == 0):
            # print(column,end='\n')
            predicatesEncode[attr_to_int[attrName]] = 1.0
        else:
            n_distinct = float(data.records['n_distinct'][0])
            if n_distinct < 0:
                predicatesEncode[attr_to_int[attrName]] = -n_distinct
            else:
                # print(table_rowcount[longtablename],end='\n')
                predicatesEncode[attr_to_int[attrName]] = n_distinct / table_rowcount[longtablename]
                
        # print(data.records['n_distinct'],end='\n')
        # print(predicatesEncode,end='\n')
        # predicates[str(i)] = predicatesEncode
    
    with open(querypath, 'r') as file:
        lines = file.readlines()
    
    predicatesEncode_fin = [0 for _ in range(len(attrNames))]

    j = 0

    for line in lines:
        i = 0
        while i < len(words):
            if '.' in words[i]:
                word = words[i].split('.')
                if word[0].split('(')[0] == 'MIN':
                    shorttable = word[0].split('(')[1]
                    column = word[1].strip(')')
                    if shorttable in shorttolong:
                        attrname = shorttable + '.' + column.strip(';')
                        predicatesEncode_fin[attr_to_int[attrname]] = predicatesEncode[attr_to_int[attrname]]
                else:
                    shorttable = word[0]
                    column = word[1]
                    if shorttable in shorttolong:
                        attrname = shorttable + '.' + column.strip(';')
                        predicatesEncode_fin[attr_to_int[attrname]] = predicatesEncode[attr_to_int[attrname]]
            i = i + 1
        j = j + 1
        predicates[str(j)] = predicatesEncode_fin

    f = open("algorithm_examples/AlphaJoin/source/predicatesEncodedDict",'w')
    f.write(str(predicates))


        




if __name__ == '__main__':
    file_path = "/home/zbw/PilotScopeCore/pilotscope/Dataset/Imdb/job_train_ascii.txt"
    output_file_path = "/home/zbw/PilotScopeCore/algorithm_examples/AlphaJoin/source/shorttolong_imdb_tiny.txt"
    # db_config.db = "imdb"
    # getshorttolong(file_path, output_file_path)

    # # 打开文件并读取内容
    # with open('algorithm_examples/AlphaJoin/source/shorttolong_imdb_tiny.txt', 'r') as file:
    #     data = file.readlines()

    # key_list = []

    # shorttolong = dict()
    # for i in data:
    #     if i == '\n' or i =='Process finished with exit code 0\n':
    #         continue
    #     i.strip('n')
    #     i.strip('\\')
    #     # print(i.split(':')[1])
    #     # print(i.split(':')[0].replace("'", "").strip(' '))
    #     key_list.append(i.split(':')[0].replace("'", "").strip(' '))
    #     shorttolong[i.split(':')[0].replace("'", "").strip(' ')] = i.split(':')[1].replace("'", "").strip(' ').replace("\n","")

    table_name_path = "/home/zbw/PilotScopeCore/algorithm_examples/AlphaJoin/source/longtoshort_imdb_tiny.txt"
    get_selectivity(table_name_path)





