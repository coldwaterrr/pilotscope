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
        table_rowcount[table] = data.records['count'][0]
        # print(data.records['count'][0])

    # 获取每条语句涉及到的列
    attr = set()
    querypath = "pilotscope/Dataset/Imdb/job_train_ascii.txt"
    
    with open(querypath, 'r') as file:
        lines = file.readlines()
    
    for line in lines:
        words = line.strip().split()
        i = 0
        while i < len(words):
            if '.' in words[i]:
                # print(words[i])
                
    
    


 


# Get all the attributes used to select the filter vector
# 获取用于选择滤波器向量的所有属性
def getQueryAttributions():
    # fileList = os.listdir(querydir)
    # fileList.sort()
    attr = set()  # 创建一个无序不重复的元素集

    rowscount = dict()

    for queryName in fileList:
        print(queryName)
        querypath = querydir + "/" + queryName
        file_object = open(querypath)
        file_context = file_object.readlines()  # 获取query语句
        file_object.close()

        # find WHERE
        k = -1
        for i in range(len(file_context)):
            k = k + 1
            if file_context[i].find("WHERE") != -1:
                break

        # handle a sentence after WHERE
        # 处理 WHERE 后的句子
        for i in range(k, len(file_context)):
            temp = file_context[i].split()  # 默认空格分隔
            for word in temp:
                if '.' in word:
                    if word[0] == "'":
                        continue
                    if word[0] == '(':
                        word = word[1:]  # object[start:end:step]   object[:]表示从头取到尾，步长默认为1  object[::]一样表示从头到尾，步长为1
                    if word[-1] == ';':  # object[:5]没有Start表示从头开始取,步长为1，object[5:]表示从5开始到尾，步长为1
                        word = word[:-1]

                    if word.split('.')[0] not in key_list:
                        continue

                    short_tablename = word.split('.')[0]
                    column = word.split('.')[1]

                    print(short_tablename)
                    long_tablename = shorttolong[short_tablename]

                    if column == 'kind' or column == 'id' or column == 'role':
                        selectivity[word] = 1.0

                    # 最简单的选择率:唯一值数/行数
                    # 获取每一张表的具体行数
                    key_exist_1 = word in selectivity
                    key_exist_2 = long_tablename in rowscount
                    if key_exist_1 == False and key_exist_2 == False:
                        sql = '''
                        select count(*) from %s
                        '''% (long_tablename)
                        cur.execute(sql)
                        rows = cur.fetchall()

                        for row in rows:
                            rowscount[long_tablename] = float(row[0])

                    # 查出唯一值，计算选择率
                        sql='''
                        select n_distinct from pg_stats where tablename = '%s' and attname = '%s'
                        ''' % (long_tablename, column)
                        cur.execute(sql)
                        rows = cur.fetchall()
                        # print(rows)
                        for row in rows:
                            n_distinct = float(row[0])

                        if n_distinct < 0:
                            selectivity[word] = -n_distinct
                        else:
                            selectivity[word] = n_distinct / rowscount[long_tablename]



                    elif key_exist_1 == False and key_exist_2 == True:
                        # 查出唯一值，计算选择率
                        sql = '''
                        select n_distinct from pg_stats where tablename = '%s' and attname = '%s'
                        ''' % (long_tablename, column)
                        cur.execute(sql)
                        rows = cur.fetchall()
                        # print(rows)
                        for row in rows:
                            n_distinct = float(row[0])

                        if n_distinct < 0:
                            selectivity[word] = -n_distinct
                        else:
                            selectivity[word] = n_distinct / rowscount[long_tablename];


                        if n_distinct < 0:
                            selectivity[word] = -n_distinct
                        else:
                            selectivity[word] = n_distinct / float(rowscount[long_tablename])

                    attr.add(word)

    attrNames = list(attr)
    attrNames.sort()
    return attrNames





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





