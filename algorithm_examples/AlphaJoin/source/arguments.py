import argparse

# get the arguments...
def get_args():
    # 定义命令行解析器对象
    parse = argparse.ArgumentParser(description='nn')
    # 添加命令行参数
    parse.add_argument('--env-name', type=str, default='postgresql', help='the training environment')
    parse.add_argument('--save-dir', type=str, default='saved_models/', help='the folder that save the models')
    parse.add_argument('--cuda', action='store_true', help='if use the GPU to do the training')  # 运行时该变量有传参就将该变量设为True
    # get args...
    # 从命令行中结构化解析参数
    args = parse.parse_args()  # 把刚才的属性从parse给args,后面直接通过args使用

    return args

args = get_args()
print (args)