import os
import sys
#от входного файла должно генерироваться необходимое количество файлов
#которые должны породится на основе указаных обработчиков и конфигураций
#добавление нового шага в pipeline для генерирования новой информации в любое место
#pipeline должно быть рутинной операцией в рамках данной инфраструктуры
#... например:
# задаем генерирование статистики по торговле топовыми инструментами в середине pipeline
# bin=get_top_stat input=moex_fx.stat.*.*.clean output=moex_fx.stat.top
# moex_fx.top_stat MOEX_CLEAN/moex_fx.stat.*.clean MOEX_STAT/moex_fx.stat.top
# stage and extension должно определять папку?
#



#SX_PATH_BIN
#SX_ARCHIVE_DIR
#SX_

class VirtualTree:
    def __init__(self):
        pass

class PipelineTree:
    def __init__(self):
        self.pipeline = []

    def add(self, handler,input_file, output_file):
        self.pipeline.append((handler, input_file, output_file))

class DefaultHander:
    def __init__(self, input_file):
        self.input_file = input_file
        pass

    def get_output_file(self):
        return "/home/semen/devel/code/synchro/file1.txt"

def HandlerResolver(file_name):
    if file_name == "file.txt":
        return DefaultHander(file_name)
    return None


def get_pipeline_tree(file_name, pipeline_tree):
    file_handler = HandlerResolver(file_name)
    if file_handler:
        output_file = file_handler.get_output_file()
        pipeline_tree.add(file_handler, file_name, output_file)
        pipeline_tree = get_pipeline_tree(output_file, pipeline_tree)
        return pipeline_tree
    else:
        return pipeline_tree

def cli_handle_add_file(file_name):
    pipeline_tree = PipelineTree()
    pipeline_tree = get_pipeline_tree(file_name, pipeline_tree)
    pipeline = pipeline_tree.pipeline

    transaction_files = []
    #prepare transaction data
    for handler_file in pipeline:
        handler = handler_file[0]
        input_file = handler_file[1]
        output_file = handler_file[2]
        tmp_output_file = "~/tmp/"+output_file
        transaction_files.append((tmp_output_file, output_file))
        rc = os.system(f"{handler.get_path()} {input_file} {tmp_output_file}")
        if rc < 0:
            clean_tmp_files()
            log_transaction_error()
            mark_file_like_bad()

    #apply transaction if all ok
    for step in transaction_files:
        tmp_output_file = step[0]
        output_file = step[1]
        os.system(f"mv {tmp_output_file} {output_file}")






def cli():
    while True:
        cli_input = raw_input()
        [command, file_name] = cli_input.split(' ')
        

        if command == "add":
            print(command," -> ", file_name)
            print("---- start adding file ----")
            cli_handle_add_file(file_name)
        else:
            print("unknown command")

cli()