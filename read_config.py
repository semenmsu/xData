import collections
import os
import sys
import shutil
import re

dirs = {}
handlers = []

HandlerPattern = collections.namedtuple(
    'HandlerPattern', ['handler', 'input_pattern', 'output_pattern'])


def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def create_dir_if_not_exist(directory):
    directory = os.path.dirname(directory)
    if not os.path.exists(directory):
        os.makedirs(directory)


def read_dirs():
    with open("config/dirs.conf") as f:
        for line in f:
            [dir_name, dir_path] = line.split("=")
            dir_path = dir_path.strip()
            dirs[dir_name] = dir_path
    print(dirs)


def read_config():
    with open("config/moex_fx.conf") as f:
        for line in f:
            values = line.split(' ')
            values = [value.strip() for value in values]
            if len(values) > 2:
                [handler, input_file_pattern, output_file_pattern] = values
                handlers.append(HandlerPattern(
                    handler, input_file_pattern, output_file_pattern))
    pass


def find_matching():  # should be only one
    pass


def find_handlers(input_file, handlers, matched_handlers=[], level=0):
    for handler in handlers:
        input_pattern = handler.input_pattern
        if input_pattern == input_file:
            matched_handlers.append((level, handler))
            matched_handlers = find_handlers(
                handler.output_pattern, handlers, matched_handlers, level+1)
    return matched_handlers


def print_matched_handlers(matched_handlers):
    for handler_row in matched_handlers:
        level = handler_row[0]
        handler = handler_row[1]
        print(" "*4*level, handler)


'''
 def get_char_type(type_name):
        pattern = re.compile("^c(\d+)")
        match = pattern.match(type_name)
        return f"char __name__[{match.group(1)}]"

    def get_decimal_type(type_name):
        pattern = re.compile("d(\d+)\.(\d+)")
        match = pattern.match(type_name)
        return f"struct cgate_decimal_{match.group(1)}_{match.group(2)}"
'''
read_dirs()
read_config()
dd = {}
dd[r"^fast.(.+).raw.7z$"] = dirs["SX_ARCHIVE_DIR"]
dd[r"(.+).extr$"] = dirs["SX_EXTRACTED_PATH"]
dd[r"(.+).def.raw.(.+)"] = dirs["SX_RAW_DIR"]
dd[r"(.+).trades.raw.(.+)"] = dirs["SX_RAW_DIR"]
dd[r"(.+).stat.raw.(.+)"] = dirs["SX_RAW_DIR"]
dd[r"(.+).def.clean.(.+)"] = dirs["SX_CLEAN_DIR"]
dd[r"(.+).trades.clean.(.+)"] = dirs["SX_CLEAN_DIR"]
dd[r"(.+).stat.clean.(.+)"] = dirs["SX_CLEAN_DIR"]


def get_full_file_name(date, file_name):
    print("get_full_file_name------------------------")
    # fast.moex_fx.def.app.itubuntu.raw.7z
    #pattern = re.compile("^fast.*.raw.7z$")
    #match = pattern.match(file_name)
    matched_dirs = []
    for key, value in dd.items():
        print(key)
        pattern = re.compile(key)

        match = pattern.match(file_name)

        if match:
            if not dd[key]:
                raise Exception("can't find dir for pattern")
            print("mathced    >>>>>>>>>>>>>>>>>>", file_name, key)
            matched_dirs.append(dd[key])
        else:
            print("not matching :::::::::::::::::", file_name, key)

    if len(matched_dirs) != 1:
        print("KKKKKKKKKKKKKKKKKKKKKKK", matched_dirs)
        raise Exception(
            "should be only one matching pattern for get_full_file_name")

    file_dir = matched_dirs[0]
    full_file_name = os.path.join(file_dir, date, file_name)
    return full_file_name


def generate_pipeline(date, input_file, matched_handlers):
    if len(matched_handlers) == 0:
        return []

    pipeline = []

    tmp_file_to_real = {}
    current_input_file = get_full_file_name(date, input_file)
    for enumerated_handler in enumerate(matched_handlers):
        index = enumerated_handler[0]
        handler_row = enumerated_handler[1]
        handler = handler_row[1]
        tmp_outputfile = str(index)+".tmp"
        tmp_outputfile = os.path.join(dirs["SX_TMP_DIR"], tmp_outputfile)
        tmp_file_to_real[tmp_outputfile] = get_full_file_name(
            date, handler.output_pattern)
        #print("tmp_file ", tmp_outputfile)
        #print(input_file, " to ", tmp_outputfile)
        full_handler_path = os.path.join(dirs["SX_EXE_PATH"], handler.handler)
        cmd = f"python {full_handler_path} {current_input_file} {tmp_outputfile}"
        # print(cmd)
        pipeline.append(cmd)
        current_input_file = tmp_outputfile
    return (pipeline, tmp_file_to_real)
    # print(tmp_file_to_real)

# file_dict tmp_file_names to real_file_names


def apply_pipeline(pipeline, file_dict):
    tmp_directory = dirs["SX_TMP_DIR"]
    clean_dir(tmp_directory)
    for cmd in pipeline:
        print(cmd)
        res = os.system(cmd)
        if (res < 0):
            clean_dir(tmp_directory)
            print("pipeline was broken")
            break
        print("result = ", res)

    for d in os.listdir(tmp_directory):
        full_tmp_file_name = os.path.join(tmp_directory, d)

        if not file_dict[full_tmp_file_name]:
            clean_dir(tmp_directory)
            print("pipeline was broken")
            raise Exception("can't find tmp_file in file_dict")

    for d in os.listdir(tmp_directory):
        full_tmp_file_name = os.path.join(tmp_directory, d)
        print(full_tmp_file_name, " => ", file_dict[full_tmp_file_name])
        create_dir_if_not_exist(file_dict[full_tmp_file_name])
        mv_cmd = f"mv {full_tmp_file_name} {file_dict[full_tmp_file_name]}"
        os.system(mv_cmd)


def check_matching(date, input_file):
    matched_handlers = find_handlers(input_file, handlers)
    print_matched_handlers(matched_handlers)
    [pipeline, file_dict] = generate_pipeline(
        date, input_file, matched_handlers)
    apply_pipeline(pipeline, file_dict)


def resolve_dir_for_input_file(input_file):
    return "/home/data/archive/"


def cli():
    while True:
        try:
            cli_input = input()
            print(cli_input)
            values = cli_input.split(' ')
            values = [value.strip() for value in values]
            [command, date, file_name] = values
            input_dir = resolve_dir_for_input_file(file_name)
            if input_dir == None:
                print("can't resolve input_dir")
                raise Exception("can't resolve dir for input file")
            print("input_dir ", input_dir)
            if command == "add":
                print(command, " -> ", file_name)
                print("---- start adding file ----")
            elif command == "check":
                print("-------- ", command, date, file_name)
                check_matching(date, file_name)
            else:
                print("unknown command")
        except Exception as ex:
            print("exception ", ex)
            break


cli()
