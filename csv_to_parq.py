import os
import re
import csv
import sys
import time
import glob
import numpy
import pandas
import ciso8601
import datetime
import multiprocessing
from ext_list import ext_list
from guppy import hpy


def log_it(msg):
    for m in str(msg).split("\n"):
        print("%s: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), m))
    sys.stdout.flush()

def read_lookup_file(fn):
    d = {}
    with open(fn) as csvfile:
        rdr = csv.reader(csvfile, delimiter=',')
        for row in rdr:
            d[row[0]] = row[1]
    return d

def write_lookup_file(fn, data):
    with open(fn, "w", encoding='utf-8') as fw:
        for k, v in data.items():
            fw.write("%s,%s\n" % (k, v))


def lookup_to_parquet(lookup_name, to_path):
    df = pandas.read_csv("lookup-%s.csv" % lookup_name, names=['name', 'id'])
    df.to_parquet(to_path, engine='fastparquet', index=False)


def get_lookup(dd, val):
    if val in dd['dict'] and dd['dict'][val] != "":
        return dd['dict'][val]
    else:
        with locks[dd["name"]]:
            dd['dict'] = read_lookup_file(dd['fn'])
            if val in dd['dict'] and dd['dict'][val] != "":
                return dd['dict'][val]
            else:
                dd['dict'][val] = len(dd['dict'])+1
                write_lookup_file(dd['fn'], dd['dict'])
                return dd['dict'][val]


def process_file(file_name, to_path, row_count):
    log_it("Start %s" % (file_name))
    efd = [None] * len(cols)
    for i, ef in enumerate(cols):
        if "ignore" in ef:
            continue
        efd[i] = {"name": ef["name"], "data": numpy.ndarray(shape=(row_count), dtype=ef["type"])}
        if "lookup" not in ef:
            continue
        efd[i]["fn"] = "lookup-%s.csv"% ef["name"]
        efd[i]["dict"]: {}
        with locks[ef["name"]]:
            if not os.path.exists(efd[i]["fn"]):
                fw = open(efd[i]["fn"], "w")
                fw.close()
                efd[i]['dict'] = {}
            else:
                efd[i]['dict'] = read_lookup_file(efd[i]["fn"])

    row_num = 0 
    with open(file_name) as csvfile:
        rdr = csv.reader(csvfile, delimiter='|')
        for row in rdr:
            if row_num >= row_count:
                break
            if len(row) <= 5:
                continue
            if row[0] == "":
                continue
            for i in range(0, len(cols)):
                if "row_func" in cols[i]:
                    val = cols[i]['row_func'](row)
                else:
                    val = row[i]
                if "ignore" in cols[i]:
                    continue
                elif "lookup" in cols[i]:
                    val = get_lookup(efd[i], val)
                elif "func" in cols[i]:
                    val = cols[i]['func'](val)
                efd[i]["data"][row_num] = val
            row_num += 1

    df = pandas.DataFrame()
    for i, col in enumerate(cols):
        if "ignore" not in col:
            df[col["name"]] = efd[i]['data']
            if 'object' not in str(col["type"]):
                df[col["name"]] = df[col["name"]].astype(col["type"])
    log_it("Shape %s %s" % (str(df.shape), file_name))
    if row_num < row_count:
        df.dropna(inplace = True)

    pq_file = "%s/%s.parquet" % (to_path, os.path.basename(file_name))
    df.to_parquet(pq_file, engine='fastparquet', index=False)
    log_it("Saved %s" % (pq_file))


def get_extension(row):
    name, ext = os.path.splitext(row[4])
    if len(ext) <= 1:
        return "[n]"
    else:
        ext = ext[1:]
    if ext in ext_list:
        return ext
    else:
        return "[u]"


def fix_path(x):
    pts = x.split('/')
    if len(pts) > limit_path_depth:
        return '/'.join(pts[:(limit_path_depth-1)])
    elif x[-1] == "/":
        return '/'.join(pts[:-2])
    else:
        return '/'.join(pts[:-1])


limit_path_depth = 8

MAXUINT32 = 4294967295
MAXINT32 =  2147483647
locks = {"file_type": multiprocessing.Lock(),
         "owner": multiprocessing.Lock(),
         "op_type": multiprocessing.Lock(),
         "extension": multiprocessing.Lock(),
         }
cols = [
    {"name": "directory_id", "type": numpy.int64},
    {"name": "item_id", "type": numpy.int64},
    {"name": "file_type", "type": numpy.int32, "lookup": True},
    {"name": "path", "type": object, "func": fix_path},
    {"name": "name", "type": object},
    {"name": "size", "type": numpy.int32, "func": lambda x: int(x) if int(x) < MAXINT32 else MAXINT32},
    {"name": "blocks", "type": numpy.int32, "func": lambda x: int(x) if int(x) < MAXINT32 else MAXINT32},
    {"name": "owner", "type": numpy.int32, "lookup": True},
    {"name": "last_mod_day", "type": numpy.int32, 
                "func": lambda x: (ciso8601.parse_datetime(x[:10]).date() - datetime.date(2000, 1, 1)).days},
    {"name": "extension", "type": numpy.int32, "row_func": get_extension, "lookup":True},
]
rows_per_file = 5000000




def break_up_file(data_file, to_path):
    file_size = os.stat(data_file).st_size
    log_it("Read CSV, split up %s gb file into ~%s files" % (int(file_size/1000000000), int((file_size/rows_per_file)/100)))
    st = time.time()
    with open(data_file) as rdr:
        row_num = 0
        w_file_num = 0
        w_file_name = "%s/index-part-%04d.txt" % (to_path, w_file_num)
        fw = open(w_file_name, "w")
        for row in rdr:
            if row_num>0 and (row_num % rows_per_file) == 0:
                log_it("Read %9s rows %6s / sec" % (row_num, int(row_num / (time.time() - st))))
                fw.close()
                w_file_num += 1
                w_file_name = "%s/index-part-%04d.txt" % (to_path, w_file_num)
                fw = open(w_file_name, "w")
            fw.write(row)
            row_num += 1
            # if row_num >= 4500000:
            #     break
        fw.close()
    log_it("Completed spliting of file")


def process_files(path, to_path):
    to_path_tree = "%s.tree" % to_path
    if not os.path.exists(to_path_tree):
        os.makedirs(to_path_tree)
    pool = multiprocessing.Pool(processes=6)
    for w_file_name in glob.glob("/mnt/product/source/qumulo-filesystem-walk/index-part-*.txt"):
        pool.apply_async(process_file, (w_file_name, to_path_tree, rows_per_file))
        # process_file(w_file_name, to_path_tree, rows_per_file)
    pool.close()
    pool.join()
    for col in cols:
        if "lookup" in col:
            lookup_to_parquet(col["name"], "/%s.%s" % (to_path, col["name"]))

if __name__ == '__main__':
    break_up_file(data_file="/mnt/product/source/qumulo-filesystem-walk/output-walk-log.txt", 
                  to_path="/mnt/product/source/qumulo-filesystem-walk/")
    process_files(path="/mnt/product/source/qumulo-filesystem-walk/",
                  to_path="/mnt/product/pq/gravytrain")
