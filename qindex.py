import sys
import re
import argparse
import random
import time
from qwalk_worker import QWalkWorker,log_it
from qumulo.rest_client import RestClient
from qtasks.Search import *
import multiprocessing
try:
    import queue # python2/3
except:
    pass


SNAP_NAME = "Snapshot for index"
MAX_WORKER_COUNT = 2
WAIT_SECONDS = 1

def snap_worker(creds, q, q_lock, q_len):
    p_name = multiprocessing.current_process().name
    worker_id = int(re.match(r'.*?-([0-9])+', p_name).groups(1)[0])-1
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    file_list = []
    while True:
        try:
            data = q.get(True, timeout=5)
            if data["value"]["op"] == "DELETE":
                print("DELETE  - %s" % data)
                pass
            elif data["type"] == "new_dir":
                print("NEW DIR  - %s" % data)
                for dd in rc.fs.read_entire_directory(id_ = data["id"]):
                    for d in dd["files"]:
                        print("NEW ITEM: %s" % d["name"])
            else:
                d = rc.fs.get_file_attr(path = data["value"]["path"])
                if data["value"]["op"] == "CREATE" and d["type"] == "FS_FILE_TYPE_DIRECTORY":
                    add_to_q(q, q_lock, q_len, {"type":"new_dir", "value": data["value"], "id": d["id"]})
                elif data["value"]["op"] == "CREATE":
                    print("NEW FILE - %s" % data)
                elif d["type"] != "FS_FILE_TYPE_DIRECTORY":
                    print("CHANGE   - %s" % data)
        except queue.Empty:
            # this is expected
            break
        except:
            # this is not expected
            log_it("!! Exception !!")
            log_it(sys.exc_info())
            traceback.print_exc(file=sys.stdout)
        time.sleep(random.random())
        with q_lock:
            q_len.value -= 1

def add_to_q(q, q_lock, q_len, item):
    with q_lock:
        q_len.value += 1
        q.put(item)

def process_snap_diff(creds, path, snap_before, snap_after):
    q = multiprocessing.Queue()
    q_len = multiprocessing.Value("i", 0)
    q_lock = multiprocessing.Lock()

    pool = multiprocessing.Pool(MAX_WORKER_COUNT, 
                                 snap_worker,
                                 (creds, q, q_lock, q_len))

    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])
    results = rc.snapshot.get_all_snapshot_tree_diff(older_snap=snap_before['id'], newer_snap=snap_after['id'])
    ent_list = []
    for res in results:
        for ent in res['entries']:
            print(ent)
            add_to_q(q, q_lock, q_len, {"type":"diff_item", "value": ent})
    #         ent_list.append(ent)
    #         if len(ent_list) > 1:
    #             add_to_q(q, q_lock, q_len, ent_list)
    #             ent_list = []
    # add_to_q(q, q_lock, q_len, ent_list)
    while True:
        log_it("Queue length: %s" % q_len.value)
        time.sleep(WAIT_SECONDS)
        if q_len.value <= 0:
            break


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', help='Qumulo hostname', required=True)
    parser.add_argument('-u', help='Qumulo API user', 
                              default=os.getenv('QUSER') or 'admin')
    parser.add_argument('-p', help='Qumulo API password',
                              default=os.getenv('QPASS') or 'admin')
    parser.add_argument('-d', help='Root Directory', default='/')
    try:
        args, other_args = parser.parse_known_args()
    except:
        print("-"*80)
        parser.print_help()
        print("-"*80)
        sys.exit(0)
    if args.d != '/':
        args.d = re.sub('/$', '', args.d) + '/'

    creds = {"QHOST": args.s, "QUSER": args.u, "QPASS": args.p}
    log_it("Log in to: %s" % (args.s))
    rc = RestClient(creds["QHOST"], 8000)
    rc.login(creds["QUSER"], creds["QPASS"])

    res = rc.snapshot.list_snapshot_statuses()
    existing_snap = None
    for snap in res['entries']:
        if snap['name'] == SNAP_NAME and snap['source_file_path'] == args.d:
            existing_snap = snap
            break
    # snap = rcs[len(rcs)-1].snapshot.create_snapshot(path=path, name=SNAP_NAME)

    snap_before = rc.snapshot.get_snapshot(746219)
    snap_after = rc.snapshot.get_snapshot(748466)

    if snap_before:
        process_snap_diff(creds, 
                          args.d,
                          snap_before,
                          snap_after
                          )
    else:
        w = QWalkWorker(creds, 
                    Search(['--re', '.', 
                            '--cols', 'dir_id,id,type,name,size,blocks,owner,change_time']), 
                    args.d,
                    None, "qumulo-fs-index.txt", None)
        # snap = rc.snapshot.create_snapshot(path=args.d, name=SNAP_NAME)
        # log_it("Snapshot created: %s" % snap["id"])
        # w.run(snap['id'])


if __name__ == "__main__":
    main()
