import vk_api
import requests
import shutil
import sqlite3
import os
from threading import Timer
from multiprocessing import Process
from time import sleep
from hashlib import md5
from requests_toolbelt import MultipartEncoder
import requests
import transliterate
import json

login, password = '79104827408', 'q1w2E#R$'
cloud_path = '/home/rkorolev/CloudVk/'

vk_session = vk_api.VkApi(login, password)


try:
    vk_session.authorization()
except vk_api.AuthorizationError as error_msg:
    print(error_msg)

my_id = vk_session.method('users.get', ({}))[0]['id']


class VkFile():

    def __init__(self, path, modify):
        self.path = path
        self.modify_time = modify

    def __key(self):
        return (self.path, self.modify_time)

    def __eq__(x, y):
        return x.__key() == y.__key()

    def __hash__(self):
        return hash(self.__key())


def get_md5(file_name):
    hash_md5 = md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def init_db():
    db_path = cloud_path+'cloud.db'
    if os.path.isfile(db_path):
        return sqlite3.connect(db_path)
    else:
        con = sqlite3.connect(db_path)
        sql_raw = '''create table files(
                    path varchar(255) unique,
                    title varchar(255),
                    url varchar(255),
                    status varchar(10),
                    mtime real,
                    vk_id int
                    );'''
        cur = con.cursor()
        cur.execute(sql_raw)


def get_cursor():
    db_path = cloud_path+'cloud.db'
    con = sqlite3.connect(db_path)
    return con.cursor()


def qt(s):
    return '"' + s + '"'


def get_vk_docs(user_id=my_id):
    docs_list = vk_session.method('docs.get', ({'owner_id': user_id}))['items']
    vk_docs = {x['title']: {'id': x['id'], 'url': x['url']} for x in docs_list}
    return vk_docs


def get_db_docs(db):
    db.execute('select * from files')
    db_docs = {d[0]: {'id': d[4], } for d in db.fetchall()}
    return db_docs


def get_local_docs():
    file_tree = os.walk(cloud_path)
    local_dict = {}
    for path, dirs, files in file_tree:
        path = path.replace(cloud_path, '')
        for f in files:
            if path+f != 'cloud.db':
                f, bf = transliterate.translit(f, reversed=True), f
                os.rename(bf, f)
                files.remove(bf)
                mtime = os.path.getmtime(cloud_path + path + f)
                local_dict[path+f] = {'mtime': mtime, 'name': f}
    return local_dict


def download_file(url, name):
    r = requests.get(url, stream=True)
    with open(cloud_path + name, 'wb') as f:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, f)


def upload_file(path):
    url = vk_session.method('docs.getUploadServer')['upload_url']
    # with open(path, 'r') as file:
    #     response = requests.post(url, files={'file': file}).text  # .json()
    m = MultipartEncoder(fields={'file': (transliterate.translit(path.split('/')[-1], reversed=True), open(path, 'rb'))})
    file = requests.post(url, data=m, headers={'Content-Type': m.content_type}).json()['file']
    resp = vk_session.method('docs.save', ({'file': file}))
    # try:
    #     upload = vk_api.VkUpload(vk_session)
    #     resp = upload.document(path)
    # except:
    #     p_l = path.split('/')
    #     p = path.replace(p_l[-1], '1.jpg')
    #     os.rename(path, )
    #     resp = upload_file(path)
    return resp


def get_changes(db):
    db_docs = get_db_docs(db)
    vk_docs = get_vk_docs()
    local_docs = get_local_docs()
    raws = ''
    for doc in set(vk_docs.keys()) - set(db_docs.keys()):
        values = ', '.join([qt(doc),
                            qt(doc),
                            qt(str(vk_docs[doc]['url'])),
                            '"vk_added"',
                            '0',
                            qt(str(vk_docs[doc]['id']))])
        raws += 'insert into files values(' + values + '); '
    db.executescript(raws)
    raws = ''
    for doc in set(local_docs.keys()) - set(db_docs.keys()):
        mtime = os.path.getmtime(cloud_path + doc)
        values = ', '.join([qt(doc),
                            qt(doc),
                            '""',
                            '"local_added"',
                            str(mtime),
                            '0'])
        raws += 'insert or replace into files values(' + values + '); '
    db.executescript(raws)
    raws = ''
    local_removed = set(db_docs.keys()) - set(local_docs.keys())
    raw = 'update files set status="local_removed" where path in ("' + '", "'.join(local_removed) + '") and status="ok"'
    db.executescript(raw)
    vk_removed = set(db_docs.keys()) - set(vk_docs.keys())
    raw = 'update files set status="vk_removed" where path in ("' + '", "'.join(vk_removed) + '") and status="ok"'
    db.executescript(raw)


def apply_changes(db):
    raw = 'select url, path from files where status="vk_added";'
    db.execute(raw)
    download_list = db.fetchall()
    for download in download_list:
        download_file(download[0], download[1])
        raw = 'update files set status="ok" where path="'+download[1]+'";'
        db.executescript(raw)

    raw = 'select path from files where status="local_added";'
    upload_list = db.execute(raw)
    for upload in upload_list:
        vk_id = upload_file(cloud_path + upload[0])
        raw = 'update files set status="ok", vk_id=' + str(vk_id[0]['id']) + ' where path="'+upload[0]+'";'
        db.executescript(raw)

    raw = 'select path from files where status="vk_removed";'
    remove_list = db.execute(raw)
    for path in remove_list:
        os.remove(cloud_path + path[0])
        raw = 'delete from files where path="'+path[0]+'";'
        db.executescript(raw)

    raw = 'select vk_id, path from files where status="local_removed";'
    remove_list = db.execute(raw)
    for f in remove_list:
        vk_session.method('docs.delete', ({'owner_id': my_id, 'doc_id': f[0]}))
        raw = 'delete from files where path="'+f[1]+'";'
        db.executescript(raw)


def sync():
    db = get_cursor()
    while True:
        get_changes(db)
        apply_changes(db)
        sleep(10)


def main():
    sync_proces = Process(target=sync)
    sync_proces.start()

if __name__ == '__main__':
    init_db()
    main()
