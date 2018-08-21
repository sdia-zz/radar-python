#!/usr/bin/env python3
#-*- coding:utf-8 -*-




import time
import re

from datetime import datetime
import sqlite3

import lxml.html
import requests


'''
https://pypi.org/
https://pypi.org/simple/
https://pypi.org/project/pomegranate/
http://pypi.python.org/pypi/pomegranate/
'''




def sqlite_dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


PYPI_DB = sqlite3.connect('./pypi.db')
PYPI_DB.row_factory = sqlite_dict_factory


QUERIES = [
    'DROP TABLE IF EXISTS projects;',
    'DROP TABLE IF EXISTS projects_stg;',
    '''
    CREATE TABLE projects (
        name               TEXT,
        description        TEXT,
        version            TEXT,
        release_last       DATE,
        updated_at         TIMESTAMP
    );
    ''',
    '''
    CREATE TABLE projects_stg (
        name               TEXT
    );
    ''',
]


def project_url(name):
    return 'https://pypi.org/project/{}/'.format(name)


def db_insert(table, rec):
    if table == 'projects_stg':
        cmd = "INSERT INTO projects_stg (name) VALUES ('{name}');"

    else:
        cmd = ("INSERT INTO projects    ( \n"
               "    name,                 \n"
               "    description,          \n"
               "    version,              \n"
               "    release_last,         \n"
               "    updated_at            \n"
               ") VALUES (                \n"
               "    '{name}',             \n"
               '    "{description}",      \n'
               "    '{version}',          \n"
               "    '{release_last}',     \n"
               "    '{updated_at}'        \n"
               ");")



    try:
        cmd = cmd.format(**rec)
        PYPI_DB.execute(cmd)
    except:
        print(cmd)
        exit(1)



def stage_projects():
    print('init database...')
    for cmd in QUERIES:
        PYPI_DB.execute(cmd)
        PYPI_DB.commit()

    print('call pypi.org...')
    r = requests.get('https://pypi.org/simple/')
    r.raise_for_status()

    root = lxml.html.fromstring(r.content)
    projects = root.xpath('/html/body/a')

    print('insert projects...')
    for el in projects:
        # el = next(projects)
        s = el.get('href') # /simple/yoda/
        name = s.split('/')[2]
        rec = dict(name=name)
        db_insert('projects_stg', rec)

    PYPI_DB.commit()
    print('load initial done.')



def update_projects():

    cmd = 'select name from projects_stg'
    # cmd += ' order by RANDOM() LIMIT 5;'
    cur = PYPI_DB.cursor()
    cur.execute(cmd)

    for d in cur:
        name = d['name']

        url = project_url(name)
        r = requests.get(url)

        try:
            r.raise_for_status()
        except:
            print('skipping: ', url)
            continue

        root = lxml.html.fromstring(r.content)

        ## <p class="package-description__summary">A set of initial UI components for z3c.form.</p>
        desc = root.xpath("//p[@class='package-description__summary']/text()")[0]
        desc = desc.replace('"', "'")
        desc = re.escape(desc)

        ## <p class="package-header__date">Last released:
        ##     <time class="-js-relative-time" datetime="2015-11-09T14:47:32+0000">
        ##         Nov 9, 2015
        ##     </time>
        ## </p>
        release_last = root.xpath("//p[@class='package-header__date']/time")[0].get('datetime')

        ## <h1 class="package-header__name">
        ##     z3c.formui 3.0.0
        ## </h1>
        version = root.xpath("//h1[@class='package-header__name']/text()")[0]
        version = version.strip().split()[-1].strip()



        rec = dict(name=name,
                   description=desc,
                   release_last=release_last,
                   version=version,
                   updated_at=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))


        db_insert('projects', rec)
        ## print(url)
        ## print(rec)
        ## print('waiting...')
        ## time.sleep(1)

    PYPI_DB.commit()





def get_new_projects(db):
    pass


def get_new_releases(db):
    pass


def main():
    # stage_projects()
    update_projects()



if __name__ == '__main__':
    main()
