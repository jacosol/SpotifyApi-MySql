# @Created on  : 28/10/2020 01:33
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : trials.py
from DatabaseManger import DataBaseManager
import numpy as np

db = DataBaseManager(logging_level=1)

db.drop_table('Albums')
TABLES = {}
TABLES['Albums'] = ('create table Albums '
                    '(album_id VARCHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200),'
                    'total_tracks INT);')

db.create_tables(TABLES)
# db.cnx.commit()
qry = 'INSERT INTO Albums (album_id, name, total_tracks) VALUES (%s, %s, %s);'
db.cursor.execute(qry, ('asdfa', 'asdfb', np.int64(3)))

