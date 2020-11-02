# @Created on  : 02/11/2020 13:17
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : baisc_trials.py
from DatabaseManger import DataBaseManager

db = DataBaseManager(logging_level=1)

# what happens when trying to add again an existing id?

db.cursor.execute('insert ignore into Albums (album_id, name) values (\'9999CUp5tnuVanzlN4pWtP\', \'bla\');')
db.cnx.commit()