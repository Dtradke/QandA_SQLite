import json, sqlite3, qanda, uuid

class UserEntity_Impl(qanda.UserEntity):
    conn = sqlite3.connect('qanda.db', isolation_level=None)
    cursor = conn.cursor()

    def initialize( self ):
      """create table for this class if necessary"""
      """delete all rows in the table for this class"""
      self.cursor.execute('''CREATE TABLE IF NOT EXISTS user
                            (id TEXT, email TEXT, passhash TEXT, PRIMARY KEY(email) )''')
      self.cursor.execute('DELETE FROM user')
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      try:
          row = self.cursor.execute('SELECT FROM user WHERE  id=?', id).fetchone()
          user_obj = qanda.User(row[0], row[1], row[2])
      except sqlite3.IntegrityError:
          raise KeyError
      return user_obj

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      self.cursor.execute('SELECT * FROM user')
      users = self.cursor.fetchall()
      for user in users:
          obj = qanda.User(user[0], user[1], user[2])
          all_rows.append(obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.cursor.execute('SELECT count(*) FROM user WHERE id = ?', (id, ))
      if check is not 0:
          self.cursor.execute('DELETE FROM user WHERE id = ?', (id, ))
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      all_uid_count = []
      self.cursor.execute('DROP VIEW IF EXISTS user_answer_count')
      self.cursor.execute('''CREATE VIEW user_answer_count AS SELECT uid, count(*) AS count FROM answer a
                                        WHERE uid NOT IN (SELECT uid FROM question q WHERE a.qid = q.id)
                                        GROUP BY uid ORDER BY count DESC''')
      uid_count = self.cursor.execute('''SELECT id, user_answer_count.count FROM user u LEFT JOIN user_answer_count
                                        ON u.id = user_answer_count.uid ORDER BY user_answer_count.count DESC
                                        LIMIT ? OFFSET ?''', (limit, offset)).fetchall()
      for uids in uid_count:
          uid_obj = qanda.Rank(uids[0], uids[1])
          all_uid_count.append(uid_obj)
      return all_uid_count

    def new( self, email, passhash = None ):
      """create a new instance in db using the given parameters"""
      """unique user id is returned"""
      """if email already exists, KeyError exception will be thrown"""
      uid = str(uuid.uuid4())
      try:
          self.cursor.execute('INSERT INTO user (id, email, passhash) VALUES (?,?,?)', (uid, email, passhash))
      except sqlite3.IntegrityError:
          raise KeyError
      return uid
