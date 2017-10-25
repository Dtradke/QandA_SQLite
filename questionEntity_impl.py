import json, sqlite3, qanda, uuid

class QuestionEntity_Impl(qanda.QuestionEntity):
    conn = sqlite3.connect('qanda.db', isolation_level=None)
    cursor = conn.cursor()

    def initialize( self ):
      """create table for this class if necessary"""
      """delete all rows in the table for this class"""
      self.cursor.execute('''CREATE TABLE IF NOT EXISTS question
                            (id TEXT, uid TEXT, body TEXT, PRIMARY KEY(id),
                            FOREIGN KEY (uid) REFERENCES user(email) ON DELETE RESTRICT ON UPDATE CASCADE  )''')
      self.cursor.execute('DELETE FROM question')
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      if id is not '0':
          try:
              row = self.cursor.execute('SELECT * FROM question WHERE  id=?', (id,)).fetchone()
              question_obj = qanda.Question(row[0], row[2])
          except sqlite3.IntegrityError:
              raise KeyError
      else:
          raise KeyError
      return question_obj

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      self.cursor.execute('SELECT * FROM question')
      questions = self.cursor.fetchall()
      for question in questions:
          question_obj = qanda.Question(question[0], question[2])
          all_rows.append(question_obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.cursor.execute('SELECT count(*) FROM question WHERE id = ?', (id, )).fetchone()[0]
      if check is not 0:
          if id is 0:
              raise KeyError
          self.cursor.execute('DELETE FROM question WHERE id = ?', (id, ))
          self.cursor.execute('DELETE FROM answer WHERE qid = ?', (id,)) #CAN MAYBE GET RID OF THIS AT END
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      #return question ids in order of how many answers they have
      all_qid_count = []
      qid_count = self.cursor.execute('''SELECT qid, count(*) FROM answer GROUP BY qid UNION
                                        SELECT q.id, 0 FROM question q WHERE q.id NOT IN (SELECT DISTINCT(qid) FROM answer)
                                        ORDER BY count(*) DESC LIMIT ? OFFSET ?''', (limit, offset)).fetchall()
      for qids in qid_count:
          qid_obj = qanda.Rank(qids[0], qids[1])
          all_qid_count.append(qid_obj)
      return all_qid_count

    def new( self, user_id, text ):
      """allow a user to pose a question"""
      """unique question id is returned"""
      """KeyError exception should be thrown if user_id not found"""
      if user_id is not '0':
          try:
              qid = str(uuid.uuid4())
              self.cursor.execute('INSERT INTO question (id, uid, body) VALUES (?,?,?)', (qid, user_id, text))
          except sqlite3.IntegrityError:
              raise KeyError
      else:
          raise KeyError
      return qid
