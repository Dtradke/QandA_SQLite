import json, sqlite3, qanda, uuid

class AnswerEntity_Impl(qanda.AnswerEntity):
    conn = sqlite3.connect('qanda.db', isolation_level=None)
    cursor = conn.cursor()

    def initialize( self ):
      """create table for this class if necessary"""
      """delete all rows in the table for this class"""
      self.cursor.execute('''CREATE TABLE IF NOT EXISTS answer
                            (id TEXT, qid TEXT, uid TEXT, body TEXT, PRIMARY KEY(id),
                            FOREIGN KEY (uid) REFERENCES user(email) ON DELETE RESTRICT ON UPDATE CASCADE  )''')
                            #check to see if you should add a foreign key to question
      self.cursor.execute('''CREATE TABLE IF NOT EXISTS vote
                            (uid TEXT, aid TEXT, votes INTEGER, PRIMARY KEY(uid, aid),
                            FOREIGN KEY (aid) REFERENCES answer(id) ON DELETE CASCADE ON UPDATE CASCADE,
                            FOREIGN KEY (uid) REFERENCES user(email) ON DELETE CASCADE ON UPDATE CASCADE )''')
      self.cursor.execute('DELETE FROM answer')
      self.cursor.execute('DELETE FROM vote')
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.cursor.execute('SELECT count(*) FROM answer WHERE id = ?', (id, )).fetchone()[0]
      if check is not 0:
          row = self.cursor.execute('SELECT * FROM answer WHERE  qid=?', (id,)).fetchone()
      else:
          raise KeyError
      return row

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      self.cursor.execute('SELECT * FROM answer')
      answers = self.cursor.fetchall()
      for answer in answers:
          tally = Tally_Vote.vote_tally(self, answer[0])
          answer_obj = qanda.Answer(answer[0], answer[3], tally[0], tally[1])
          all_rows.append(answer_obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      if id is not '0':
          try:
              self.cursor.execute('DELETE FROM answer WHERE id = ?', (id, ))
          except sqlite3.IntegrityError:
              raise KeyError
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      all_aid_count = []
      aid_count = self.cursor.execute('''SELECT a.id, SUM(v.votes) AS sum_vote FROM answer a LEFT JOIN vote v ON a.id  = v.aid
                                        GROUP BY a.id ORDER BY sum_vote DESC LIMIT ? OFFSET ?''', (limit, offset)).fetchall()
      for aids in aid_count:
          aid_obj = qanda.Rank(aids[0], aids[1])
          all_aid_count.append(aid_obj)
      return all_aid_count

    def new( self, user_id, question_id, text ):
      """allow a user to answer a question"""
      """unique answer id is returned"""
      """KeyError exception should be thrown if user_id or question_id not found"""
      aid = str(uuid.uuid4())
      if user_id is not '0' and question_id is not '0':
          try:
              self.cursor.execute('''INSERT INTO answer (id, qid, uid, body)
                                    VALUES (?,?,?,?)''', (aid, question_id, user_id, text))
          except sqlite3.IntegrityError:
              raise KeyError
      else:
          raise KeyError
      return aid

    def get_answers( self, question_id ):
      """find all answers to a question"""
      """answers are returned as an array of Answer objects"""
      """KeyError exception should be thrown if question_id not found"""
      try:
          answer_arr = []
          self.cursor.execute('SELECT * FROM answer WHERE qid = ?', (question_id,))
          answers = self.cursor.fetchall()
          for answer in answers:
              tally = Tally_Vote.vote_tally( self, answer[0])
              obj = qanda.Answer(answer[0], answer[3], tally[0], tally[1])
              answer_arr.append(obj)
      except sqlite3.IntegrityError:
          raise KeyError
      return answer_arr

    def vote( self, user_id, answer_id, vote ):
      """allow a user to vote on a question; vote is of class Vote"""
      """up and down votes are returned as a pair"""
      """KeyError exception should be thrown if user_id or answer_id not found"""
      if user_id is not '0':
          try:
              self.cursor.execute('''INSERT INTO vote (uid, aid, votes)
                                    VALUES (?,?,?)''', (user_id, answer_id, vote.value))
          except sqlite3.IntegrityError:
              raise KeyError
      else:
          raise KeyError
      return Tally_Vote.vote_tally(self, answer_id)

class Tally_Vote():

    def vote_tally( self, aid ):
        up_tally = self.cursor.execute('''SELECT count(*) FROM vote
                                            WHERE votes = ? and aid = ?''', (1,aid)).fetchone()[0]
        down_tally = self.cursor.execute('''SELECT count(*) FROM vote
                                            WHERE votes = ? and aid = ?''', (-1,aid)).fetchone()[0]
        tally = [up_tally, down_tally]
        return tally
