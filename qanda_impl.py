import json, sqlite3, qanda, answerEntity_impl, questionEntity_impl, userEntity_impl

class QandA_Impl(qanda.QandA):
    file = 'qanda.db'
    conn = sqlite3.connect('qanda.db', isolation_level=None)
    cursor = conn.cursor()
    ue_obj = None
    qe_obj = None
    ae_obj = None

    def initialize( self ):
      """make sure database is empty by deleting all existing rows"""
      self.cursor.execute('PRAGMA foreign_keys=ON')
      userEntity_impl.UserEntity_Impl.initialize(self)
      answerEntity_impl.AnswerEntity_Impl.initialize(self)
      questionEntity_impl.QuestionEntity_Impl.initialize(self)
      return

    def user_entity( self ):
      """return an object that implements UserEntity"""
      ue_obj = userEntity_impl.UserEntity_Impl()
      return ue_obj

    def question_entity( self ):
      """return an object that implements QuestionEntity"""
      qe_obj = questionEntity_impl.QuestionEntity_Impl()
      return qe_obj

    def answer_entity( self ):
      """return an object that implements AnswerEntity"""
      ae_obj = answerEntity_impl.AnswerEntity_Impl()
      return ae_obj
