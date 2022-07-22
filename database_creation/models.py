

### models.py ###

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date

Base = declarative_base()

class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)

    
    def __repr__(self):
        return "<User(username='{}', password='{}'>"\
                .format(self.username, self.password)