from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime
from .database import Base

class Game(Base):
    __tablename__ = "game"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    genre = Column(String)
    release_year = Column(Integer)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime)

class Feature(Base):
    __tablename__ = "features"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    type_feature = Column(Integer)
    game_id = Column(Integer, ForeignKey('game.id'))



