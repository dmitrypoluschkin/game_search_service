from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime
from sqlalchemy.sql import func
from .database import Base

class Game(Base):
    __tablename__ = "game"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    genre = Column(String, nullable=False)
    release_year = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

class Feature(Base):
    __tablename__ = "features"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    type_feature = Column(Integer, nullable=False)
    game_id = Column(Integer, ForeignKey('game.id'), nullable=False)



