from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base


PG_DSN = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(50))
    eye_color = Column(String(60))
    films = Column(String(400))
    gender = Column(String(60))
    hair_color = Column(String(50))
    height = Column(String(50))
    homeworld = Column(String(400))
    mass = Column(String(50))
    name = Column(String(100))
    skin_color = Column(String(50))
    species = Column(String(400))
    starships = Column(String(400))
    vehicles = Column(String(400))
