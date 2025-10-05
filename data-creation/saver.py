import os
import pandas as pd
import logging
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
CSV_FILE = os.path.join(OUTPUT_DIR, "dataset.csv")
DB_URL = "postgresql+pg8000://admin:admin123@postgres:5432/race_data"

Base = declarative_base()

class Sailor(Base):
    __tablename__ = "sailors"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    nation = Column(String)
    team = Column(String)
    sail = Column(String)

class Time(Base):
    __tablename__ = "times"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, unique=True)

class Position(Base):
    __tablename__ = "positions"
    id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)

class Performance(Base):
    __tablename__ = "performances"
    id = Column(Integer, primary_key=True)
    heading_30min = Column(Integer)
    heading_last_report = Column(Integer)
    heading_24h = Column(Integer)
    speed_30min = Column(Float)
    speed_last_report = Column(Float)
    speed_24h = Column(Float)
    vmg_30min = Column(Float)
    vmg_last_report = Column(Float)
    vmg_24h = Column(Float)
    dist_30min = Column(Float)
    dist_last_report = Column(Float)
    dist_24h = Column(Float)
    dtf = Column(Float)
    dtl = Column(Float)

class Conditions(Base):
    __tablename__ = "conditions"
    id = Column(Integer, primary_key=True)
    wind_speed = Column(Float)
    wind_direction = Column(Float)
    wind_gust = Column(Float)

class FactRace(Base):
    __tablename__ = "fact_race"
    id = Column(Integer, primary_key=True)
    sailor_id = Column(Integer, ForeignKey("sailors.id"))
    time_id = Column(Integer, ForeignKey("times.id"))
    position_id = Column(Integer, ForeignKey("positions.id"))
    performance_id = Column(Integer, ForeignKey("performances.id"))
    conditions_id = Column(Integer, ForeignKey("conditions.id"))
    ranking = Column(Integer)

    sailor = relationship("Sailor")
    time = relationship("Time")
    position = relationship("Position")
    performance = relationship("Performance")
    conditions = relationship("Conditions")

def get_or_create(session, model, defaults=None, **kwargs):
    """
    Returns an existing row if it exists, otherwise creates it.
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        params = {**kwargs}
        if defaults:
            params.update(defaults)
        instance = model(**params)
        session.add(instance)
        session.flush()  # flush to assign ID
        return instance


def save_to_postgres():
    logging.info("Connecting to database...")
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    logging.info(f"Loading CSV: {CSV_FILE}")
    df = pd.read_csv(CSV_FILE, parse_dates=["Time in France"])

    logging.info("Inserting data...")
    for _, row in df.iterrows():
        # Dimensions
        sailor = get_or_create(
            session,
            Sailor,
            defaults={"nation": row["Nation"], "team": row["Team"], "sail": row["Sail"]},
            name=row["Sailor"]
        )
        time = get_or_create(session, Time, timestamp=row["Time in France"])
        position = get_or_create(session, Position, latitude=row["Latitude"], longitude=row["Longitude"])
        performance = get_or_create(
            session,
            Performance,
            heading_30min=row["Heading 30min"],
            heading_last_report=row["Heading Last Report"],
            heading_24h=row["Heading 24h"],
            speed_30min=row["Speed 30min"],
            speed_last_report=row["Speed Last Report"],
            speed_24h=row["Speed 24h"],
            vmg_30min=row["VMG 30min"],
            vmg_last_report=row["VMG Last Report"],
            vmg_24h=row["VMG 24h"],
            dist_30min=row["Dist 30min"],
            dist_last_report=row["Dist Last Report"],
            dist_24h=row["Dist 24h"],
            dtf=row["DTF"],
            dtl=row["DTL"]
        )
        conditions = get_or_create(
            session,
            Conditions,
            wind_speed=row["Wind Speed"],
            wind_direction=row["Wind Direction"],
            wind_gust=row["Wind Gust"]
        )

        # Fact table: prevent duplicates by checking existing combination
        existing_fact = session.query(FactRace).filter_by(
            sailor_id=sailor.id,
            time_id=time.id,
            position_id=position.id,
            performance_id=performance.id,
            conditions_id=conditions.id
        ).first()

        if existing_fact:
            logging.info(f"FactRace already exists for sailor {sailor.name} at {time.timestamp}. Skipping.")
            continue

        fact = FactRace(
            sailor_id=sailor.id,
            time_id=time.id,
            position_id=position.id,
            performance_id=performance.id,
            conditions_id=conditions.id,
            ranking=row["Ranking"]
        )
        session.add(fact)

    session.commit()
    logging.info("Data insertion complete.")
