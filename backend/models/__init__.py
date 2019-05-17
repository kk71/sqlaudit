# Author: kk.Fang(fkfkbill@gmail.com)


def init_models():

    import settings

    # connect to mongodb

    from mongoengine import connect as me_connect
    me_connect(
        db=settings.MONGO_DB,
        host=settings.MONGO_SERVER,
        port=settings.MONGO_PORT,
        username=settings.MONGO_USER,
        password=settings.MONGO_PASSWORD
    )

    # connect to oracle

    global Session, engine, base

    import cx_Oracle
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    oracle_dsn = cx_Oracle.makedsn(settings.ORACLE_IP, settings.ORACLE_PORT, sid=settings.ORACLE_SID)
    engine = create_engine(
        f"oracle://{settings.ORACLE_USERNAME}:{settings.ORACLE_PASSWORD}@{oracle_dsn}",
        echo=settings.ORM_ECHO)
    base = declarative_base()
    Session = sessionmaker(bind=engine)

