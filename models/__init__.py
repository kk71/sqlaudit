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
        password=settings.MONGO_PASSWORD,

        # this parameter should be passed to pymongo
        # since pymongo's connection isn't thread safe
        connect=False
    )

    # connect to oracle

    global Session, engine, base

    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(
        f"mysql+pymysql://{settings.MYSQL_USERNAME}:{settings.MYSQL_PASSWORD}@"
        f"{settings.MYSQL_IP}:{settings.MYSQL_PORT}/{settings.MYSQL_DATABASE}",
        echo=settings.ORM_ECHO)
    base = declarative_base()
    Session = sessionmaker(bind=engine)

