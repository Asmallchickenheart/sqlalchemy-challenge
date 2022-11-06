# use the framework from the flask library
from flask import Flask, app
from flask import jsonify
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np
from markupsafe import escape

# create an app, being sure to pass __name__

app = Flask(__name__)


# create engine to hawaii.sqlite
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate Analysis API!<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/start<br/>"
        f"/api/v1.0/temp/start/end"
    )


database_path = "/Users/siyuanliang/BootCamp_University/sqlalchemy_challenge/sqlalchemy-challenge/hawaii.sqlite"
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)
year_ago = dt.date(2017, 8, 23) - dt.timedelta(days=365)


@app.route('/api/v1.0/precipitation')
def precipitation():
    q = session.query(Measurement.date, Measurement.prcp).all()
    df = pd.DataFrame(q, columns=['date', 'precipitation'])
    q_dict = df.set_index('date').to_dict()['precipitation']

    return jsonify(q_dict)


@app.route('/api/v1.0/stations')
def stations():
    each_station = session.query(Measurement.station).group_by(
        Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    station_df = pd.DataFrame(each_station, columns=['station'])
    each_station_dict = station_df.to_dict()['station']
    return jsonify(each_station_dict)


@app.route('/api/v1.0/tobs')
def temp_monthly():
    last_12_months = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= year_ago
                                                                              ).filter(
        Measurement.station == 'USC00519281').order_by(Measurement.date.desc()).all()
    df = pd.DataFrame(last_12_months, columns=["date", "tobs"])
    q_dict = df.set_index('date').to_dict()['tobs']
    return jsonify(q_dict)


@app.route('/api/v1.0/temp/<start>')
def stats(start):
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')
    start_date = start_date.date()
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    q = session.query(*sel).filter(Measurement.date >= start_date).all()
    df = pd.DataFrame(q, columns=['min', 'avg', 'max'])
    q_dict = df.to_dict()
    return jsonify(q_dict)




@app.route('/api/v1.0/temp/<start>/<end>')
def calc_temps(start, end):
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')
    start_date = start_date.date()
    end_date = dt.datetime.strptime(end, '%Y-%m-%d')
    end_date = end_date.date()
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    q = session.query(*sel).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    df = pd.DataFrame(q, columns=['min', 'avg', 'max'])
    q_dict = df.to_dict()
    return jsonify(q_dict)



session.close()
