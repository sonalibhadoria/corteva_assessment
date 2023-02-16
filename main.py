## Import the modules
import datetime
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Api, Resource
from glob import glob
import fileinput as fi
from sqlalchemy import func

## Define constants
DB_URL = r"sqlite:///case_study.db"
WX_DATA_PATH = r"data\wx_data\*"
YLD_DATA_PATH = r"data\yld_data\*"
PER_PAGE = 10
BATCH_SIZE = 100000

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
db = SQLAlchemy(app)
ma = Marshmallow(app)
api = Api(app)

#### Problem 1 - Data Modeling

## 1.1 Data Model for Weather Data
class WXData(db.Model):
    station_name = db.Column(db.String, primary_key=True)
    date = db.Column(db.DateTime, primary_key=True)
    max_temp = db.Column(db.Integer, nullable=True)
    min_temp = db.Column(db.Integer, nullable=True)
    precipitation = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return '<WXData %s %s %s %s %s>' % (self.station_name, self.date, self.max_temp, self.min_temp, self.precipitation)

class WXDataSchema(ma.Schema):
    class Meta:
        model = WXData
        load_instance = True
        fields = ("station_name", "date", "max_temp", "min_temp", "precipitation")

#wxdata_schema = WXDataSchema()
wxdatas_schema = WXDataSchema(many=True)


## 1.2 Data Model for Yield Data
class YLDData(db.Model):
    year = db.Column(db.Integer, primary_key=True)
    grain_yield = db.Column(db.Integer)

    def __repr__(self):
        return '<YLDData %s %s>' % (self.year, self.grain_yield)

class YLDDataSchema(ma.Schema):
    class Meta:
        model = YLDData
        load_instance = True
        fields = ("year", "grain_yield")

#ylddata_schema = YLDDataSchema()
ylddatas_schema = YLDDataSchema(many=True)


## 1.3 Data Model for Weather Stats Data
class WXStats(db.Model):
    station_name = db.Column(db.String, primary_key=True)
    year = db.Column(db.Integer, primary_key=True)
    avg_max_temp = db.Column(db.Float, nullable=True)
    avg_min_temp = db.Column(db.Float, nullable=True)
    total_precipitation = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return '<WXStats %s %s %s %s %s>' % (self.station_name, self.year, self.avg_max_temp, self.avg_min_temp, self.total_precipitation)

class WXStatsSchema(ma.Schema):
    class Meta:
        model = WXStats
        load_instance = True
        fields = ("station_name", "year", "avg_max_temp", "avg_min_temp", "total_precipitation")

#wxstat_schema = WXStatsSchema()
wxstats_schema = WXStatsSchema(many=True)


## 1.4 Flask management command to initialize our database/ model schema
@app.cli.command('initdb')
def init_command():
    from sqlalchemy_utils import database_exists, create_database
    
    if database_exists(DB_URL):
        print('DB Already Exists.')

    if not database_exists(DB_URL):
        print('Creating database...')
        create_database(DB_URL)

    print('Creating tables...')
    db.create_all()



## Problem 2 - Ingestion of weather and yield data
def load_wx_data(wx_data_path, existing_wx_data):
    records = []

    with fi.input(files=glob(wx_data_path)) as f:    
        for line in f:
            
            temp = {}
            
            record = [item.strip('\n').strip() for item in line.split('\t')]
            station_name = f.filename().split('\\')[-1].strip('.txt')
            
            
            if station_name not in existing_wx_data:
                temp['station_name'] = station_name
                temp['date'] = datetime.datetime.strptime(record[0], '%Y%m%d')
                temp['max_temp'] = int(record[1]) if record[1] != '-9999' else None
                temp['min_temp'] = int(record[2]) if record[2] != '-9999' else None
                temp['precipitation'] = int(record[3]) if record[3] != '-9999' else None

                records.append(WXData(station_name=temp['station_name'], 
                                        date=temp['date'], 
                                        max_temp=temp['max_temp'], 
                                        min_temp=temp['min_temp'], 
                                        precipitation=temp['precipitation']))

    return records

def load_yld_data(yld_data_path, existing_yld_data):
    records = []

    with fi.input(files=glob(yld_data_path)) as f:    
        for line in f:
            temp = {}
            record = [item.strip('\n').strip() for item in line.split('\t')]
            temp['year'] = int(record[0])
            temp['grain_yield'] = int(record[1])
          
            if temp not in existing_yld_data:
                records.append(YLDData(year=temp['year'], 
                                        grain_yield=temp['grain_yield']))    
    return records

class IngestData(Resource):
    def post(self):
        app.logger.info('Data Ingestion API Invoked')
        
        existing_wx_data = { item['station_name'] for item in wxdatas_schema.dump(WXData.query.with_entities(WXData.station_name).all()) }
        existing_yld_data = ylddatas_schema.dump(YLDData.query.all())
        
        app.logger.info('{} records already exist for Weather Data'.format(len(wxdatas_schema.dump(WXData.query.with_entities(WXData.station_name).all()))))
        app.logger.info('{} records already exist for Yield Data'.format(len(existing_yld_data)))

        app.logger.info('Data Ingestion Started at {}'.format(str(datetime.datetime.now())))

        wx_data = load_wx_data(WX_DATA_PATH, existing_wx_data)
                
        for index in range(0, len(wx_data), BATCH_SIZE):
            db.session.add_all(wx_data[index : index+BATCH_SIZE])
            db.session.commit()
            app.logger.info('{}/{} records ingested for Weather Data'.format(index+BATCH_SIZE, len(wx_data)))
        
        app.logger.info('Total of {} new records ingested for Weather Data'.format(len(wx_data)))
        
        yld_data = load_yld_data(YLD_DATA_PATH, existing_yld_data)
        db.session.add_all(yld_data)
        db.session.commit()
        app.logger.info('Total of {} new records ingested for Yield Data'.format(len(yld_data)))

        app.logger.info('Data Ingestion Completed at {}'.format(str(datetime.datetime.now())))
        return 'Success', 201


## Problem 3 - Data Analysis
class CalculateStats(Resource):
    def post(self):
        app.logger.info('Calculate Weather Data Stats API Invoked')

        old_records_deleted = WXStats.query.delete()
        app.logger.info('{} older records deleted for Weather Stats Data'.format(old_records_deleted))

        new_stats = db.session\
                        .query(WXData.station_name.label('station_name'), \
                            func.strftime('%Y',WXData.date).label('year'), \
                            func.avg(WXData.max_temp).label('avg_max_temp'), \
                            func.avg(WXData.min_temp).label('avg_min_temp'), \
                            func.sum(WXData.precipitation).label('total_precipitation'))\
                        .group_by(WXData.station_name, func.strftime('%Y',WXData.date))
        
        new_wx_stats = [ WXStats(station_name=stat['station_name'], year=stat['year'], avg_max_temp=stat['avg_max_temp'], avg_min_temp=stat['avg_min_temp'], total_precipitation=stat['total_precipitation']) for stat in wxstats_schema.dump(new_stats) ]

        
        db.session.add_all(new_wx_stats)
        db.session.commit()
        app.logger.info('{} new stats records ingested for Weather Stats Data'.format(len(new_wx_stats)))

        return 'Success', 201

## Problem 4 - REST APIs to fetch the Data fron DB
class Weather(Resource):
    def get(self):

        station_name = str(request.args['station_name']) if 'station_name' in request.args else None
        date = str(request.args['date']) if 'date' in request.args else None
        page = int(request.args['page']) if 'page' in request.args else 1

        if station_name is None and date is None:
            wx_data = WXData.query.paginate(page, PER_PAGE, error_out=False)
            
        elif station_name is None:
            wx_data = WXData.query.filter(WXData.date==datetime.datetime.strptime(date, '%Y%m%d')).paginate(page, PER_PAGE, error_out=False)
            
        elif date is None:
            wx_data = WXData.query.filter(WXData.station_name==station_name).paginate(page, PER_PAGE, error_out=False)
            
        else:
            wx_data = WXData.query.filter(db.and_(WXData.station_name==station_name, WXData.date==datetime.datetime.strptime(date, '%Y%m%d'))).paginate(page, PER_PAGE, error_out=False)
        
        return wxdatas_schema.dump(wx_data.items)

class Yield(Resource):
    def get(self):

        year = int(request.args['year']) if 'year' in request.args else None
        page = int(request.args['page']) if 'page' in request.args else 1
                
        if year is None:
            yld_data = YLDData.query.paginate(page, PER_PAGE, error_out=False)
        else:
            yld_data = YLDData.query.filter(YLDData.year==year).paginate(page, PER_PAGE, error_out=False)
        
        return ylddatas_schema.dump(yld_data.items)

class WeatherStats(Resource):
    def get(self):

        station_name = str(request.args['station_name']) if 'station_name' in request.args else None
        year = int(request.args['year']) if 'year' in request.args else None
        page = int(request.args['page']) if 'page' in request.args else 1

        if station_name is None and year is None:
            wx_stats = WXStats.query.paginate(page, PER_PAGE, error_out=False)
            
        elif station_name is None:
            wx_stats = WXStats.query.filter(WXStats.year==year).paginate(page, PER_PAGE, error_out=False)
            
        elif year is None:
            wx_stats = WXStats.query.filter(WXStats.station_name==station_name).paginate(page, PER_PAGE, error_out=False)
            
        else:
            wx_stats = WXStats.query.filter(db.and_(WXStats.station_name==station_name, WXStats.year==year)).paginate(page, PER_PAGE, error_out=False)
        
        return wxstats_schema.dump(wx_stats.items)


## POST API to ingest the Weather and Yield Data in DB
api.add_resource(IngestData, '/api/ingest')

## POST API to calculate the Weather Stats and ingest in corresponding table in DB
api.add_resource(CalculateStats, '/api/cal_stats')

## GET API to get a JSON-formatted response the ingested Weather data in database. Supports query parameters (station_name, date, page) with pagination enabled
api.add_resource(Weather, '/api/weather')

## GET API to get a JSON-formatted response the ingested Yield data in database. Supports query parameters (year, page) with pagination enabled
api.add_resource(Yield, '/api/yield')

## GET API to get a JSON-formatted response the ingested Weather Stats data in database. Supports query parameters (station_name, year, page) with pagination enabled
api.add_resource(WeatherStats, '/api/weather/stats')


if __name__ == '__main__':
    app.run(debug=True)
