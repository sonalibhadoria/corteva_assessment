Steps to run the app

1. Move to repository directory

2. Install dependencies
pip install -r requirements.txt

3. Initialize the DB
flask --app main.py initdb

4. Run the Flask app
python main.py

5. Ingest the data
curl -X POST http://127.0.0.1:5000/api/ingest

6. Calculate and Ingest the Weather Stats
curl -X POST http://127.0.0.1:5000/api/cal_stats

7. Get the Weather Data
curl -X GET "http://127.0.0.1:5000/api/weather"
curl -X GET "http://127.0.0.1:5000/api/weather?station_name=USC00134063&date=19850101&page=1"

curl -X GET "http://127.0.0.1:5000/api/weather?station_name=USC00134063&date=19850101"
curl -X GET "http://127.0.0.1:5000/api/weather?station_name=USC00134063&page=1"
curl -X GET "http://127.0.0.1:5000/api/weather?date=19850101&page=1"

curl -X GET "http://127.0.0.1:5000/api/weather?station_name=USC00134063"
curl -X GET "http://127.0.0.1:5000/api/weather?date=19850101"
curl -X GET "http://127.0.0.1:5000/api/weather?page=1"


8. Get the Weather Stats Data
curl -X GET "http://127.0.0.1:5000/api/weather/stats"
curl -X GET "http://127.0.0.1:5000/api/weather/stats?station_name=USC00134063&year=1985&page=1"

curl -X GET "http://127.0.0.1:5000/api/weather/stats?station_name=USC00134063&year=1985"
curl -X GET "http://127.0.0.1:5000/api/weather/stats?station_name=USC00134063&page=1"
curl -X GET "http://127.0.0.1:5000/api/weather/stats?year=1985&page=1"

curl -X GET "http://127.0.0.1:5000/api/weather/stats?station_name=USC00134063"
curl -X GET "http://127.0.0.1:5000/api/weather/stats?year=1985"
curl -X GET "http://127.0.0.1:5000/api/weather/stats?page=1"

