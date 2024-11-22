# Import libraries for date handling, math operations, database interaction, and web server
import datetime as dt
import numpy as np

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Connect to the SQLite database
database_path = "sqlite:///Resources/hawaii.sqlite"
db_engine = create_engine(database_path)

# Set up a mapping between database tables and Python objects
Database = automap_base()
Database.prepare(autoload_with=db_engine)

# Map the database tables to variables
MeasurementTable = Database.classes.measurement
StationTable = Database.classes.station

# Open a session to interact with the database
db_session = Session(db_engine)

# Create a Flask web application
app = Flask(__name__)

# Define the homepage route
@app.route("/")
def homepage():
    """Show available API routes."""
    return (
        "<h1>Hawaii Climate Data API</h1>"
        "<p>Use the routes below to access the climate data:</p>"
        "<ul>"
        "<li>/api/v1.0/precipitation - Last year's precipitation</li>"
        "<li>/api/v1.0/stations - List of weather stations</li>"
        "<li>/api/v1.0/tobs - Last year's temperature observations</li>"
        "<li>/api/v1.0/temp/start - Min, Avg, Max temperature from a start date</li>"
        "<li>/api/v1.0/temp/start/end - Min, Avg, Max temperature for a date range</li>"
        "</ul>"
        "<p>Format dates as MMDDYYYY for 'start' and 'end'.</p>"
    )

# Route to get last year's precipitation data
@app.route("/api/v1.0/precipitation")
def precipitation_data():
    """Retrieve precipitation data for the past year."""
    # Calculate the date one year ago from the latest date
    last_year_start = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Query precipitation data since that date
    results = db_session.query(MeasurementTable.date, MeasurementTable.prcp).filter(
        MeasurementTable.date >= last_year_start
    ).all()
    
    # Close the database session
    db_session.close()

    # Format the results as a dictionary
    precipitation_dict = {date: prcp for date, prcp in results}
    return jsonify(precipitation_dict)

# Route to get a list of all stations
@app.route("/api/v1.0/stations")
def list_stations():
    """Retrieve a list of weather stations."""
    # Query all station codes
    results = db_session.query(StationTable.station).all()
    
    # Close the database session
    db_session.close()

    # Convert results to a list
    station_list = [station[0] for station in results]
    return jsonify(station_list)

# Route to get last year's temperature observations
@app.route("/api/v1.0/tobs")
def temperature_observations():
    """Retrieve temperature observations for the last year."""
    # Define the start date for one year ago
    last_year_start = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Query temperature observations from the most active station
    results = db_session.query(MeasurementTable.tobs).filter(
        MeasurementTable.station == "USC00519281",
        MeasurementTable.date >= last_year_start
    ).all()
    
    # Close the database session
    db_session.close()

    # Convert results to a list
    temperature_list = [temp[0] for temp in results]
    return jsonify(temperature_list)

# Route to get temperature statistics from a start date
@app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>/<end>")
def temperature_statistics(start, end=None):
    """Retrieve min, average, and max temperatures for a given date range."""
    # Define the selection for the statistics query
    stats_query = [
        func.min(MeasurementTable.tobs),
        func.avg(MeasurementTable.tobs),
        func.max(MeasurementTable.tobs),
    ]

    # Parse the start date
    start_date = dt.datetime.strptime(start, "%m%d%Y")
    
    if end:
        # If an end date is provided, parse it and filter by date range
        end_date = dt.datetime.strptime(end, "%m%d%Y")
        results = db_session.query(*stats_query).filter(
            MeasurementTable.date >= start_date,
            MeasurementTable.date <= end_date
        ).all()
    else:
        # If no end date, filter from the start date forward
        results = db_session.query(*stats_query).filter(
            MeasurementTable.date >= start_date
        ).all()
    
    # Close the database session
    db_session.close()

    # Format the results as a list
    temperature_stats = list(np.ravel(results))
    return jsonify(temperature_stats)

# Run the app
if __name__ == "__main__":
    app.run()
