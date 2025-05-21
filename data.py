from sqlalchemy import create_engine, text

QUERY_FLIGHT_BY_ID = text("SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, "
                          "flights.DEPARTURE_DELAY as DELAY FROM flights JOIN airlines "
                          "ON flights.airline = airlines.id WHERE flights.ID = :id")
QUERY_FLIGHT_BY_DATE = text("SELECT ID, ORIGIN_AIRPORT, DESTINATION_AIRPORT, AIRLINE, "
                            "DEPARTURE_DELAY AS DELAY FROM flights WHERE YEAR = :year "
                            "AND MONTH = :month AND DAY = :day")
QUERY_FLIGHT_BY_AIRLINE = text("SELECT f.AIRLINE AS ID, f.ORIGIN_AIRPORT AS "
                               "ORIGIN_AIRPORT, f.DESTINATION_AIRPORT AS DESTINATION_AIRPORT, "
                               "a.AIRLINE AS AIRLINE, f.DEPARTURE_DELAY AS DELAY "
                               "FROM flights AS f JOIN airlines AS a ON f.AIRLINE = a.ID "
                               "WHERE a.AIRLINE = :airline AND f.DEPARTURE_DELAY >= 20")
QUERY_FLIGHT_BY_AIRPORT = text("SELECT f.ID AS ID, f.ORIGIN_AIRPORT AS ORIGIN_AIRPORT, "
                               "f.DESTINATION_AIRPORT AS DESTINATION_AIRPORT, "
                               "a.AIRLINE AS AIRLINE, f.DEPARTURE_DELAY AS DELAY "
                               "FROM flights AS f JOIN airlines AS a ON f.AIRLINE = a.ID "
                               "WHERE f.ORIGIN_AIRPORT = :airport AND f.DEPARTURE_DELAY >= 20")

class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        with self._engine.connect() as connection:
            return connection.execute(query, params).mappings().all()

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)


    def get_flights_by_date(self, flight_day, flight_month, flight_year):
        """
        """
        params = {'year': flight_year, 'month': flight_month, 'day': flight_day}
        return self._execute_query(QUERY_FLIGHT_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline_input):
        """
        """
        params = {'airline': airline_input}
        return self._execute_query(QUERY_FLIGHT_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport_input):
        """
        """
        params = {'airport': airport_input}
        return self._execute_query(QUERY_FLIGHT_BY_AIRPORT, params)


    def __del__(self):
        """
        Closes the connection to the databse when the object is about to be destroyed
        """
        self._engine.dispose()
