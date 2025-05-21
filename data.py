from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError, IntegrityError, SQLAlchemyError

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
QUERY_DELAY_PERCENTAGE_BY_AIRLINE = text("SELECT a.AIRLINE AS airline_name, "
                                         "ROUND(COUNT(CASE WHEN f.DEPARTURE_DELAY > 0 THEN 1 END) "
                                         "* 100.0 / COUNT(*), 2) AS delay_percentage "
                                         "FROM flights f JOIN airlines a ON f.AIRLINE = a.ID "
                                         "GROUP BY a.AIRLINE ORDER BY delay_percentage DESC;")


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
        try:
            with self._engine.connect() as connection:
                return connection.execute(query, params).mappings().all()
        except (OperationalError, ProgrammingError, IntegrityError) as error:
            print(f'Database-specific error: {error}')
        except SQLAlchemyError as error:
            print(f'SQLAlchemy error: {error}')
        except Exception as error:
            print(f'Unexpected error: {error}')
        return []

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, flight_day, flight_month, flight_year):
        """
        Search all flight details by date and returns a list of all founded flights.
        """
        params = {'year': flight_year, 'month': flight_month, 'day': flight_day}
        return self._execute_query(QUERY_FLIGHT_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline_input):
        """
        Search all delayed flights and their details by airline
        and returns a list of all founded flights.
        """
        params = {'airline': airline_input}
        return self._execute_query(QUERY_FLIGHT_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport_input):
        """
        Search all delayed flights and their details by origin airport
        and returns a list of all founded flights.
        """
        params = {'airport': airport_input}
        return self._execute_query(QUERY_FLIGHT_BY_AIRPORT, params)

    def get_delay_percentage_by_airline(self):
        """
        Gets percentage of all delayed flights by airlines and
        returns a list of all founded Airlines.
        """
        return self._execute_query(QUERY_DELAY_PERCENTAGE_BY_AIRLINE, {})

    def __del__(self):
        """
        Closes the connection to the database when the object is about to be destroyed
        """
        self._engine.dispose()
