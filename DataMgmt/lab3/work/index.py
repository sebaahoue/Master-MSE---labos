from os import name
from neo4j import GraphDatabase
import pandas as pd


class GenerateTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def create_cities(self):
        cities = pd.read_csv('data/cities.csv', sep=';')
        for index, row in cities.iterrows():
            with self.driver.session() as session:
                session.write_transaction(
                    self._create_city,
                    row['name'],
                    row['latitude'],
                    row['longitude'],
                    row['population']
                )

    def create_lines(self):
        lines = pd.read_csv('data/lines.csv', sep=';')
        for index, row in lines.iterrows():
            with self.driver.session() as session:
                session.write_transaction(
                    self._create_line,
                    row['city1'],
                    row['city2'],
                    row['km'],
                    row['time'],
                    row['nbTracks']
                )

    def create_graph_lines_km(self):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_graph_lines_km
            )
    
    def create_graph_lines_time(self):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_graph_lines_time
            )


    @staticmethod
    def _create_city(tx, name, latitude, longitude, population):
        query = (
            """
            CREATE (c:City { name: $name, latitude: $latitude, longitude: $longitude, population: $population })
            RETURN c
            """
        )
        result = tx.run(query, name=name, latitude=latitude, longitude=longitude, population=population)

        city_created = result.single()['c']
        print("Created City: {name}".format(name=city_created['name']))

    @staticmethod
    def _create_line(tx, city1, city2, km, time, nbTracks):
        query = (
            """
            MATCH (c1:City), (c2:City)
            WHERE c1.name = $city1 AND c2.name = $city2
            CREATE (c1)-[l1:Line {km: $km, time: $time, nbTracks: $nbTracks}]->(c2)
            CREATE (c2)-[l2:Line {km: $km, time: $time, nbTracks: $nbTracks}]->(c1)
            RETURN l1, l2
            """
        )
        result = tx.run(query, city1=city1, city2=city2, km=km, time=time, nbTracks=nbTracks)

        line_created = result.single()['l1']
        # print("Created Line: {city1} - {city2}".format(city1=line_created['c1'], city2=line_created['c2']))

    
    @staticmethod
    def _create_graph_lines_km(tx):
        query = (
            """
            CALL gds.graph.create(
                'lineKM',
                'City',
                'Line',
                {
                    relationshipProperties: 'km'
                }
            )
            """
        )
        result = tx.run(query)

    @staticmethod
    def _create_graph_lines_time(tx):
        query = (
            """
            CALL gds.graph.create(
                'lineTime',
                'City',
                'Line',
                {
                    relationshipProperties: 'time'
                }
            )
            """
        )
        result = tx.run(query)


if __name__ == "__main__":
    generate_train_network = GenerateTrainNetwork("neo4j://localhost:7687")

    # create all city nodes
    generate_train_network.create_cities()
    generate_train_network.create_lines()
    generate_train_network.create_graph_lines_km()
    generate_train_network.create_graph_lines_time()
