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


if __name__ == "__main__":
    generate_train_network = GenerateTrainNetwork("neo4j://localhost:7687")

    # create all city nodes
    generate_train_network.create_cities()
