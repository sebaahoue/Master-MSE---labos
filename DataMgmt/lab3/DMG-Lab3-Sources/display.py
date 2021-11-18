from neo4j import GraphDatabase
import folium


# display city on the folium map
def display_city_on_map(m, popup, latitude, longitude, radius=1000, color="#3186cc"):
    folium.Circle(
        location=(latitude, longitude),
        radius=radius,
        popup=popup,
        color=color,
        fill=True,
        fill_opacity=0.8,
    ).add_to(m)


# display polyline on the folium map
# locations: (list of points (latitude, longitude)) – Latitude and Longitude of line
def display_polyline_on_map(m, locations, popup=None, color="#3186cc", weight=2.0):
    folium.PolyLine(
        locations,
        popup=popup,
        color=color,
        weight=weight,
        opacity=1
    ).add_to(m)


class DisplayTrainNetwork:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def display_cities(self):
        map_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_cities, map_1)
        map_1.save('out/1.html')

    def display_lines(self):
        map_2 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_cities, map_2)
            session.read_transaction(self._display_lines, map_2)
        map_2.save('out/2.1.html')

    @staticmethod
    def _display_cities(tx, m):
        query = (
            """
            MATCH (c:City)
            RETURN c
            """
        )
        result = tx.run(query)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['c']['name'],
                latitude=record['c']['latitude'],
                longitude=record['c']['longitude']
            )

    @staticmethod
    def _display_lines(tx, m):
        query = (
            """
            MATCH (c1:City)-[l:Line]->(c2:City)
            RETURN c1, c2
            """
        )
        result = tx.run(query)
        for record in result:
            # print(record)
            display_polyline_on_map(
                m=m,
                locations=[(record['c1']['latitude'], record['c1']['longitude']),(record['c2']['latitude'], record['c2']['longitude'])]
            )


if __name__ == "__main__":
    display_train_network = DisplayTrainNetwork("neo4j://localhost:7687")

    center_switzerland = [46.800663464, 8.222665776]

    # display cities on the map
    display_train_network.display_cities()
    display_train_network.display_lines()