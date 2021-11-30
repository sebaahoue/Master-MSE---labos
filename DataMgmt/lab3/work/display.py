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

    def display_city_requests(self):
        map_2_2 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_cities_request, map_2_2)
        map_2_2.save('out/2.2.html')

    def display_shortest_path_km(self):
        map_2_3_1 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_shortest_path_km, map_2_3_1)
        map_2_3_1.save('out/2.3.1.html')

    def display_shortest_path_time(self):
        map_2_3_2 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_shortest_path_time, map_2_3_2)
        map_2_3_2.save('out/2.3.2.html')

    def display_minst(self):
        map_2_4 = folium.Map(location=center_switzerland, zoom_start=8)
        with self.driver.session() as session:
            session.read_transaction(self._display_cities, map_2_4)
            session.read_transaction(self._display_lines, map_2_4)
            session.read_transaction(self._display_minst, map_2_4)
        map_2_4.save('out/2.4.html')
            


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
            
    @staticmethod
    def _display_cities_request(tx, m):
        query = (
            """
            MATCH(c1:City)-[:Line*1..4]->(c2:City {name: 'Luzern'})
            where(c1.population > 100000)
            return c1
            """
        )
        result = tx.run(query)
        for record in result:
            display_city_on_map(
                m=m,
                popup=record['c1']['name'],
                latitude=record['c1']['latitude'],
                longitude=record['c1']['longitude']
            )


    @staticmethod
    def _display_shortest_path_km(tx, m):
        query = (
            """
            MATCH (source:City {name: 'Geneve'}), (target:City {name: 'Chur'})
            CALL gds.shortestPath.dijkstra.stream('lineKM', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'km'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
            """
        )
        result = tx.run(query)
        path_cities=[]
        for record in result:
            for node in record['path']:
                path_cities.append(node)
        for city in path_cities:
            display_city_on_map(
                m=m,
                popup=city['name'],
                latitude=city['latitude'],
                longitude=city['longitude']
            )
        for i in range(1, len(path_cities)):
            display_polyline_on_map(
                m=m,
                locations=[(path_cities[i-1]['latitude'], path_cities[i-1]['longitude']), (path_cities[i]['latitude'], path_cities[i]['longitude'])]
            )

    @staticmethod
    def _display_shortest_path_time(tx, m):
        query = (
            """
            MATCH (source:City {name: 'Geneve'}), (target:City {name: 'Chur'})
            CALL gds.shortestPath.dijkstra.stream('lineTime', {
                sourceNode: source,
                targetNode: target,
                relationshipWeightProperty: 'time'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
            """
        )
        result = tx.run(query)
        path_cities=[]
        for record in result:
            for node in record['path']:
                path_cities.append(node)
        for city in path_cities:
            display_city_on_map(
                m=m,
                popup=city['name'],
                latitude=city['latitude'],
                longitude=city['longitude']
            )
        for i in range(1, len(path_cities)):
            display_polyline_on_map(
                m=m,
                locations=[(path_cities[i-1]['latitude'], path_cities[i-1]['longitude']), (path_cities[i]['latitude'], path_cities[i]['longitude'])]
            )

    @staticmethod
    def _display_minst(tx, m):
        query = (
            """
            MATCH path = (c:City {name: 'Bern'})-[:MINST*]-()
            WITH relationships(path) AS rels
            UNWIND rels AS rel
            WITH DISTINCT rel AS rel
            RETURN startNode(rel) AS c1, endNode(rel) AS c2
            """
        )
        result = tx.run(query)
        for record in result:
            # print(record)
            display_polyline_on_map(
                m=m,
                locations=[(record['c1']['latitude'], record['c1']['longitude']),(record['c2']['latitude'], record['c2']['longitude'])],
                color='#d11b1b'
            )


if __name__ == "__main__":
    display_train_network = DisplayTrainNetwork("neo4j://localhost:7687")

    center_switzerland = [46.800663464, 8.222665776]

    # display cities on the map
    display_train_network.display_cities()
    display_train_network.display_lines()
    display_train_network.display_city_requests()
    display_train_network.display_shortest_path_km()
    display_train_network.display_shortest_path_time()
    display_train_network.display_minst()