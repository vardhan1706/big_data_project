from neo4j import GraphDatabase
import yaml
import pandas as pd


class Neo4jDataLoader:
    """
    A class to load data from a pandas DataFrame into Neo4j.
    """

    def __init__(self, uri, user, password):
        """
        Initialize Neo4jDataLoader with the URI, username, and password.

        Parameters:
            uri (str): URI to connect to Neo4j.
            user (str): Username for Neo4j authentication.
            password (str): Password for Neo4j authentication.
        """
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the Neo4j driver."""
        self._driver.close()

    def create_node(self, tx, node_label, properties):
        """
        Create a node in Neo4j.

        Parameters:
            tx: Neo4j transaction.
            node_label (str): Label for the node.
            properties (dict): Properties of the node.
        """
        query = f"CREATE (n:{node_label} {{"
        for key, value in properties.items():
            if ' ' in key:  # Check if property name contains spaces
                key = f"`{key}`"  # Enclose property name in backticks
            # Check if value is a string before attempting to replace single quotes
            if isinstance(value, str):
                value = value.replace("'", "\\'")  # Escape single quotes
            query += f"{key}: '{value}', "
        query = query[:-2]  # remove the last comma and space
        query += "})"
        tx.run(query)

    def load_data_from_csv(self, csv_file_path, node_label, batch_size=1000):
        """
        Load data from a CSV file into Neo4j using bulk insertion.

        Parameters:
            csv_file_path (str): Path to the CSV file.
            node_label (str): Label for the nodes in Neo4j.
            batch_size (int): Number of rows to batch together (default is 1000).
        """
        print("Loading data from CSV into Neo4j...")
        data = pd.read_csv(csv_file_path)
        total_rows = len(data)
        num_batches = (total_rows // batch_size) + 1

        with self._driver.session() as session:
            for i in range(num_batches):
                start_index = i * batch_size
                end_index = min((i + 1) * batch_size, total_rows)
                batch_data = data.iloc[start_index:end_index]

                query = f"UNWIND $batch as row CREATE (n:{node_label}) SET n += row"
                session.run(query, batch=batch_data.to_dict(orient="records"))

        print("Data loaded successfully.")


# Example usage
if __name__ == "__main__":
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    neo4j_uri = config['NEO4J']['neo4j_uri']
    neo4j_user = config['NEO4J']['neo4j_user']
    neo4j_password = config['NEO4J']['neo4j_password']
    print(neo4j_uri)
    print(neo4j_password)
    loader = Neo4jDataLoader(neo4j_uri, neo4j_user, neo4j_password)
    csv_file_path = config['NEO4J']['csv_file_path']
    node_label = "ConsumerComplaints"
    print(csv_file_path)
    loader.load_data_from_csv(csv_file_path, node_label)

    # Close the connection
    loader.close()
