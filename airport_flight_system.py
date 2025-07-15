from neo4j import GraphDatabase
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirportFlightSystem:
    """Sistema de gerenciamento de aeroportos e voos usando Neo4j"""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "123456789", database: str = "flights-db"):
        """
        Inicializa conexão com Neo4j

        Args:
            uri: URI de conexão do Neo4j
            user: Usuário do Neo4j
            password: Senha do Neo4j
            database: Nome do banco de dados (padrão: "flights-db")
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None

        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            logger.info(f"✅ Conectado ao Neo4j com sucesso! Database: {database}")
            self._test_connection()
        except Exception as e:
            logger.error(f"❌ Erro na conexão: {e}")
            raise

    def _test_connection(self):
        """Testa a conexão com o banco"""
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1 as test")
                record = result.single()
                if record:
                    logger.info(f"🔍 Teste de conexão no database '{self.database}': {record['test']}")
        except Exception as e:
            logger.error(f"❌ Falha no teste de conexão: {e}")
            raise

    def setup_database(self):
        """Configura o banco de dados com constraints e índices"""
        logger.info(f"🔧 Configurando banco de dados '{self.database}'...")

        with self.driver.session(database=self.database) as session:
            # Criar constraints
            try:
                session.run("CREATE CONSTRAINT airport_code_unique FOR (a:Airport) REQUIRE a.code IS UNIQUE")
                logger.info("✅ Constraint airport_code_unique criada")
            except Exception as e:
                logger.info(f"ℹ Constraint airport_code_unique já existe: {e}")

            # Criar índices
            try:
                session.run("CREATE INDEX airport_code_index FOR (a:Airport) ON (a.code)")
                logger.info("✅ Índice airport_code_index criado")
            except Exception as e:
                logger.info(f"ℹ Índice airport_code_index já existe: {e}")

            try:
                session.run("CREATE INDEX flight_date_index FOR ()-[f:FLIGHT]-() ON (f.date)")
                logger.info("✅ Índice flight_date_index criado")
            except Exception as e:
                logger.info(f"ℹ Índice flight_date_index já existe: {e}")

            try:
                session.run("CREATE INDEX flight_airline_index FOR ()-[f:FLIGHT]-() ON (f.airline)")
                logger.info("✅ Índice flight_airline_index criado")
            except Exception as e:
                logger.info(f"ℹ Índice flight_airline_index já existe: {e}")

    def create_airport(self, code: str, name: str, city: str, state: str, country: str = "USA"):
        """
        Cria ou atualiza um aeroporto

        Args:
            code: Código do aeroporto (ex: "LAS")
            name: Nome do aeroporto
            city: Cidade
            state: Estado
            country: País (padrão: "USA")
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MERGE (a:Airport {code: $code})
            SET a.name = $name,
                a.city = $city,
                a.state = $state,
                a.country = $country
            RETURN a
            """

            result = session.run(query,
                                 code=code, name=name, city=city, state=state, country=country)

            record = result.single()
            if record:
                logger.info(f"✅ Aeroporto {code} criado/atualizado: {name}")
                return record["a"]
            else:
                logger.error(f"❌ Falha ao criar aeroporto {code}")
                return None

    def create_flight(self, flight_data: Dict[str, Any]):
        """
        Cria um voo entre dois aeroportos

        Args:
            flight_data: Dicionário com dados do voo seguindo o formato:
                {
                    'date': '2015-01-01',
                    'airline': 'Spirit Air Lines',
                    'origin_airport': 'LAS',
                    'destination_airport': 'MSP',
                    'scheduled_departure': 25,
                    'departure_time': 19,
                    'departure_delay': -6,
                    'scheduled_arrival': 526,
                    'arrival_time': 509,
                    'arrival_delay': -17,
                    'scheduled_time': 181,
                    'elapsed_time': 170
                }
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (origin:Airport {code: $origin_airport})
            MATCH (destination:Airport {code: $destination_airport})
            CREATE (origin)-[f:FLIGHT {
                date: date($date),
                airline: $airline,
                scheduled_departure: $scheduled_departure,
                departure_time: $departure_time,
                departure_delay: $departure_delay,
                scheduled_arrival: $scheduled_arrival,
                arrival_time: $arrival_time,
                arrival_delay: $arrival_delay,
                scheduled_time: $scheduled_time,
                elapsed_time: $elapsed_time
            }]->(destination)
            RETURN f
            """

            result = session.run(query, **flight_data)
            record = result.single()

            if record:
                #logger.info(
                    #f"✅ Voo criado: {flight_data['origin_airport']} → {flight_data['destination_airport']} ({flight_data['airline']})")
                return record["f"]
            else:
                logger.error(f"❌ Falha ao criar voo: {flight_data}")
                return None

    def get_all_airports(self) -> List[Dict[str, Any]]:
        """Retorna todos os aeroportos"""
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (a:Airport)
            RETURN a.code as code, a.name as name, a.city as city, a.state as state, a.country as country
            ORDER BY a.code
            """

            result = session.run(query)
            airports = []
            for record in result:
                airports.append({
                    'code': record['code'],
                    'name': record['name'],
                    'city': record['city'],
                    'state': record['state'],
                    'country': record['country']
                })

            logger.info(f"📊 Encontrados {len(airports)} aeroportos")
            return airports

    def get_flights_by_airport(self, airport_code: str) -> List[Dict[str, Any]]:
        """Retorna todos os voos de um aeroporto específico"""
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (origin:Airport {code: $airport_code})-[f:FLIGHT]->(destination:Airport)
            RETURN origin.code as origin, destination.code as destination,
                   f.date as date, f.airline as airline,
                   f.departure_delay as departure_delay, f.arrival_delay as arrival_delay,
                   f.scheduled_time as scheduled_time, f.elapsed_time as elapsed_time
            ORDER BY f.date
            """

            result = session.run(query, airport_code=airport_code)
            flights = []
            for record in result:
                flights.append({
                    'origin': record['origin'],
                    'destination': record['destination'],
                    'date': record['date'],
                    'airline': record['airline'],
                    'departure_delay': record['departure_delay'],
                    'arrival_delay': record['arrival_delay'],
                    'scheduled_time': record['scheduled_time'],
                    'elapsed_time': record['elapsed_time']
                })

            logger.info(f"📊 Encontrados {len(flights)} voos para {airport_code}")
            return flights

    def get_flights_with_delays(self, min_delay: int = 0) -> List[Dict[str, Any]]:
        """Retorna voos com atraso na partida"""
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (origin:Airport)-[f:FLIGHT]->(destination:Airport)
            WHERE f.departure_delay > $min_delay
            RETURN origin.code as origin, destination.code as destination,
                   f.date as date, f.airline as airline,
                   f.departure_delay as departure_delay, f.arrival_delay as arrival_delay
            ORDER BY f.departure_delay DESC
            """

            result = session.run(query, min_delay=min_delay)
            flights = []
            for record in result:
                flights.append({
                    'origin': record['origin'],
                    'destination': record['destination'],
                    'date': record['date'],
                    'airline': record['airline'],
                    'departure_delay': record['departure_delay'],
                    'arrival_delay': record['arrival_delay']
                })

            logger.info(f"📊 Encontrados {len(flights)} voos com atraso > {min_delay} min")
            return flights

    def get_delay_statistics(self) -> Dict[str, Any]:
        """Retorna estatísticas de atraso por aeroporto"""
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (origin:Airport)-[f:FLIGHT]->(destination:Airport)
            RETURN origin.code as airport,
                   avg(f.departure_delay) as avg_departure_delay,
                   avg(f.arrival_delay) as avg_arrival_delay,
                   count(f) as total_flights
            ORDER BY avg_departure_delay DESC
            """

            result = session.run(query)
            stats = []
            for record in result:
                stats.append({
                    'airport': record['airport'],
                    'avg_departure_delay': round(record['avg_departure_delay'], 2),
                    'avg_arrival_delay': round(record['avg_arrival_delay'], 2),
                    'total_flights': record['total_flights']
                })

            logger.info(f"📊 Estatísticas geradas para {len(stats)} aeroportos")
            return stats

    def clear_database(self):
        """Limpa todos os dados do banco"""
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info(f"🧹 Banco de dados '{self.database}' limpo")

    def get_database_info(self) -> Dict[str, Any]:
        """Retorna informações sobre o banco de dados"""
        with self.driver.session(database=self.database) as session:  
            # Contar nós
            result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            total_nodes = result.single()["total_nodes"]

            # Contar relacionamentos
            result = session.run("MATCH ()-[r]-() RETURN count(r) as total_relationships")
            total_relationships = result.single()["total_relationships"]

            # Listar labels
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]

            # Listar tipos de relacionamento
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in result]

            info = {
                'total_nodes': total_nodes,
                'total_relationships': total_relationships,
                'labels': labels,
                'relationship_types': rel_types
            }

            logger.info(f"📊 Info do banco '{self.database}': {total_nodes} nós, {total_relationships} relacionamentos")
            return info

    def close(self):
        """Fecha a conexão com o banco"""
        if self.driver:
            self.driver.close()
            logger.info(f"🔌 Conexão com '{self.database}' fechada")

