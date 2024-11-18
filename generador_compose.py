import configparser
import yaml

def generate_yaml(num_clients, client_files, language_num_nodes):
    # Base configuration
    base_config = {
        "name": "tp1",
        "services": {
            "doctor": {
                "container_name": "doctor",
                "build": {"context": ".", "dockerfile": "./doctor/Dockerfile"},
                "image": "doctor:latest",
                "networks": ["testing_net"],
                "environment": [
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            },
            "gateway": {
                "container_name": "gateway",
                "build": {"context": ".", "dockerfile": "./gateway/Dockerfile"},
                "image": "gateway:latest",
                "networks": ["testing_net"],
                "depends_on": [
                    "indie_filter",
                    "action_filter",
                    "games_counter",
                    "positive_review_filter",
                    "positive_review_filter_2",
                    "positive_review_filter3",
                    "positive_review_filter4",
                    "negative_review_filter",
                    "indie_game_review_filter",
                    "indie_game_review_filter2",
                    "indie_game_review_filter3",
                    "indie_game_review_filter4",
                    "action_game_review_filter",
                    "action_name_accumulator",
                    "percentile_accumulator",
                    # Agregar dinámicamente los language_filter_i
                ] + [f"language_filter_{i}" for i in range(1, language_num_nodes + 1)],
                "environment": [
                    "LOGGING_LEVEL=INFO",
                    "OUTPUT_QUEUE=reviews_queue",
                    "AMOUNT_OF_REVIEW_INSTANCE=4",
                    "AMOUNT_OF_GAMES_INSTANCE=2",
                    'OUTPUT_EXCHANGES=["games"]',
                    'INPUT_QUEUES={"result_queue_gateway": "result_queue"}',
                ],
                "volumes": ["./results_gateway:/results_gateway"],
            },
            "games_counter": {
                "container_name": "games_counter",
                "build": {"context": ".", "dockerfile": "./games_counter/Dockerfile"},
                "image": "games_counter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'INPUT_QUEUES={"games_queue_counter":"games"}',
                    'OUTPUT_EXCHANGES=["result_queue"]',
                    "LOGGING_LEVEL=INFO",
                ],
            },
            "indie_filter": {
                "container_name": "indie_filter",
                "build": {"context": ".", "dockerfile": "./genre_filter/Dockerfile"},
                "image": "genre_filter:latest",
                "depends_on": [
                    "top10_indie_counter",
                    "indie_game_review_filter",
                    "indie_game_review_filter2",
                    "indie_game_review_filter3",
                    "indie_game_review_filter4",
                ],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["indie_games"]',
                    'INPUT_QUEUES={"games_queue_filter":"games"}',
                    "LOGGING_LEVEL=INFO",
                    "GENRE=Indie",
                ],
            },
            "action_filter": {
                "container_name": "action_filter",
                "build": {"context": ".", "dockerfile": "./genre_filter/Dockerfile"},
                "image": "genre_filter:latest",
                "depends_on": ["top10_indie_counter", "action_game_review_filter"],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["action_games"]',
                    'INPUT_QUEUES={"games_queue_filter":"games"}',
                    "INSTANCE_ID=2",
                    "LOGGING_LEVEL=INFO",
                    "GENRE=Action",
                ],
            },
            "range_filter": {
                "container_name": "range_filter",
                "build": {"context": ".", "dockerfile": "./range_filter/Dockerfile"},
                "image": "range_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["indie_range_games"]',
                    'INPUT_QUEUES={"indie_games_queue":"indie_games"}',
                    "LOGGING_LEVEL=INFO",
                ],
            },
            "top10_indie_counter": {
                "container_name": "top10_indie_counter",
                "build": {
                    "context": ".",
                    "dockerfile": "./top10_indie_counter/Dockerfile",
                },
                "image": "top10_indie_counter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["result_queue"]',
                    'INPUT_QUEUES={"indie_range_games_queue":"indie_range_games"}',
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["./top10_indie_counter/data:/data"],
                "entrypoint": "python3 main.py",
            },
            "positive_review_filter": {
                "container_name": "positive_review_filter",
                "build": {
                    "context": ".",
                    "dockerfile": "./positivity_filter/Dockerfile",
                },
                "image": "positivity_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["positive_reviews"]',
                    'INPUT_QUEUES={"to_positive_review_1":"to_positive_review"}',
                    "LOGGING_LEVEL=INFO",
                    "POSITIVITY=1",
                    "INSTANCE_ID=0",
                ],
            },
            "positive_review_filter4": {
                "container_name": "positive_review_filter4",
                "build": {
                    "context": ".",
                    "dockerfile": "./positivity_filter/Dockerfile",
                },
                "image": "positivity_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["positive_reviews_4"]',
                    'INPUT_QUEUES={"to_positive_review_4":"to_positive_review"}',
                    "LOGGING_LEVEL=INFO",
                    "POSITIVITY=1",
                    "INSTANCE_ID=0",
                ],
            },
            "positive_review_filter3": {
                "container_name": "positive_review_filter3",
                "build": {
                    "context": ".",
                    "dockerfile": "./positivity_filter/Dockerfile",
                },
                "image": "positivity_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["positive_reviews_3"]',
                    'INPUT_QUEUES={"to_positive_review_3":"to_positive_review"}',
                    "LOGGING_LEVEL=INFO",
                    "POSITIVITY=1",
                    "INSTANCE_ID=0",
                ],
            },
            "positive_review_filter_2": {
                "container_name": "positive_review_filter_2",
                "build": {
                    "context": ".",
                    "dockerfile": "./positivity_filter/Dockerfile",
                },
                "image": "positivity_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["positive_reviews_2"]',
                    'INPUT_QUEUES={"to_positive_review_2":"to_positive_review"}',
                    "LOGGING_LEVEL=INFO",
                    "POSITIVITY=1",
                    "INSTANCE_ID=0",
                ],
            },
            "negative_review_filter": {
                "container_name": "negative_review_filter",
                "build": {
                    "context": ".",
                    "dockerfile": "./positivity_filter/Dockerfile",
                },
                "image": "positivity_filter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["negative_reviews"]',
                    'INPUT_QUEUES={"reviews_queue":"reviews"}',
                    "INSTANCE_ID=1",
                    "LOGGING_LEVEL=INFO",
                    "POSITIVITY=-1",
                ],
            },
            "indie_game_review_filter": {
                "container_name": "indie_game_review_filter",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_review_filter/Dockerfile",
                },
                "image": "game_review_filter:latest",
                "depends_on": ["game_review_positive_counter"],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["games_reviews_indie"]',
                    'INPUT_GAMES_QUEUE=["indie_games_queue","indie_games"]',
                    'INPUT_REVIEWS_QUEUE=["positive_review_queue_1","positive_reviews"]',
                    "PREVIOUS_REVIEW_NODES=1",
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["./game_review_filter/data:/data"],
            },
            "indie_game_review_filter2": {
                "container_name": "indie_game_review_filter2",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_review_filter/Dockerfile",
                },
                "image": "game_review_filter:latest",
                "depends_on": ["game_review_positive_counter"],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["games_reviews_indie"]',
                    'INPUT_GAMES_QUEUE=["indie_games_queue_2","indie_games"]',
                    'INPUT_REVIEWS_QUEUE=["positive_review_queue_2","positive_reviews_2"]',
                    "PREVIOUS_REVIEW_NODES=1",
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["./game_review_filter/data:/data"],
            },
            "indie_game_review_filter3": {
                "container_name": "indie_game_review_filter3",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_review_filter/Dockerfile",
                },
                "image": "game_review_filter:latest",
                "depends_on": ["game_review_positive_counter"],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["games_reviews_indie"]',
                    'INPUT_GAMES_QUEUE=["indie_games_queue_3","indie_games"]',
                    'INPUT_REVIEWS_QUEUE=["positive_review_queue_3","positive_reviews_3"]',
                    "PREVIOUS_REVIEW_NODES=1",
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["./game_review_filter/data:/data"],
            },
            "indie_game_review_filter4": {
                "container_name": "indie_game_review_filter4",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_review_filter/Dockerfile",
                },
                "image": "game_review_filter:latest",
                "depends_on": ["game_review_positive_counter"],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["games_reviews_indie"]',
                    'INPUT_GAMES_QUEUE=["indie_games_queue_4","indie_games"]',
                    'INPUT_REVIEWS_QUEUE=["positive_review_queue_4","positive_reviews_4"]',
                    "PREVIOUS_REVIEW_NODES=1",
                    "LOGGING_LEVEL=INFO",
                ],
                "volumes": ["./game_review_filter/data:/data"],
            },
            "action_game_review_filter": {
                "container_name": "action_game_review_filter",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_review_filter/Dockerfile",
                },
                "image": "game_review_filter:latest",
                "depends_on": [
                    "game_review_positive_counter",
                    "percentile_accumulator",
                ],
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["games_reviews_action"]',
                    'INPUT_GAMES_QUEUE=["action_games_queue","action_games"]',
                    'INPUT_REVIEWS_QUEUE=["negative_review_queue","negative_reviews"]',
                    "PREVIOUS_REVIEW_NODES=1",
                    "LOGGING_LEVEL=INFO",
                    f"AMOUNT_OF_LANGUAGE_FILTERS={language_num_nodes}",
                ],
                "volumes": ["./game_review_filter/data:/data"],
            },
            "game_review_positive_counter": {
                "container_name": "game_review_positive_counter",
                "build": {"context": ".", "dockerfile": "./review_counter/Dockerfile"},
                "image": "review_counter:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["result_queue"]',
                    'INPUT_QUEUES={"games_reviews_queue":"games_reviews_indie"}',
                    "LOGGING_LEVEL=INFO",
                ],
            },

            "action_name_accumulator": {
                "container_name": "action_name_accumulator",
                "build": {
                    "context": ".",
                    "dockerfile": "./game_name_accumulator/Dockerfile",
                },
                "image": "game_name_accumulator:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["result_queue"]',
                    'INPUT_QUEUES={"action_english_reviews_queue":"english_reviews"}',
                    "LOGGING_LEVEL=INFO",
                    "REVIEWS_LOW_LIMIT=5000",
                    f"PREVIOUS_LANGUAGE_NODES={language_num_nodes}",
                ],
            },
            "percentile_accumulator": {
                "container_name": "percentile_accumulator",
                "build": {
                    "context": ".",
                    "dockerfile": "./percentile_accumulator/Dockerfile",
                },
                "image": "percentile_accumulator:latest",
                "networks": ["testing_net"],
                "environment": [
                    'OUTPUT_EXCHANGES=["result_queue"]',
                    'INPUT_QUEUES={"games_reviews_action_queue":"games_reviews_action"}',
                    "LOGGING_LEVEL=INFO",
                    "PERCENTILE=90",
                    "INSTANCE_ID=3",
                ],
            },
        },
        "networks": {
            "testing_net": {
                "driver": "bridge",
                "ipam": {"config": [{"subnet": "172.25.125.0/24"}]},
            }
        },
    }

    # Generate clients
    for i in range(1, num_clients + 1):
        client_name = f"client{i}"
        game_file = client_files[client_name]["game_file"]
        review_file = client_files[client_name]["review_file"]
        
        
        base_config["services"][client_name] = {
            "container_name": f"client{i}",
            "build": {"context": ".", "dockerfile": "./client/Dockerfile"},
            "image": "client:latest",
            "networks": ["testing_net"],
            "depends_on": ["gateway"],
            "environment": [
                "LOGGING_LEVEL=INFO",
                f"CLIENT_ID={i}",
                "BOUNDARY_IP=gateway",
                "BOUNDARY_PORT=12345",
                "DELAY=5",
                "RETRIES=5",
                f"GAME_FILE={game_file}",
                f"REVIEW_FILE={review_file}",
            ],
            "volumes": ["./data:/data", "./results:/results"],
        }
        
    for i in range(1, language_num_nodes + 1):
        base_config["services"][f"language_filter_{i}"] = {
            "container_name": f"language_filter_{i}",
            "build": {
                "context": ".",
                "dockerfile": "./language_filter/Dockerfile",
            },
            "image": "language_filter:latest",
            "networks": ["testing_net"],
            "environment": [
                'OUTPUT_EXCHANGES=["english_reviews"]',
                f'INPUT_QUEUES={{"games_reviews_action_queue_{i}":"games_reviews_action"}}',
                "LOGGING_LEVEL=INFO",
                'INSTANCE_ID=0'
            ],
        }
    return base_config


def save_yaml(config, filename="docker-compose-system.yaml"):
    with open(filename, "w") as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False)

def load_node_files(config_file="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)

    # Read number of clients
    num_clients = int(config["global"]["num_clients"])
    language_num_nodes = int(config["language_filter"]["instances"])
    # Collect client-specific file information
    client_files = {}
    for i in range(1, num_clients + 1):
        client_name = f"client{i}"
        client_files[client_name] = {
            "game_file": config[client_name]["game_file"],
            "review_file": config[client_name]["review_file"]
        }
    
    return num_clients, client_files, language_num_nodes


# Ejemplo de uso
if __name__== "__main__":
    num_clients, client_files, language_num_nodes = load_node_files()
    config = generate_yaml(num_clients, client_files, language_num_nodes)
    save_yaml(config)
    print(f"Archivo YAML generado con {num_clients} clientes.")
