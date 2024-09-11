import os

from dotenv import load_dotenv, find_dotenv

CONFIG_FILE = '.env'

ROOT_DIR = os.getcwd() + "/top-songs-qdrant-master"
DATA_DIR = os.path.join(ROOT_DIR, "content/songs")
STATIC_DIR = os.path.join(ROOT_DIR, "content/assets")

# Search Configuration
MAX_SEARCH_LIMIT = 100
DEFAULT_LIMIT = 20
GROUP_BY_FIELD = ""

# Set up credentials and environment variables
_ = load_dotenv(find_dotenv())


class Settings:
    """Take environment variables from .env file"""

    def __init__(self):
        # Qdrant config
        self.url = os.getenv('QDRANT_URL')
        self.api_key = os.getenv('QDRANT_API_KEY')
        self.collection_name = os.getenv('COLLECTION_NAME')
        self.text_field_name = os.getenv('TEXT_FIELD_NAME')
        self.model_card = os.getenv('MODEL_CARD')
        self.model_card_sparce = os.getenv('MODEL_CARD_SPARSE')

        os.environ['QDRANT_URL'] = self.url
        os.environ['QDRANT_API_KEY'] = self.api_key
        os.environ['EMBEDDINGS_MODEL'] = self.model_card
        os.environ['MODEL_CARD_SPARSE'] = self.model_card_sparce
        os.environ['TEXT_FIELD_NAME'] = self.text_field_name


settings = Settings()
