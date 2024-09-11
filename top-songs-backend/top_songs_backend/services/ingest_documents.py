import json
import os
import uuid

import xmltodict

from datetime import datetime
from tqdm import tqdm
from xml.etree import ElementTree

from qdrant_client import QdrantClient
from qdrant_client.http import models

from top_songs_backend.config.config import (
    ROOT_DIR,
    DATA_DIR,
)


class HybridQdrantOperations:

    def __init__(self):
        self.url = os.getenv('QDRANT_URL')
        self.api_key = os.getenv('QDRANT_API_KEY')
        self.collection_name = os.getenv('COLLECTION_NAME')
        self.text_field_name = os.getenv('TEXT_FIELD_NAME')
        self.model_card = os.getenv('MODEL_CARD')
        self.model_card_sparce = os.getenv('MODEL_CARD_SPARSE')

        self.client = QdrantClient(
            url=os.environ['QDRANT_URL'],
            api_key=os.environ['QDRANT_API_KEY']
        )
        self.client.set_model(self.model_card)
        self.client.set_sparse_model(self.model_card_sparce)

        self.metadata = []
        self.documents = []

    def ingest_doc(self, file_path: str):
        print("file_path: ", file_path)
        if not os.path.exists(file_path):
            print(f"Document {file_path} doesn't exit")
            return None

        obj = {
            "id": str(uuid.uuid4())
        }
        with open(file_path, 'r') as xml_file:
            print("file_path: ", file_path)
            # Read XML document
            xml_data = xml_file.read()

            # Parse the given XML input and convert it into a dictionary
            xml_to_dict = xmltodict.parse(xml_data)
            top_song = xml_to_dict.pop('top-song')

            # Parsing XML document
            # Parse the given XML input and convert it into a json object
            tree = ElementTree.parse(file_path)
            root = tree.getroot()

            for child in root:
                tag_key = child.tag.split('}')[1]
                tag_value = json.dumps(top_song[tag_key])
                if tag_key == 'title':
                    obj[tag_key] = {
                        "href": str(child.attrib.get('href')),
                        "text":  str(child.text)
                    }
                if tag_key == 'artist':
                    obj[tag_key] = {
                        "href": str(child.attrib.get('href')),
                        "name": str(child.text)
                    }
                if tag_key == 'weeks':
                    if isinstance(json.loads(tag_value)['week'], list):
                        obj[tag_key] = {
                            "last": datetime.strptime(child.attrib.get('last'), '%Y-%m-%d').date(),
                            "week": [
                                {"date": datetime.strptime(week, '%Y-%m-%d').date()}
                                for week in json.loads(tag_value)['week']
                            ]
                        }
                    else:
                        obj[tag_key] = {
                            "last": datetime.strptime(child.attrib.get('last'), '%Y-%m-%d').date(),
                            "week": [{"date": [json.loads(tag_value)['week']]}]
                        }

                if tag_key == 'formats':
                    if isinstance(json.loads(tag_value)['format'], list):
                        obj[tag_key] = [
                            {"format": frmt}
                            for frmt in json.loads(tag_value)['format']
                        ]
                    else:
                        obj[tag_key] = [
                            {"format": json.loads(tag_value)['format']}
                        ]

                if tag_key == 'album':
                    obj[tag_key] = {
                        "url": str(child.attrib.get('url')),
                        "uri": str(child.attrib.get('uri')),
                        "name": str(child.text)
                    }

                if tag_key == 'recorded':
                    obj[tag_key] = str(child.text)

                if tag_key == 'genres':
                    if isinstance(json.loads(tag_value)['genre'], list):
                        obj[tag_key] = [
                            {"genre": genre}
                            for genre in json.loads(tag_value)['genre']
                        ]
                    else:
                        obj[tag_key] = [
                            {"genre": json.loads(tag_value)['genre']}
                        ]

                if tag_key == 'lengths':
                    if isinstance(json.loads(tag_value)['length'], list):
                        obj[tag_key] = [
                            {"length": length}
                            for length in json.loads(tag_value)['length']
                        ]
                    else:
                        obj[tag_key] = [
                            {"length": json.loads(tag_value)['length']}
                        ]

                if tag_key == 'writers':
                    if isinstance(json.loads(tag_value)['writer'], list):
                        obj[tag_key] = [
                            {"writer": writer}
                            for writer in json.loads(tag_value)['writer']
                        ]
                    else:
                        obj[tag_key] = [
                            {"writer": json.loads(tag_value)['writer']}
                        ]

                if tag_key == 'producers':
                    if isinstance(json.loads(tag_value)['producer'], list):
                        obj[tag_key] = [
                            {"producer": producer}
                            for producer in json.loads(tag_value)['producer']
                        ]
                    else:
                        obj[tag_key] = [
                            {"producer": json.loads(tag_value)['producer']}
                        ]

                if tag_key == 'descr':
                    tag_value = json.dumps(top_song[tag_key])
                    print("tag_value:", tag_value)
                    obj['description'] = tag_value

                    self.documents.append(tag_value)

            print("obj:", obj)
            print("\n")
            print("doc:", self.documents)
            self.metadata.append(obj)

    def ingest_all_docs(self, data_dir: str):

        for xml_doc in os.listdir(data_dir):
            self.ingest_doc(os.path.join(data_dir, xml_doc))

    def create_collection(self):

        collections = self.client.get_collections()
        if self.collection_name not in [coll.name for coll in collections.collections]:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=self.client.get_fastembed_vector_params(),
                sparse_vectors_config=self.client.get_fastembed_sparse_vector_params(on_disk=True),
                optimizers_config=models.OptimizersConfigDiff(
                    default_segment_number=3,
                    indexing_threshold=0
                ),
                quantization_config=models.ScalarQuantization(
                    scalar=models.ScalarQuantizationConfig(
                        type=models.ScalarType.INT8,
                        quantile=0.99,
                        always_ram=True
                    )
                )
            )

    def create_payload_index(self):

        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name=self.text_field_name,
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                min_token_len=2,
                max_token_len=20,
                lowercase=True
            )
        )

        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="title",
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                lowercase=True
            )
        )

        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="genres.genre",
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                lowercase=True
            )
        )

        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="artist",
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                lowercase=True
            )
        )

        """
        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="weeks.last",
            field_schema=models.PayloadIndexInfo(
                data_type=models.PayloadSchemaType.DATETIME
            )
        )

        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name="weeks.week.date",
            field_schema=models.PayloadIndexInfo(
                data_type=models.PayloadSchemaType.DATETIME
            )
        )
        
        """

        """
        
        self.client.create_payload_index(
            collection_name=f'{self.collection_name}',
            field_name="genre",
            field_schema=models.KeywordIndexParams(
                type=models.KeywordIndexType.KEYWORD
            )
        )

        self.client.create_payload_index(
            collection_name=f'{self.collection_name}',
            field_name="title",
            field_schema=models.KeywordIndexParams(
                type=models.KeywordIndexType.KEYWORD
            )
        )

        self.client.create_payload_index(
            collection_name=f'{self.collection_name}',
            field_name="artist",
            field_schema=models.KeywordIndexParams(
                type=models.KeywordIndexType.KEYWORD
            )
        )
        """

        """
        self.client.create_payload_index(
            collection_name=f'{self.collection_name}',
            field_name="last",
            field_schema=models.DatetimeIndexParams(
                type=models.DatetimeIndexType,
                range=False,
            ),
        )
        
        self.client.create_payload_index(
            collection_name=f'{self.collection_name}',
            field_name="week",
            field_schema=models.DatetimeIndexParams(
                type=models.DatetimeIndexType,
                range=False,
            ),
        )

        """

    def insert_documents(self):
        self.client.add(
            collection_name=self.collection_name,
            documents=self.documents,
            metadata=self.metadata,
            ids=tqdm(range(len(self.metadata)))
        )


print("ROOT_DIR: ", ROOT_DIR)
print("DATA_DIR: ", DATA_DIR)
print("\n")


if __name__ == '__main__':
    operations = HybridQdrantOperations()

    # Parse all given XML documents input and convert it into a json object
    operations.ingest_all_docs(DATA_DIR)

    # Create collection
    operations.create_collection()
    operations.create_payload_index()

    # Insert documents
    operations.insert_documents()


"""
{
    'title': {
        'href': 'http://en.wikipedia.org/wiki/Fallin%27', 
        'text': "Fallin'"
    }, 
    'artist': {
        'href': 'http://en.wikipedia.org/wiki/Alicia_Keys', 
        'name': 'Alicia Keys'
    }, 
    'weeks': {
        'last': '2001-10-13', 
        'week': ['2001-08-18', '2001-08-25', '2001-09-01', '2001-09-29', '2001-10-06', '2001-10-13']
    }, 
    'album': {
        'url': 'http://upload.wikimedia.org/wikipedia/en/thumb/6/6d/Alicia-keys-fallin-single.jpg/200px-Alicia-keys-fallin-single.jpg', 
        'uri': '/images/Alicia-Keys+Fallin.jpg', 
        'name': 'Songs in A Minor'
    }, 
    'recorded': '2001; KrucialKeys Studios , ( New York City , New York\n  )', 
    'genres': ['R&B', 'soul'], 
    'lengths': ['3:30 (album version)', '3:16 (radio edit)'], 
    'writers': 'Alicia Keys', 
    'producers': 'Alicia Keys', 
    'description': '{"p": [{"i": ["Songs in A Minor", "Billboard", "Rolling Stone", "Blender"], "#text": "\\"Fallin\'\\" is a song recorded, written, and produced by\\n    American R&B-soul singer\\u2013songwriter Alicia Keys for her\\n    debut album, \\n    (2001). Released to radio and music\\n    video outlets in 2001, it is generally considered her signature\\n    song. \\"Fallin\'\\" became Keys\' first number-one single in the\\n    United States and her first top five hit in several countries.\\n    It is also Keys\' second biggest single to date after 2007\'s \\"No\\n    One\\". \\"Fallin\'\\" won three Grammy Awards in 2002, including Song\\n    of the Year, Best R&B Song, and Best Female R&B Vocal\\n    Performance. In 2009 the single was named the 29th most\\n    successful song of the 2000s, on the \\n    Hot 100 Songs of the Decade. \\n    ranked it number sixty-two on their Top 100\\n    Songs of the 2000s decade. The song charted at 413 in \\n    magazine\'s 500 Greatest Songs Since You Were\\n    Born."}, "Keys described the song as being a strong representative of\\n    who she was as an artist. She explained that the song was about\\n    the \\"ins and outs\\" of being in a relationship. She went on to\\n    say, \\"Sometimes, you\'re completely head-over-heels in love with\\n    someone, and sometimes you can\'t stand that person. You fall in\\n    and out, sometimes it goes back and forth, and that\'s just what\\n    relationships are about.\\"", "Although it is regarded as Keys\' signature song, it almost\\n    did not become her single. Before she joined J Records, Keys\\n    had signed a recording contract with Sony\'s Columbia Records.\\n    Sony felt that Keys should sing more mainstream-accessible\\n    material written by others, while she insisted upon recording\\n    her own compositions. As a result, Keys\' recording career\\n    remained in limbo for two years. Bored and with free time on\\n    her hands because of Sony\'s lack of interest in her, Keys\\n    decided to put her time to good use and began to write a song.\\n    Instantly, arpeggios hit her, and she became inspired with the\\n    chords that would define \\"Fallin\'\\". She quickly composed lyrics\\n    basing the tune on the emotions she faced during one of her\\n    first serious romantic relationships.", "As Keys was still in artistic limbo, the song was originally\\n    not meant for her; Sony intended to have Kim Scott, a child\\n    prodigy signed to their label, record \\"Fallin\'\\". Keys became\\n    uneasy over the matter, as she wanted to record the song\\n    herself, but could not because Sony was not focusing on her\\n    career. At first Keys had surrendered the piece to Scott\\n    willingly, but later changed her mind, and Scott did not record\\n    it. Clive Davis, a former Columbia executive then at Arista\\n    Records, heard about Keys and was impressed with her artistry.\\n    After Davis started a label of his own, J Records, he bought\\n    out Keys\' Sony contract and signed her to his label.", {"i": "Songs in A Minor", "#text": "Once at J Records, Keys began working on her debut album, \\n    . Unlike Sony, J Records did not mind\\n    Keys recording her own compositions, and encouraged her to do\\n    so. When it was time for a first single to be chosen,\\n    \\"Girlfriend\\", a song Keys had worked on with Jermaine Dupri\\n    while at Columbia, was considered, but Keys and other\\n    executives agreed that \\"Fallin\'\\" would be the best choice."}, "\\"Fallin\'\\" is a gospel-influenced song. It begins with piano\\n    and basic drum programming, then proportionally builds up to a\\n    crescendo. The record also features a violin performance of the\\n    pizzicato and legato strings by Miri Ben-Ari. Keys\'\\n    collaborator Kerry \\"Krucial\\" Brothers provides the song\'s\\n    digital programming. The song samples 1966\'s \\"It\'s a Man\'s\\n    Man\'s Man\'s World\\" by James Brown. The song is written in the\\n    key of E minor.", {"i": ["Rolling Stone", "NME", "The Independent", "Blender", "The Village Voice", "Australian Idol"], "span": {"@style": "padding-left:0.1em;", "#text": "\'"}, "#text": "The song was one of the most critically acclaimed songs of\\n    the year. Barry Walters of \\n    compared Keys performance in the song to an\\n    Aretha Franklin vibe. Sal Cinquemani of Slant Magazine\\n    complimented Keys\' voice, saying it was a \\"restrained vocal\\n    that never permeates the track\'s tight arrangement\\". Christian\\n    Ward of \\n    alluded to the song being emotional, saying it will\\n    make the listener \\"weep \'til you\'re a dribbling, snotty wreck\\".\\n    Simon Price of \\n    called the song Keys\' breakthrough song.\\n    Stephen Thomas Erlewine of Allmusic pointed out the lack of\\n    depth in the song, saying it \\"doesn\'t have much body to it\\",\\n    which he felt was \\"a testament to Keys\' skills as a musician\\".\\n    The song was listed at number four hundred and thirteen on \\n    magazine\'s list of The 500 Greatest Songs Since\\n    You Were Born and at number four in \\n    \\n    s 2001 Pazz & Jop\\n    critics\' poll. \\"Fallin\'\\" was banned from many \\n    auditions due to its popularity."}, {"i": "American Idol", "#text": "\\"Fallin\'\\" was released in April 2001 as Keys\' debut single,\\n    amidst considerable promotion and praise from Davis and J\\n    Records. \\"Fallin\'\\" peaked at number one on the U.S. Hot 100 and\\n    Hot R&B/Hip-Hop Songs charts and remained there for six and\\n    four weeks, respectively. It also reached the top position in\\n    the Netherlands, Belgium, and New Zealand as well as the top\\n    five in the United Kingdom, France, Italy, Germany, Austria,\\n    Switzerland, Norway, and Ireland and the top ten in Sweden and\\n    Australia. A remix of the song, featuring Busta Rhymes and\\n    Rampage from the Flipmode Squad (also at that time signed to J\\n    Records), included on the British edition of her album,\\n    reimagines the song as a hip hop-flavored dance song. Since its\\n    release, \\"Fallin\'\\" has become a popular standard for\\n    contestants in the reality television series \\n    ."}, "As one of the most critically acclaimed singles of 2001,\\n    \\"Fallin\'\\" was nominated for several awards. \\"Fallin\'\\" connected\\n    well with NARAS as the song was nominated in 2002 for four\\n    Grammy Awards: Song of the Year, Record of the Year, Best\\n    Female R&B Vocal Performance, and Best R&B Song. It\\n    ended up winning Song of the Year, Best Female R&B Vocal\\n    Performance, and Best R&B Song (Record of the Year was\\n    awarded to U2\'s \\"Walk On\\").", "At the 2001 Billboard Music Awards, \\"Fallin\'\\" was nominated\\n    for the Hot 100 Single of the Year; however, it lost the award\\n    to Lifehouse\'s \\"Hanging by a Moment\\". The song was also\\n    nominated for Outstanding Song and Outstanding Music Video at\\n    the 2002 NAACP Image Awards; it did not win in either\\n    category.", "The music video for \\"Fallin\'\\", directed by Chris Robinson.\\n    Unlike most other contemporary R&B videos, the video for\\n    \\"Fallin\'\\" was a low-key clip with no dancing. The video opens\\n    with a radio playing \\"Girlfriend\\", where Keys is sitting at a\\n    piano. The plot has Keys traveling to a prison to visit her\\n    incarcerated boyfriend. The plot is continued in the video for\\n    Keys\' next single, \\"A Woman\'s Worth\\", which explores what\\n    happens when Keys\' boyfriend is released and, with her help,\\n    adjusts back to regular life. Keys said in an interview that\\n    she was supposed to be the one incarcerated, and her boyfriend\\n    was visiting her."]}'}

"""


