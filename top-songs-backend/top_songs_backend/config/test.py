import os

from qdrant_client import QdrantClient, grpc, models
from qdrant_client.http.models import models as rest


class HybrideQdrantTest:

    def __init__(self):
        self.client = QdrantClient(
            url=os.environ['QDRANT_URL'],
            api_key=os.environ['QDRANT_API_KEY'],
            prefer_grpc=True
        )

    @classmethod
    def convert_payload_schema_type(cls, model: grpc.PayloadSchemaType) -> rest.PayloadSchemaType:
        if model == grpc.PayloadSchemaType.Float:
            return rest.PayloadSchemaType.FLOAT
        elif model == grpc.PayloadSchemaType.Geo:
            return rest.PayloadSchemaType.GEO
        elif model == grpc.PayloadSchemaType.Integer:
            return rest.PayloadSchemaType.INTEGER
        elif model == grpc.PayloadSchemaType.Keyword:
            return rest.PayloadSchemaType.KEYWORD
        elif model == grpc.PayloadSchemaType.Bool:
            return rest.PayloadSchemaType.BOOL
        elif model == grpc.PayloadSchemaType.Text:
            return rest.PayloadSchemaType.TEXT
        elif model == grpc.PayloadSchemaType.Datetime:
            return rest.PayloadSchemaType.DATETIME
        # elif model == grpc.PayloadSchemaType.Uuid:
        #    return rest.PayloadSchemaType.UUID
        else:
            raise ValueError(f"invalid PayloadSchemaType model: {model}")

    @classmethod
    def convert_tokenizer_type(cls, model: grpc.TokenizerType) -> rest.TokenizerType:
        if model == grpc.Prefix:
            return rest.TokenizerType.PREFIX
        if model == grpc.Whitespace:
            return rest.TokenizerType.WHITESPACE
        if model == grpc.Word:
            return rest.TokenizerType.WORD
        if model == grpc.Multilingual:
            return rest.TokenizerType.MULTILINGUAL
        raise ValueError(f"invalid TokenizerType model: {model}")

    @classmethod
    def convert_text_index_params(cls, model: grpc.TextIndexParams) -> rest.TextIndexParams:
        return rest.TextIndexParams(
            type="text",
            tokenizer=cls.convert_tokenizer_type(model.tokenizer),
            min_token_len=model.min_token_len if model.HasField("min_token_len") else None,
            max_token_len=model.max_token_len if model.HasField("max_token_len") else None,
            lowercase=model.lowercase if model.HasField("lowercase") else None,
        )

    @classmethod
    def convert_integer_index_params(
            cls, model: grpc.IntegerIndexParams
    ) -> rest.IntegerIndexParams:
        return rest.IntegerIndexParams(
            type=rest.IntegerIndexType.INTEGER,
            range=model.range,
            lookup=model.lookup,
            is_principal=model.is_principal if model.HasField("is_principal") else None,
            on_disk=model.on_disk if model.HasField("on_disk") else None,
        )

    @classmethod
    def convert_keyword_index_params(
            cls, model: grpc.KeywordIndexParams
    ) -> rest.KeywordIndexParams:
        return rest.KeywordIndexParams(
            type=rest.KeywordIndexType.KEYWORD,
            is_tenant=model.is_tenant if model.HasField("is_tenant") else None,
            on_disk=model.on_disk if model.HasField("on_disk") else None,
        )

    @classmethod
    def convert_geo_index_params(cls, _: grpc.GeoIndexParams) -> rest.GeoIndexParams:
        return rest.GeoIndexParams(
            type=rest.GeoIndexType.GEO,
        )

    @classmethod
    def convert_bool_index_params(cls, _: grpc.BoolIndexParams) -> rest.BoolIndexParams:
        return rest.BoolIndexParams(
            type=rest.BoolIndexType.BOOL,
        )

    @classmethod
    def convert_datetime_index_params(
            cls, model: grpc.DatetimeIndexParams
    ) -> rest.DatetimeIndexParams:
        return rest.DatetimeIndexParams(
            type=rest.DatetimeIndexType.DATETIME,
            is_principal=model.is_principal if model.HasField("is_principal") else None,
            on_disk=model.on_disk if model.HasField("on_disk") else None,
        )

    @classmethod
    def convert_float_index_params(cls, model: grpc.FloatIndexParams) -> rest.FloatIndexParams:
        return rest.FloatIndexParams(
            type=rest.FloatIndexType.FLOAT,
            is_principal=model.is_principal if model.HasField("is_principal") else None,
            on_disk=model.on_disk if model.HasField("on_disk") else None,
        )

    @classmethod
    def convert_uuid_index_params(cls, model: grpc.UuidIndexParams) -> rest.UuidIndexParams:
        return rest.UuidIndexParams(
            type=rest.UuidIndexType.UUID,
            is_tenant=model.is_tenant if model.HasField("is_tenant") else None,
            on_disk=model.on_disk if model.HasField("on_disk") else None,
        )

    @classmethod
    def convert_payload_schema_params(
            cls, model: rest.PayloadSchemaParams
    ) -> grpc.PayloadIndexParams:
        if isinstance(model, rest.TextIndexParams):
            return grpc.PayloadIndexParams(text_index_params=cls.convert_text_index_params(model))

        if isinstance(model, rest.IntegerIndexParams):
            return grpc.PayloadIndexParams(
                integer_index_params=cls.convert_integer_index_params(model)
            )

        if isinstance(model, rest.KeywordIndexParams):
            return grpc.PayloadIndexParams(
                keyword_index_params=cls.convert_keyword_index_params(model)
            )

        if isinstance(model, rest.FloatIndexParams):
            return grpc.PayloadIndexParams(
                float_index_params=cls.convert_float_index_params(model)
            )

        if isinstance(model, rest.GeoIndexParams):
            return grpc.PayloadIndexParams(geo_index_params=cls.convert_geo_index_params(model))

        if isinstance(model, rest.BoolIndexParams):
            return grpc.PayloadIndexParams(bool_index_params=cls.convert_bool_index_params(model))

        if isinstance(model, rest.DatetimeIndexParams):
            return grpc.PayloadIndexParams(
                datetime_index_params=cls.convert_datetime_index_params(model)
            )

        if isinstance(model, rest.UuidIndexParams):
            return grpc.PayloadIndexParams(uuid_index_params=cls.convert_uuid_index_params(model))

        raise ValueError(f"invalid PayloadSchemaParams model: {model}")