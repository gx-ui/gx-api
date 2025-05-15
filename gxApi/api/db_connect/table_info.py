from typing import Dict
from pydantic import BaseModel,  ConfigDict


class InDataSource(BaseModel):
    id: int = None
    name: str = None
    url: str = None
    model_config = ConfigDict(from_attributes=True)


class InDataAsset(BaseModel):
    id: int = None
    name: str = None
    model_config = ConfigDict(from_attributes=True)


class DataSourceWithAssets(InDataSource):
    dataassets: list[InDataAsset]
    model_config = ConfigDict(from_attributes=True)


class FieldsInfo(BaseModel):
    tableName: str = None
    column_info: list[Dict[str, str]] =[]
    def add_column(self, column_name: str, column_type: str):
        self.column_info.append({column_name: column_type})