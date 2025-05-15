from fastapi import APIRouter
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from tortoise.query_utils import Prefetch
from api.db_connect.table_info import *
from api.models import DataSource as DS
from api.models import User as US
from api.models import DataAsset as DA
from tortoise.exceptions import DoesNotExist
pg = APIRouter()
# postgresql+psycopg2://postgres:123456@127.0.0.1:5432/postgres
# postgresql+psycopg2://postgres:123456@39dl416aa932.vicp.fun:24053/postgres

# 连接用户的pgsql数据库,并获取数据库的表名
@pg.post("/tables")
async def get_tables(datasource: InDataSource):
    engine = None
    try:
        engine = create_engine(datasource.url)
        connection = engine.connect()
        inspector = inspect(engine)
        tablesName = inspector.get_table_names()
        connection.close()
        return {"dataSourceName": datasource.name, "tablesName": tablesName, "code": 200}
    except SQLAlchemyError as e:
        # 捕获 SQLAlchemy 的通用数据库异常
        return {"message": f"数据库连接失败: {str(e)}"}
    except Exception as e:
        # 捕获其他未知异常
        return {"message": f"存在错误: {str(e)}"}
    finally:
        if engine:
            engine.dispose()

@pg.get("/{assetDataId}/tablesInfo")
async def get_tables_info(assetDataId: int):
    engine = None
    try:
        data_asset = await DA.get(id=assetDataId).prefetch_related("datasource")
        data_source = data_asset.datasource
        print(data_source.url,data_asset.name)
        # 创建引擎并连接数据库
        engine = create_engine(data_source.url)
        connection = engine.connect()
        inspector = inspect(engine)
        # 获取表列信息
        columns = inspector.get_columns(data_asset.name)
        # 初始化模型
        fields_info = FieldsInfo(tableName=data_asset.name)
        # 添加字段信息
        for col in columns:
            fields_info.add_column(column_name=col["name"], column_type=str(col["type"]))
        # 关闭连接
        connection.close()
        # 成功返回
        return {"code": 200, "data": fields_info}
    except SQLAlchemyError as e:
        return {"code": 500, "message": f"数据库操作失败: {str(e)}"}
    except Exception as e:
        return {"code": 500, "message": f"未知错误: {str(e)}"}
    finally:
        if engine:
            engine.dispose()  # 确保资源最终释放

# 获取指定的数据源（依据名称）
@pg.get("/{userid}/dataSource/{name}")
async def get_dataSource(userid: int, name: str):
    try:
        user = await US.get(id=userid)
        dataSource = await DS.get(name=name, user=user)
        return {"dataSourceId": dataSource.id, "code": 200}
    except DoesNotExist:
        return {"dataSourceId": 0, "message": "数据源不存在", "code": 404}

# 获取用户所有的数据源
@pg.get("/{userid}/dataSource")
async def get_dataSource(userid: int):
    user = await US.get(id=userid)
    try:
        dataSource = await DS.filter(user=user)
        return {"dataSource": dataSource, "code": 200}
    except DoesNotExist:
        return {"dataSource": [], "message": "数据源不存在", "code": 404}

# 创建数据源
@pg.post("/{userid}/dataSource")
async def create_datasource(userid: int, datasource: InDataSource):
    try:
        user = await US.get(id=userid)
        # 直接尝试创建数据源，依赖数据库唯一约束
        dataSource = await DS.create(
            name=datasource.name,
            url=datasource.url,
            user=user
        )
        return {"message": "数据源创建成功", "dataSourceId": dataSource.id, "code": 200}
    except IntegrityError as e:
        if "unique" in str(e).lower():
            return {"message": "数据源已存在", "code": 409}
        raise
    except Exception as e:
        return {"message": f"存在错误: {str(e)}", "code": 500}

# 获取用户所有的数据资产(不包含数据源)
@pg.get("/{userid}/dataAsset")
async def get_dataAsset(userid: int):
    # 需要先获取用户所有的数据源，然后根据数据源获取所以数据资产
    user = await US.get(id=userid)
    try:
        data_source = await DS.filter(user=user).prefetch_related(Prefetch("dataassets"))
        if not data_source:
            return {"dataAsset": [], "code": 200}
        # 收集所有 datasource  用于批量查询
        dataAsset = []
        for ds in data_source:
            dataAsset.extend(ds.datasources)
        return {"dataAsset": dataAsset, "code": 200}
    except Exception as e:
        return {"dataAsset": [], "code": 500, "message": f"Internal server error: {str(e)}"}

# 从数据源创建数据资产
@pg.post("/{dataSourceId}/dataAsset")
async def create_dataasset(dataSourceId: int, dataAssetNames: list[str]):
    try:
        # 获取数据源对象，若不存在则抛出异常
        datasource = await DS.get(id=dataSourceId)
    except DoesNotExist:
        return {"message": "数据源不存在", "code": 404}
        # 准备要创建的 DataAsset 实例列表
    new_assets = []
    for name in dataAssetNames:
        new_assets.append(DA(name=name, datasource=datasource))
    created_ids = []
    existing_names = []
    # 批量尝试创建，利用数据库唯一约束自动过滤重复项
    for asset in new_assets:
        try:
            await asset.save()
            created_ids.append(asset.id)
        except IntegrityError as e:
            # 捕获唯一约束冲突，记录已存在的名称
            if "unique" in str(e).lower():
                existing_names.append(asset.name)
            else:
                raise
    # 根据结果返回不同的响应
    if not created_ids:
        return {"message": f"所有数据资产已存在: {', '.join(existing_names)}", "code": 409}
    elif existing_names:
        return {
            "message": f"部分创建成功，以下已存在: {', '.join(existing_names)}",
            "dataAssetId": created_ids,
            "code": 200
        }
    else:
        return {"message": "全部数据资产创建成功", "dataAssetId": created_ids, "code": 200}

# 获取用户所有的数据源和数据资产
@pg.get("/{userid}/dataSourceAndAsset")
async def get_dataSourceAndAsset(userid: int):
    try:
        user = await US.get(id=userid)
        # 获取用户的所有数据源，并预加载其数据资产
        data_sources = await DS.filter(user=user).prefetch_related(
            Prefetch("dataassets", queryset=DA.all())
        )
        if not data_sources:
            return {"dataSource": [], "totalDataSourceCount": 0, "totalDataAssetCount": 0, "code": 200, "message":"ok"}
        total_asset_count = sum(len(ds.dataassets or []) for ds in data_sources)
        dataSources = [
            {
                "id": ds.id,
                "name": ds.name,
                "assetCount": len(ds.dataassets or []),
                "dataassets": [InDataAsset.model_validate(asset) for asset in (ds.dataassets or [])]
            }
            for ds in data_sources
        ]
        return {
            "code": 200,
            "totalDataSourceCount": len(dataSources),
            "totalDataAssetCount": total_asset_count,
            "dataSource": dataSources
        }
    except Exception as e:
        return {"dataSource": [], "message": f"发生错误: {str(e)}", "code": 500}

@pg.post("/expectations")
async def get_expectations(dataSourceName: str, tablesNames: list[str]):
    return {"DataSource": dataSourceName, "DataAsset": tablesNames}



