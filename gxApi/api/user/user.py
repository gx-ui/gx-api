from fastapi import HTTPException, APIRouter
from api.models import DataSource,User
user_api = APIRouter()

@user_api.get("/{username}")
async def getDataSource(username: str):
    # 查询用户是否存在
    user = await User.get_or_none(username=username)
    if not user:
        raise HTTPException(status_code=404, detail="用户未找到")
    # 查询该用户的所有数据源
    dataSources = await DataSource.filter(user=user)
    # 提取数据源名称列表
    dataSourcesList = [source.name for source in dataSources]
    # 返回结果
    return {
        "dataSourcesList": dataSourcesList,
        "sum": len(dataSourcesList)
}
