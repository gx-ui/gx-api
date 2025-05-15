from fastapi import FastAPI
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from tortoise.contrib.fastapi import register_tortoise
from setting import T_ORM
from api.user.login import login
from api.db_connect.pgsql.pgsql_cn import pg
from api.user.user import user_api
app = FastAPI()
register_tortoise(
    app=app,
    config=T_ORM,
)



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许的前端地址
    allow_credentials=True,
    allow_methods=["*"],  # 允许的HTTP方法
    allow_headers=["*"],  # 允许的HTTP头部
)

app.include_router(router=pg, prefix="/pg", tags=["连接pgsql进行配置数据"])
app.include_router(router=login, tags=["用户登入"])
app.include_router(router=user_api, tags=["获取用户信息"])



if __name__ == "__main__":

    uvicorn.run("main:app", port=8080, reload=True)





