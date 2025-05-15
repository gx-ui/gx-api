from fastapi import APIRouter

from pydantic import BaseModel
from tortoise.exceptions import DoesNotExist

from api.models import User
class r_User(BaseModel):
    username: str=None
    password: str=None


login = APIRouter()


@login.post("/login")
async def user_login(userin: r_User):
    try:
        user = await User.get(username=userin.username)
        if user.password != userin.password:
            return {"code":401,"message": "用户名或密码不正确"}
        else:
            return {"code":200,"message": "登入成功", "username": user.username,"userid":user.id}
    except DoesNotExist:
        return {"code":401,"message": "用户名或密码不正确"}
