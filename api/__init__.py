from sanic import Blueprint
from .liupanshan_bp import liupanshanbp
# from .wuzhong_bp import wuzhongbp

api = Blueprint.group(liupanshanbp,url_prefix="/api")
#api = Blueprint.group(liupanshanbp,wuzhongbp,url_prefix="/api")
