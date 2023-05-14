from sanic import Blueprint
from .liupanshan_bp import lpsbp

api = Blueprint.group(lpsbp,url_prefix="/api")
