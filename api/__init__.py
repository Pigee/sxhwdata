from sanic import Blueprint
from .liupanshan_bp import liupanshanbp
from .wuzhong_bp import wuzhongbp
'''
from .changcheng_bp import changchengbp
from .hongsibao_bp import hongsibaobp
from .ningdong_bp import ningdongbp
from .taiyangshan_bp import taiyangshanbp
from .tongxin_bp import tongxinbp
from .yanchi_bp import yanchibp
from .yinchuan_bp import yinchuanbp
from .zhongning_bp import zhongningbp
from .zhongwei_bp import zhongweibp
from .pingluo_bp import pingluobp
'''
api = Blueprint.group(liupanshanbp,wuzhongbp,url_prefix="/api")
#api = Blueprint.group(liupanshanbp,wuzhongbp,changchengbp,hongsibaobp,ningdongbp,tongxinbp,yanchibp,yinchuanbp,zhongningbp,url_prefix="/api")
