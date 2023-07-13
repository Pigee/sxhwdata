from sanic import Sanic
from api import api
import aiomysql
from conf.mysqldb import Callconn
#from sanic_ext import Extend

app = Sanic("sxhwdata")
app.blueprint(api)
# Extend(app)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=13137, dev=True)

