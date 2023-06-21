from sanic import Sanic
from api import api
import pymysql

app = Sanic("sxhwdata")
app.blueprint(api)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=13137, dev=True)


