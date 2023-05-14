from sanic import Sanic
from api import api


app = Sanic("sxhwdata")
app.blueprint(api)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1337, dev=True)
