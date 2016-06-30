#!/usr/bin/env python
from flaskexample import app
#app.run(host='0.0.0.0', port=5000, debug = True)
if __name__ == '__main__':
    app.run(debug = True)
