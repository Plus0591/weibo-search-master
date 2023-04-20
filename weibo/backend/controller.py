from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/')
def hello():
    data = {'name': 'John', 'age': 30, 'city': 'New York'}
    return jsonify(data)


if __name__ == '__main__':
    app.run(port=8000)
