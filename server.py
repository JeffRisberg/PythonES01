from flask import Flask, request, jsonify
from flask_cors import CORS

from elastic import connect_elastic

# Define the app
app = Flask(__name__)
app.config.from_object('config')
CORS(app)

# Connect to es node
es = connect_elastic(app.config['ELASTIC_IP'], app.config['ELASTIC_PORT'])


@app.route("/query/<string:match_phrase>", methods=["GET"])
def query(match_phrase):
  res = es.search(index="products",
                  query={"match_phrase": {"name": match_phrase}})
  print("Got %d Hits:" % res['hits']['total']['value'])

  for hit in res['hits']['hits'][0:5]:
    print(hit['_source'])

  return jsonify(res['hits']['hits'][0:5])


if __name__ == '__main__':
  app.run(debug=True, host="0.0.0.0", port=5000)
