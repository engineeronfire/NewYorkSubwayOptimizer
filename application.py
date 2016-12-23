from flask import Flask, request, render_template, jsonify
import json
from sns import subscribe, notify
from mta import plan_trip
# print a nice greeting.

application = Flask(__name__)



@application.route('/tweets',methods = ['POST','GET'])
def tweets():
    source = request.form['source']
    destination = request.form['destination']
    print source, destination
    response = plan_trip(source, destination)
    notify(response)
    return jsonify({'data': response})

@application.route('/')
def map():
    return render_template('index.html')
# EB looks for an 'application' callable by default.
# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run(host='0.0.0.0', port=8000)
