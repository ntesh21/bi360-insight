import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from flask import Flask, render_template, jsonify
from dataAnalysis.order_analysis import get_orders_summary,get_order_status, get_order_time, get_late_deliveries, get_num_orders_per_year

app = Flask(__name__)


types_of_payments = {
    "credit card": 74,
    "boleto": 19,
    "voucher":6,
    "debit card":1,
    "not defined":0
}


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/orders_analysis')
def orders_analysis():
    order_summary = get_orders_summary()
    return render_template('order.html', order_summary=order_summary)

####################################Order#######################
@app.route('/orders_per_year')
def orders_per_year_data():
    orders_per_year = get_num_orders_per_year()
    return jsonify(orders_per_year)

@app.route('/orders_status')
def orders_status_data():
    order_status = get_order_status()
    return jsonify(order_status)

@app.route('/orders_time_per_day')
def orders_time_per_day_data():
    purchase_time = get_order_time()
    return jsonify(purchase_time)

@app.route('/late_deliveries')
def orders_late_deliveries():
    late_deliveries = get_late_deliveries()
    return jsonify(late_deliveries)

@app.route('/payment_types')
def payment_types_data():
    return jsonify(types_of_payments)

if __name__ == '__main__':
    app.run(debug=True)
