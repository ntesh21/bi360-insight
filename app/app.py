import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from flask import Flask, render_template, jsonify
from dataAnalysis.order_analysis import get_orders_summary,get_order_status, get_order_time, get_late_deliveries, get_num_orders_per_year
from dataAnalysis.sales_analysis import get_city_wise_sellers, get_product_sold_per_category, get_payment_types, get_sales_summary


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

####################################  Order Analysis ##########################################

@app.route('/orders_analysis')
def orders_analysis():
    order_summary = get_orders_summary()
    return render_template('order.html', order_summary=order_summary)

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

########################################  Sales Analysis    #######################
@app.route('/sales_analysis')
def sales_analysis():
    sales_summary = get_sales_summary()
    return render_template('sales.html', sales_summary=sales_summary)

@app.route('/city_wise_sellers')
def citiwise_seller_data():
    city_wise_sellers = get_city_wise_sellers()
    return jsonify(city_wise_sellers)

@app.route('/top_products_sold_per_category')
def top_products_sold_per_category():
    top_products_per_category,_ = get_product_sold_per_category()
    return jsonify(top_products_per_category)

@app.route('/bottom_products_sold_per_category')
def bottom_products_sold_per_category():
    _,bottom_products_per_category = get_product_sold_per_category()
    return jsonify(bottom_products_per_category)

@app.route('/payment_types')
def payment_types_data():
    types_of_payments = get_payment_types()
    return jsonify(types_of_payments)

if __name__ == '__main__':
    app.run(debug=True)
