import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
rootdir = os.path.dirname(parentdir)
sys.path.extend([rootdir, parentdir])

from flask import Flask, render_template, jsonify
from dataAnalysis.collect_data import get_master_data
from dataAnalysis.order_analysis import get_orders_summary,get_order_status, get_order_time, get_late_deliveries, get_num_orders_per_year, get_top_states_by_order
from dataAnalysis.sales_analysis import get_city_wise_sellers, get_product_sold_per_category, get_payment_types, get_sales_summary, get_average_frieght_by_product_category
from dataAnalysis.customer_analysis import get_city_with_highest_customers, get_customers_segment_by_lifetime, get_customers_segment_by_rfm, get_customer_summary, get_review_vs_rating

app = Flask(__name__)

master_df = get_master_data()

# @app.route('/')
# def index():
#     return render_template('index.html')

####################################  Order Analysis ##########################################

@app.route('/orders_analysis')
def orders_analysis():
    order_summary = get_orders_summary()
    return render_template('order.html', order_summary=order_summary)

@app.route('/orders_per_year')
def orders_per_year_data():
    orders_per_year = get_num_orders_per_year(master_df)
    return jsonify(orders_per_year)

@app.route('/orders_status')
def orders_status_data():
    order_status = get_order_status(master_df)
    return jsonify(order_status)

@app.route('/orders_time_per_day')
def orders_time_per_day_data():
    purchase_time = get_order_time(master_df)
    return jsonify(purchase_time)

@app.route('/late_deliveries')
def orders_late_deliveries():
    late_deliveries = get_late_deliveries(master_df)
    return jsonify(late_deliveries)

@app.route('/top_states_by_order')
def top_states_by_orders():
    top_orders_state = get_top_states_by_order(master_df)
    return jsonify(top_orders_state)

########################################  Sales Analysis    #######################
@app.route('/sales_analysis')
def sales_analysis():
    sales_summary = get_sales_summary(master_df)
    return render_template('sales.html', sales_summary=sales_summary)

@app.route('/city_wise_sellers')
def citiwise_seller_data():
    city_wise_sellers = get_city_wise_sellers(master_df)
    return jsonify(city_wise_sellers)

@app.route('/top_products_sold_per_category')
def top_products_sold_per_category():
    top_products_per_category,_ = get_product_sold_per_category(master_df)
    return jsonify(top_products_per_category)

@app.route('/bottom_products_sold_per_category')
def bottom_products_sold_per_category():
    _,bottom_products_per_category = get_product_sold_per_category(master_df)
    return jsonify(bottom_products_per_category)

@app.route('/payment_types')
def payment_types_data():
    types_of_payments = get_payment_types(master_df)
    return jsonify(types_of_payments)

@app.route('/avg_freight_per_product')
def avg_freight_per_product():
    avg_freight = get_average_frieght_by_product_category(master_df)
    return jsonify(avg_freight)

####################################### Customers Analysis ############################
@app.route('/')
def customers_analysis():
    customer_summary = get_customer_summary()
    return render_template('customer.html', customer_summary=customer_summary)

@app.route('/highest_customers_cities')
def highest_customers_cities():
    highest_customers = get_city_with_highest_customers()
    return jsonify(highest_customers)

@app.route('/customers_lifetime_value_segment')
def customers_by_lifetime_values():
    customer_segments = get_customers_segment_by_lifetime()
    return jsonify(customer_segments)

@app.route('/customers_rfm_segment')
def customers_by_rfm_values():
    rfm_segments,_ = get_customers_segment_by_rfm()
    return jsonify(rfm_segments)

@app.route('/customers_rfm_ratio')
def customers_by_rfm_ratio():
    _,rfm_ratio = get_customers_segment_by_rfm()
    return jsonify(rfm_ratio)

@app.route('/reviews_vs_rating')
def customers_reviews_vs_rating():
    review_vs_rating = get_review_vs_rating()
    return jsonify(review_vs_rating)

if __name__ == '__main__':
    app.run(debug=True)
