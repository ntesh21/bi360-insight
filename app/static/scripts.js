document.addEventListener('DOMContentLoaded', function () {
    fetch('/orders_per_year')
        .then(response => response.json())
        .then(data => {
            const years = Object.keys(data);
            const orders = Object.values(data);

            new Chart(document.getElementById('ordersChart'), {
                type: 'bar',
                data: {
                    labels: years,
                    datasets: [{
                        label: 'Number of Orders',
                        data: orders,
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        });
});

document.addEventListener('DOMContentLoaded', function () {
    fetch('/orders_status')
        .then(response => response.json())
        .then(data => {
            const status = Object.keys(data);
            const percentage = Object.values(data);

            new Chart(document.getElementById('ordersStatusChart'), {
                type: 'bar',
                data: {
                    labels: status,
                    datasets: [{
                        label: 'Order Status',
                        data: percentage,
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        });
});

document.addEventListener('DOMContentLoaded', function () {
    fetch('/orders_time_per_day')
        .then(response => response.json())
        .then(data => {
            const time = Object.keys(data);
            const count = Object.values(data);

            new Chart(document.getElementById('ordersTimeOfDay'), {
                type: 'bar',
                data: {
                    labels: time,
                    datasets: [{
                        label: 'Orders by Time of the Day',
                        data: count,
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        });
});

document.addEventListener('DOMContentLoaded', function () {
    fetch('/late_deliveries')
        .then(response => response.json())
        .then(data => {
            const deliverStatus = Object.keys(data);
            const deliveryPercentage = Object.values(data);
        
            var orderStatusDataPie = {
                labels: deliverStatus,
                values: deliveryPercentage,
                backgroundColor: [
                    'rgba(255, 99, 132, 0.5)',
                    'rgba(54, 162, 235, 0.5)',
                ],
                borderWidth: 1
                };
            
            console.log("HI")
            console.log(orderStatusDataPie)
        
            new Chart(document.getElementById('lateDeliveries'), {
                type: 'pie',
                data: {
                    labels:  orderStatusDataPie.keys,
                    datasets: [{
                        label: deliverStatus,
                        data: orderStatusDataPie.values,
                        backgroundColor: orderStatusDataPie.backgroundColor,
                        borderWidth: orderStatusDataPie.borderWidth
                    }]
                },
                options: {
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    },
                    
                }
            });
        });
});

document.addEventListener('DOMContentLoaded', function () {
    fetch('/payment_types')
        .then(response => response.json())
        .then(data => {
            const paymentMethods = Object.keys(data);
            const paymentPercentage = Object.values(data);
        
            var orderStatusDataPie = {
                labels: paymentMethods,
                values: paymentPercentage,
                backgroundColor: [
                'rgba(255, 99, 132, 0.5)',
                'rgba(54, 162, 235, 0.5)',
                'rgba(255, 206, 86, 0.5)',
                'rgba(75, 192, 192, 0.5)',
                'rgba(153, 102, 255, 0.5)',
                ],
                borderWidth: 1
                };
            
            console.log(orderStatusDataPie)
        
            new Chart(document.getElementById('paymentTypes'), {
                type: 'pie',
                data: {
                    labels:  orderStatusDataPie.keys,
                    datasets: [{
                        data: orderStatusDataPie.values,
                        backgroundColor: orderStatusDataPie.backgroundColor,
                        borderWidth: orderStatusDataPie.borderWidth
                    }]
                },
                options: {
                    // scales: {
                    //     y: {
                    //         beginAtZero: true
                    //     }
                    // },
                    title: {
                        display: true,
                        text: 'Type of Payments'
                    }
                    
                }
            });
        });
});


